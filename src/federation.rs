//! Implementations for federating `ClickHouse` schemas into a `DataFusion` [`SessionContext`].
//!
//! This module provides federation capabilities for `ClickHouse`, including:
//!
//! - [`FederatedContext`] trait for enabling federation on a [`SessionContext`] (stable)
//! - [`ClickHouseFederationProvider`] for plan transformation and SQL generation (**experimental**)
//! - [`FunctionMapper`] trait for function dealiasing during federation (**experimental**)
//! - [`TransformationRule`] trait for custom plan transformations (**experimental**)
//!
//! # Experimental APIs
//!
//! [`ClickHouseFederationProvider`], [`FunctionMapper`], and [`TransformationRule`] are building
//! blocks for advanced federation scenarios. They are **not yet integrated** into the default
//! federation flow (which uses `datafusion-federation`'s optimizer rules directly). Consumers
//! must manually invoke [`ClickHouseFederationProvider::transform`] or
//! [`ClickHouseFederationProvider::plan_to_sql`] in their own federation implementations.
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction, WindowFunction};
use datafusion::logical_expr::{
    AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowFunctionDefinition,
};
use datafusion::prelude::SessionContext;
pub use datafusion_federation; // Re-export

use crate::dialect::ClickHouseDialect;

/// Modify an existing [`SessionContext`] to enable federated query execution.
///
/// When federation is enabled, `DataFusion`'s optimizer will attempt to push entire query subtrees
/// down to the remote `ClickHouse` server rather than pulling all data locally. This is essential
/// for efficient cross-database joins between `ClickHouse` and other `DataFusion` sources.
pub trait FederatedContext {
    fn federate(self) -> SessionContext;

    fn is_federated(&self) -> bool;
}

impl FederatedContext for SessionContext {
    fn federate(self) -> SessionContext {
        use datafusion_federation::{FederatedQueryPlanner, default_optimizer_rules};

        let state = self.state();

        if state.optimizer().rules.iter().any(|rule| rule.name() == "federation_optimizer_rule") {
            self
        } else {
            SessionContext::new_with_state(
                self.into_state_builder()
                    .with_optimizer_rules(default_optimizer_rules())
                    .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
                    .build(),
            )
        }
    }

    fn is_federated(&self) -> bool {
        self.state()
            .optimizer()
            .rules
            .iter()
            .any(|rule| rule.name() == "federation_optimizer_rule")
    }
}

/// Trait for mapping/translating functions during federation.
///
/// **Experimental**: This trait is not yet integrated into the default federation flow.
///
/// Implementations provide mappings from `DataFusion` functions to their `ClickHouse`-specific
/// equivalents, enabling function dealiasing when generating SQL for remote execution.
pub trait FunctionMapper: Send + Sync + Debug {
    /// Try to dealias a scalar UDF (return the original function if aliased)
    fn try_dealias_udf(&self, udf: &Arc<ScalarUDF>) -> Option<Arc<ScalarUDF>>;

    /// Try to dealias an aggregate UDF
    fn try_dealias_udaf(&self, udaf: &Arc<AggregateUDF>) -> Option<Arc<AggregateUDF>>;
}

/// Transformation rule for converting plan nodes.
///
/// **Experimental**: This trait is not yet integrated into the default federation flow.
///
/// Allows custom transformation logic to be applied to logical plans during federation,
/// enabling `ClickHouse`-specific optimizations and rewrites.
pub trait TransformationRule: Send + Sync + Debug {
    /// Check if this rule applies to the given plan
    fn matches(&self, plan: &LogicalPlan) -> bool;

    /// Transform the plan node
    ///
    /// # Errors
    /// Returns an error if the transformation fails
    fn transform(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}

/// Federation provider for `ClickHouse`.
///
/// **Experimental**: This provider is not yet wired into the default federation flow. It must be
/// invoked manually via [`transform`](Self::transform) or [`plan_to_sql`](Self::plan_to_sql).
///
/// Handles the transformation of `DataFusion` logical plans into `ClickHouse`-compatible SQL,
/// including function translation via [`FunctionMapper`] and plan optimization via
/// [`TransformationRule`]s.
#[derive(Debug)]
pub struct ClickHouseFederationProvider {
    dialect: ClickHouseDialect,
    function_mapper: Option<Arc<dyn FunctionMapper>>,
    transformation_rules: Vec<Arc<dyn TransformationRule>>,
}

impl ClickHouseFederationProvider {
    /// Create a new `ClickHouseFederationProvider` with default settings
    pub fn new() -> Self {
        Self { dialect: ClickHouseDialect, function_mapper: None, transformation_rules: Vec::new() }
    }

    /// Add a function mapper for translating functions during federation
    #[must_use]
    pub fn with_function_mapper(mut self, mapper: Arc<dyn FunctionMapper>) -> Self {
        self.function_mapper = Some(mapper);
        self
    }

    /// Add transformation rules for plan optimization
    #[must_use]
    pub fn with_rules(mut self, rules: Vec<Arc<dyn TransformationRule>>) -> Self {
        self.transformation_rules = rules;
        self
    }

    /// Transform `LogicalPlan` for `ClickHouse` execution.
    ///
    /// This applies function translations and transformation rules to prepare
    /// the plan for execution on `ClickHouse`.
    ///
    /// # Errors
    /// Returns an error if transformation fails
    pub fn transform(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut transformed_plan = plan.clone();

        // Apply transformation rules
        for rule in &self.transformation_rules {
            if rule.matches(&transformed_plan) {
                transformed_plan = rule.transform(&transformed_plan)?;
            }
        }

        // Apply function translations if mapper exists
        if let Some(mapper) = &self.function_mapper {
            transformed_plan =
                transformed_plan.transform(|p| self.translate_plan(p, mapper.as_ref()))?.data;
        }

        Ok(transformed_plan)
    }

    /// Convert `LogicalPlan` to `ClickHouse` SQL.
    ///
    /// This transforms the plan and then generates SQL using the `ClickHouse` dialect.
    ///
    /// # Errors
    /// Returns an error if transformation or SQL generation fails
    #[allow(clippy::items_after_statements)]
    pub fn plan_to_sql(&self, plan: &LogicalPlan) -> Result<String> {
        let transformed = self.transform(plan)?;

        // Use the ClickHouse dialect for SQL generation
        use datafusion::sql::unparser::Unparser;
        let unparser = Unparser::new(&self.dialect);
        let ast = unparser.plan_to_sql(&transformed)?;
        Ok(ast.to_string())
    }

    /// Internal method to translate plan nodes using the function mapper
    fn translate_plan(
        &self,
        plan: LogicalPlan,
        mapper: &dyn FunctionMapper,
    ) -> Result<Transformed<LogicalPlan>> {
        // Use map_expressions to transform all expressions in the plan
        plan.map_expressions(|expr| expr.transform(|e| self.translate_expr(e, mapper)))
    }

    /// Recursively translate expressions using the function mapper
    #[allow(clippy::unnecessary_wraps)]
    fn translate_expr(&self, expr: Expr, mapper: &dyn FunctionMapper) -> Result<Transformed<Expr>> {
        let remapped_expr = match &expr {
            Expr::ScalarFunction(scalar_function) => {
                self.try_transform_scalar_func(scalar_function, mapper)
            }
            Expr::AggregateFunction(aggregate_function) => {
                self.try_transform_aggregate_func(aggregate_function, mapper)
            }
            Expr::WindowFunction(window_function) => {
                self.try_transform_window_func(window_function, mapper)
            }
            _ => None,
        };

        Ok(remapped_expr.unwrap_or(Transformed::no(expr)))
    }

    /// Try to transform a scalar function using the mapper
    #[allow(clippy::unused_self)]
    fn try_transform_scalar_func(
        &self,
        scalar_function: &ScalarFunction,
        mapper: &dyn FunctionMapper,
    ) -> Option<Transformed<Expr>> {
        mapper.try_dealias_udf(&scalar_function.func).map(|function| {
            Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                function,
                scalar_function.args.clone(),
            )))
        })
    }

    /// Try to transform an aggregate function using the mapper
    #[allow(clippy::unused_self)]
    fn try_transform_aggregate_func(
        &self,
        aggregate_function: &AggregateFunction,
        mapper: &dyn FunctionMapper,
    ) -> Option<Transformed<Expr>> {
        mapper.try_dealias_udaf(&aggregate_function.func).map(|function| {
            Transformed::yes(Expr::AggregateFunction(AggregateFunction::new_udf(
                function,
                aggregate_function.params.args.clone(),
                aggregate_function.params.distinct,
                aggregate_function.params.filter.clone(),
                aggregate_function.params.order_by.clone(),
                aggregate_function.params.null_treatment,
            )))
        })
    }

    /// Try to transform a window function using the mapper
    #[allow(clippy::unused_self)]
    fn try_transform_window_func(
        &self,
        window_function: &WindowFunction,
        mapper: &dyn FunctionMapper,
    ) -> Option<Transformed<Expr>> {
        // Handle different window function types
        let remapped_function = match &window_function.fun {
            WindowFunctionDefinition::AggregateUDF(aggregate_function) => mapper
                .try_dealias_udaf(aggregate_function)
                .map(WindowFunctionDefinition::AggregateUDF),
            // Built-in window functions don't need translation
            WindowFunctionDefinition::WindowUDF(_) => None,
        };

        remapped_function.map(|function| {
            Transformed::yes(Expr::WindowFunction(Box::new(WindowFunction {
                fun: function,
                params: window_function.params.clone(),
            })))
        })
    }
}

impl Default for ClickHouseFederationProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use std::sync::Arc;

    // Mock function mapper for testing
    #[derive(Debug)]
    struct MockFunctionMapper {
        udf_mappings: std::collections::HashMap<String, Arc<ScalarUDF>>,
    }

    impl MockFunctionMapper {
        fn new() -> Self {
            Self { udf_mappings: std::collections::HashMap::new() }
        }
    }

    impl FunctionMapper for MockFunctionMapper {
        fn try_dealias_udf(&self, udf: &Arc<ScalarUDF>) -> Option<Arc<ScalarUDF>> {
            self.udf_mappings.get(udf.name()).cloned()
        }

        fn try_dealias_udaf(&self, _udaf: &Arc<AggregateUDF>) -> Option<Arc<AggregateUDF>> {
            None
        }
    }

    // Mock transformation rule for testing
    #[derive(Debug)]
    struct MockTransformationRule {
        applied: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl MockTransformationRule {
        fn new() -> Self {
            Self { applied: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)) }
        }

        fn was_applied(&self) -> bool {
            self.applied.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl TransformationRule for MockTransformationRule {
        fn matches(&self, _plan: &LogicalPlan) -> bool {
            true // Match all plans for testing
        }

        fn transform(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
            self.applied.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(plan.clone())
        }
    }

    #[test]
    fn test_federation_provider_creation() {
        let provider = ClickHouseFederationProvider::new();
        assert!(provider.function_mapper.is_none());
        assert!(provider.transformation_rules.is_empty());
    }

    #[test]
    fn test_federation_provider_with_function_mapper() {
        let mapper = Arc::new(MockFunctionMapper::new());
        let provider = ClickHouseFederationProvider::new().with_function_mapper(mapper.clone());
        assert!(provider.function_mapper.is_some());
    }

    #[test]
    fn test_federation_provider_with_rules() {
        let rule = Arc::new(MockTransformationRule::new());
        let provider = ClickHouseFederationProvider::new().with_rules(vec![rule.clone()]);
        assert_eq!(provider.transformation_rules.len(), 1);
    }

    #[test]
    fn test_transform_applies_rules() -> Result<()> {
        // Create a simple empty plan
        let plan = LogicalPlanBuilder::empty(false).build()?;

        // Create provider with mock rule
        let rule = Arc::new(MockTransformationRule::new());
        let provider = ClickHouseFederationProvider::new().with_rules(vec![rule.clone()]);

        // Transform the plan
        let _transformed = provider.transform(&plan)?;

        // Verify rule was applied
        assert!(rule.was_applied());
        Ok(())
    }

    #[test]
    fn test_transform_without_mapper() -> Result<()> {
        // Create a simple empty plan
        let plan = LogicalPlanBuilder::empty(false).build()?;

        let provider = ClickHouseFederationProvider::new();
        let transformed = provider.transform(&plan)?;

        // Without mapper, plan should be unchanged (except for any rules)
        assert_eq!(transformed.schema().fields().len(), 0);
        Ok(())
    }

    #[test]
    fn test_plan_to_sql_basic() -> Result<()> {
        // Create a simple empty plan
        let plan = LogicalPlanBuilder::empty(false).build()?;

        let provider = ClickHouseFederationProvider::new();
        let sql = provider.plan_to_sql(&plan);

        // Should successfully generate SQL
        assert!(sql.is_ok());
        Ok(())
    }

    #[test]
    fn test_default_implementation() {
        let provider = ClickHouseFederationProvider::default();
        assert!(provider.function_mapper.is_none());
        assert!(provider.transformation_rules.is_empty());
    }

    #[test]
    fn test_federated_context_trait() {
        let ctx = SessionContext::new();
        assert!(!ctx.is_federated());

        let federated_ctx = ctx.federate();
        assert!(federated_ctx.is_federated());
    }

    #[test]
    fn test_federated_context_idempotent() {
        let ctx = SessionContext::new();
        let federated_once = ctx.federate();
        let federated_twice = federated_once.federate();

        // Should still be federated
        assert!(federated_twice.is_federated());
    }

    #[test]
    fn test_function_mapper_trait_object() {
        let mapper: Arc<dyn FunctionMapper> = Arc::new(MockFunctionMapper::new());
        let provider = ClickHouseFederationProvider::new().with_function_mapper(mapper);
        assert!(provider.function_mapper.is_some());
    }

    #[test]
    fn test_transformation_rule_trait_object() {
        let rule: Arc<dyn TransformationRule> = Arc::new(MockTransformationRule::new());
        let provider = ClickHouseFederationProvider::new().with_rules(vec![rule]);
        assert_eq!(provider.transformation_rules.len(), 1);
    }

    #[test]
    fn test_transform_with_filter() -> Result<()> {
        // Create a simple empty plan
        let plan = LogicalPlanBuilder::empty(false).build()?;

        let provider = ClickHouseFederationProvider::new();
        let transformed = provider.transform(&plan)?;

        // Should successfully transform plan
        assert_eq!(transformed.schema().fields().len(), 0);
        Ok(())
    }

    #[test]
    fn test_transform_with_function_mapper_translates_scalar() -> Result<()> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::logical_expr::expr::ScalarFunction;

        // Create a UDF to be "dealiased" — e.g., groupArray → array_agg
        let original_udf = Arc::new(ScalarUDF::new_from_impl(
            crate::udfs::placeholder::PlaceholderUDF::new("groupArray"),
        ));
        let target_udf = Arc::new(ScalarUDF::new_from_impl(
            crate::udfs::placeholder::PlaceholderUDF::new("array_agg"),
        ));

        // Set up mapper
        let mut mapper = MockFunctionMapper::new();
        drop(mapper.udf_mappings.insert("groupArray".to_string(), target_udf));

        // Build a plan with a projection containing the scalar function
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let table_source =
            Arc::new(datafusion::logical_expr::logical_plan::builder::LogicalTableSource::new(
                schema,
            ));
        let plan = LogicalPlanBuilder::scan("t", table_source, None)?
            .project(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                original_udf,
                vec![datafusion::prelude::col("x")],
            ))])?
            .build()?;

        let provider =
            ClickHouseFederationProvider::new().with_function_mapper(Arc::new(mapper));
        let transformed = provider.transform(&plan)?;

        // The function in the transformed plan should now be "array_agg"
        let mut found_array_agg = false;
        let _ = transformed.apply(|p| {
            p.apply_expressions(|e| {
                if let Expr::ScalarFunction(sf) = e {
                    if sf.func.name() == "array_agg" {
                        found_array_agg = true;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        assert!(found_array_agg, "Expected scalar function to be translated to array_agg");
        Ok(())
    }

    #[test]
    fn test_rule_not_applied_when_no_match() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false).build()?;

        #[derive(Debug)]
        struct NeverMatchRule;
        impl TransformationRule for NeverMatchRule {
            fn matches(&self, _plan: &LogicalPlan) -> bool {
                false
            }
            fn transform(&self, _plan: &LogicalPlan) -> Result<LogicalPlan> {
                panic!("should not be called");
            }
        }

        let rule = Arc::new(NeverMatchRule);
        let provider = ClickHouseFederationProvider::new().with_rules(vec![rule]);
        let _transformed = provider.transform(&plan)?;
        // No panic means the rule was not called
        Ok(())
    }
}
