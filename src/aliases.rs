//! Function aliasing utilities for scalar and aggregate functions.
//!
//! This module provides wrappers to create aliases for existing UDFs and UDAFs,
//! allowing the same function implementation to be registered under multiple names.

use datafusion::common::{Result, internal_err};
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Wrapper to provide function aliasing for scalar functions.
///
/// This struct wraps an existing [`ScalarUDF`] and provides an alternative name
/// for the same function implementation. All method calls are delegated to the
/// inner implementation, except for [`name()`](ScalarUDFImpl::name) which returns
/// the alias name.
#[derive(Debug)]
pub struct AliasUdf {
    scalar_udf_impl: Arc<dyn ScalarUDFImpl>,
    alias_name: String,
}

impl PartialEq for AliasUdf {
    fn eq(&self, other: &Self) -> bool {
        self.alias_name == other.alias_name
    }
}

impl Eq for AliasUdf {}

impl std::hash::Hash for AliasUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.alias_name.hash(state);
    }
}

impl AliasUdf {
    /// Create a new aliased scalar function.
    ///
    /// # Arguments
    /// * `inner` - The original scalar UDF to create an alias for
    /// * `alias_name` - The alternative name for this function
    #[allow(clippy::needless_pass_by_value, clippy::clone_on_ref_ptr)]
    pub fn new(inner: Arc<ScalarUDF>, alias_name: impl Into<String>) -> Self {
        Self { scalar_udf_impl: inner.inner().clone(), alias_name: alias_name.into() }
    }
}

impl ScalarUDFImpl for AliasUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.alias_name
    }

    fn signature(&self) -> &Signature {
        self.scalar_udf_impl.signature()
    }

    fn return_type(
        &self,
        arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> Result<datafusion::arrow::datatypes::DataType> {
        self.scalar_udf_impl.return_type(arg_types)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        self.scalar_udf_impl.invoke_with_args(args)
    }
}

/// Wrapper to provide function aliasing for aggregate functions.
///
/// This struct wraps an existing [`AggregateUDF`] and provides an alternative name
/// for the same function implementation. All method calls are delegated to the
/// inner implementation, except for [`name()`](AggregateUDFImpl::name) which returns
/// the alias name.
#[derive(Debug)]
pub struct AliasUdaf {
    aggregate_udf_impl: Arc<dyn AggregateUDFImpl>,
    alias_name: String,
}

impl PartialEq for AliasUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.alias_name == other.alias_name
    }
}

impl Eq for AliasUdaf {}

impl std::hash::Hash for AliasUdaf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.alias_name.hash(state);
    }
}

impl AliasUdaf {
    /// Create a new aliased aggregate function.
    ///
    /// # Arguments
    /// * `inner` - The original aggregate UDF to create an alias for
    /// * `alias_name` - The alternative name for this function
    #[allow(clippy::needless_pass_by_value, clippy::clone_on_ref_ptr)]
    pub fn new(inner: Arc<AggregateUDF>, alias_name: impl Into<String>) -> Self {
        Self { aggregate_udf_impl: inner.inner().clone(), alias_name: alias_name.into() }
    }
}

impl AggregateUDFImpl for AliasUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.alias_name
    }

    fn signature(&self) -> &Signature {
        self.aggregate_udf_impl.signature()
    }

    fn return_type(
        &self,
        arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> Result<datafusion::arrow::datatypes::DataType> {
        self.aggregate_udf_impl.return_type(arg_types)
    }

    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs<'_>,
    ) -> Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        internal_err!(
            "This is a placeholder struct for Datafusion to be able to parse and process external functions"
        )
    }
}

/// Create an aliased scalar function.
///
/// This is a convenience function that creates a new [`ScalarUDF`] with an alias name,
/// delegating all implementation to the provided inner function.
///
/// # Arguments
/// * `inner` - The original scalar UDF to create an alias for
/// * `alias` - The alternative name for this function
///
/// # Example
/// ```ignore
/// use clickhouse_datafusion::aliases::create_alias_udf;
/// use std::sync::Arc;
/// use datafusion::logical_expr::ScalarUDF;
///
/// let original = Arc::new(ScalarUDF::from(MyUdf::new()));
/// let aliased = create_alias_udf(original, "my_alias");
/// ctx.register_udf(aliased);
/// ```
pub fn create_alias_udf(inner: Arc<ScalarUDF>, alias: impl Into<String>) -> ScalarUDF {
    ScalarUDF::from(AliasUdf::new(inner, alias))
}

/// Create an aliased aggregate function.
///
/// This is a convenience function that creates a new [`AggregateUDF`] with an alias name,
/// delegating all implementation to the provided inner function.
///
/// # Arguments
/// * `inner` - The original aggregate UDF to create an alias for
/// * `alias` - The alternative name for this function
///
/// # Example
/// ```ignore
/// use clickhouse_datafusion::aliases::create_alias_udaf;
/// use std::sync::Arc;
/// use datafusion::logical_expr::AggregateUDF;
///
/// let original = Arc::new(AggregateUDF::from(MyUdaf::new()));
/// let aliased = create_alias_udaf(original, "my_alias");
/// ctx.register_udaf(aliased);
/// ```
pub fn create_alias_udaf(inner: Arc<AggregateUDF>, alias: impl Into<String>) -> AggregateUDF {
    AggregateUDF::from(AliasUdaf::new(inner, alias))
}

/// Register a scalar UDF with multiple aliases.
///
/// Registers the original function and creates alias registrations for each provided alias name.
///
/// # Arguments
/// * `ctx` - The session context to register functions in
/// * `udf` - The scalar UDF to register
/// * `aliases` - Array of alias names to register
///
/// # Example
/// ```ignore
/// use clickhouse_datafusion::aliases::register_udf_with_aliases;
/// use std::sync::Arc;
/// use datafusion::logical_expr::ScalarUDF;
///
/// let udf = Arc::new(ScalarUDF::from(MyUdf::new()));
/// register_udf_with_aliases(&ctx, udf, &["alias1", "alias2"])?;
/// ```
///
/// # Errors
/// Returns an error if function registration fails
#[allow(clippy::needless_pass_by_value, clippy::clone_on_ref_ptr)]
pub fn register_udf_with_aliases(
    ctx: &SessionContext,
    udf: Arc<ScalarUDF>,
    aliases: &[&str],
) -> Result<()> {
    ctx.register_udf((*udf).clone());
    for alias in aliases {
        ctx.register_udf(create_alias_udf(udf.clone(), *alias));
    }
    Ok(())
}

/// Register an aggregate UDF with multiple aliases.
///
/// Registers the original function and creates alias registrations for each provided alias name.
///
/// # Arguments
/// * `ctx` - The session context to register functions in
/// * `udaf` - The aggregate UDF to register
/// * `aliases` - Array of alias names to register
///
/// # Example
/// ```ignore
/// use clickhouse_datafusion::aliases::register_udaf_with_aliases;
/// use std::sync::Arc;
/// use datafusion::logical_expr::AggregateUDF;
///
/// let udaf = Arc::new(AggregateUDF::from(MyUdaf::new()));
/// register_udaf_with_aliases(&ctx, udaf, &["alias1", "alias2"])?;
/// ```
///
/// # Errors
/// Returns an error if function registration fails
#[allow(clippy::needless_pass_by_value, clippy::clone_on_ref_ptr)]
pub fn register_udaf_with_aliases(
    ctx: &SessionContext,
    udaf: Arc<AggregateUDF>,
    aliases: &[&str],
) -> Result<()> {
    ctx.register_udaf((*udaf).clone());
    for alias in aliases {
        ctx.register_udaf(create_alias_udaf(udaf.clone(), *alias));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{ColumnarValue, Volatility};

    // Simple test UDF that returns a constant value
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestUdf {
        signature: Signature,
    }

    impl TestUdf {
        fn new() -> Self {
            Self { signature: Signature::exact(vec![], Volatility::Immutable) }
        }
    }

    impl ScalarUDFImpl for TestUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "test_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(datafusion::common::ScalarValue::Int32(Some(42))))
        }
    }

    #[test]
    fn test_alias_udf_name() {
        let original = Arc::new(ScalarUDF::from(TestUdf::new()));
        let aliased = create_alias_udf(original.clone(), "aliased_name");

        assert_eq!(original.name(), "test_udf");
        assert_eq!(aliased.name(), "aliased_name");
    }

    #[test]
    fn test_alias_udf_signature_delegation() {
        let original = Arc::new(ScalarUDF::from(TestUdf::new()));
        let aliased = create_alias_udf(original.clone(), "aliased_name");

        assert_eq!(format!("{:?}", original.signature()), format!("{:?}", aliased.signature()));
    }

    #[test]
    fn test_register_udf_with_aliases() {
        let ctx = SessionContext::new();
        let udf = Arc::new(ScalarUDF::from(TestUdf::new()));

        register_udf_with_aliases(&ctx, udf, &["alias1", "alias2"]).unwrap();

        // Verify all names are registered
        assert!(ctx.state().scalar_functions().contains_key("test_udf"));
        assert!(ctx.state().scalar_functions().contains_key("alias1"));
        assert!(ctx.state().scalar_functions().contains_key("alias2"));
    }
}
