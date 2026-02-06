//! `ClickHouse` Context Extension for registering ClickHouse-specific functions in `DataFusion`.
//!
//! This module provides the [`ClickHouseContextExtension`] which aggregates all ClickHouse-specific
//! UDFs, UDAFs, and their aliases for easy registration into a `DataFusion` [`SessionContext`].

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::prelude::SessionContext;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::aliases::{create_alias_udaf, create_alias_udf};
use crate::udfs::{
    DictionarySchemaMap, arg_max_udaf, dict_get_udf, to_start_of_month_udf, to_start_of_week_udf,
};

/// Extension for integrating `ClickHouse` functions into `DataFusion`.
///
/// This struct aggregates all ClickHouse-specific scalar UDFs, aggregate UDAFs,
/// and their aliases. It provides convenient methods for registering these functions
/// into a `DataFusion` [`SessionContext`].
///
/// # Example
/// ```ignore
/// use clickhouse_datafusion::context::ClickHouseContextExtension;
/// use datafusion::prelude::SessionContext;
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let ctx = SessionContext::new();
/// let dictionary_schema = Arc::new(HashMap::new());
/// let extension = ClickHouseContextExtension::new(dictionary_schema);
///
/// extension.register_all(&ctx)?;
/// ```
#[derive(Debug, Clone)]
pub struct ClickHouseContextExtension {
    dictionary_schema: Arc<DictionarySchemaMap>,
}

impl ClickHouseContextExtension {
    /// Create a new `ClickHouse` context extension.
    ///
    /// # Arguments
    /// * `dictionary_schema` - Schema map for dictionary lookups used by `dictGet` function
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    /// use clickhouse_datafusion::context::ClickHouseContextExtension;
    ///
    /// let dictionary_schema = Arc::new(HashMap::new());
    /// let extension = ClickHouseContextExtension::new(dictionary_schema);
    /// ```
    pub fn new(dictionary_schema: Arc<DictionarySchemaMap>) -> Self {
        Self { dictionary_schema }
    }

    /// Get all `ClickHouse` scalar UDFs.
    ///
    /// Returns a vector of all scalar functions including:
    /// - `dictGet` - Dictionary lookup function
    /// - `toStartOfWeek` - Time function to get start of week
    /// - `toStartOfMonth` - Time function to get start of month
    /// - `clickhouse_eval` - Escape-hatch for `ClickHouse` SQL expressions
    /// - `clickhouse` - Wrapper for `ClickHouse` expressions
    /// - `apply` - Higher-order function support
    ///
    /// # Returns
    /// Vector of Arc-wrapped scalar UDFs
    #[allow(clippy::clone_on_ref_ptr)]
    pub fn get_udfs(&self) -> Vec<Arc<ScalarUDF>> {
        vec![
            Arc::new(dict_get_udf(self.dictionary_schema.clone())),
            Arc::new(to_start_of_week_udf()),
            Arc::new(to_start_of_month_udf()),
            Arc::new(crate::udfs::eval::clickhouse_eval_udf()),
            Arc::new(crate::udfs::clickhouse::clickhouse_udf()),
            Arc::new(crate::udfs::apply::clickhouse_apply_udf()),
        ]
    }

    /// Get all `ClickHouse` aggregate UDFs.
    ///
    /// Returns a vector of all aggregate functions including:
    /// - `argMax` - Returns the value of the first column for the row with the maximum value in the second column
    ///
    /// # Returns
    /// Vector of Arc-wrapped aggregate UDFs
    pub fn get_udafs(&self) -> Vec<Arc<AggregateUDF>> {
        vec![Arc::new(arg_max_udaf())]
    }

    /// Get function aliases mapping (`original_name` -> set of aliases).
    ///
    /// Returns mappings for common ClickHouse/DataFusion function name differences:
    /// - `array_agg` -> `groupArray`
    /// - `to_date` -> `toDate`
    ///
    /// # Returns
    /// `HashMap` mapping canonical function names to their `ClickHouse` aliases
    pub fn get_udf_aliases(&self) -> HashMap<String, HashSet<String>> {
        let mut aliases = HashMap::new();

        // array_agg <-> groupArray
        let mut group_array_aliases = HashSet::new();
        let _ = group_array_aliases.insert("groupArray".to_string());
        drop(aliases.insert("array_agg".to_string(), group_array_aliases));

        // to_date <-> toDate
        let mut to_date_aliases = HashSet::new();
        let _ = to_date_aliases.insert("toDate".to_string());
        drop(aliases.insert("to_date".to_string(), to_date_aliases));

        aliases
    }

    /// Get aggregate function aliases.
    ///
    /// Returns mappings for `ClickHouse` aggregate function name variations:
    /// - `argMax` -> `argmax`
    ///
    /// # Returns
    /// `HashMap` mapping canonical aggregate function names to their aliases
    pub fn get_udaf_aliases(&self) -> HashMap<String, HashSet<String>> {
        let mut aliases = HashMap::new();

        // argMax <-> argmax
        let mut argmax_aliases = HashSet::new();
        let _ = argmax_aliases.insert("argmax".to_string());
        drop(aliases.insert("argMax".to_string(), argmax_aliases));

        aliases
    }

    /// Register all `ClickHouse` extensions into a `SessionContext`.
    ///
    /// This method registers:
    /// 1. All scalar UDFs returned by [`get_udfs`](Self::get_udfs)
    /// 2. All aggregate UDAFs returned by [`get_udafs`](Self::get_udafs)
    /// 3. All function aliases for both scalar and aggregate functions
    ///
    /// # Arguments
    /// * `ctx` - The `DataFusion` session context to register functions in
    ///
    /// # Returns
    /// `Result<()>` - Ok if all functions were registered successfully
    ///
    /// # Example
    /// ```ignore
    /// use clickhouse_datafusion::context::ClickHouseContextExtension;
    /// use datafusion::prelude::SessionContext;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let ctx = SessionContext::new();
    /// let dictionary_schema = Arc::new(HashMap::new());
    /// let extension = ClickHouseContextExtension::new(dictionary_schema);
    ///
    /// extension.register_all(&ctx)?;
    /// # Ok::<(), datafusion::error::DataFusionError>(())
    /// ```
    /// # Errors
    /// Returns an error if any function registration fails
    pub fn register_all(&self, ctx: &SessionContext) -> Result<()> {
        // Register scalar UDFs
        for udf in self.get_udfs() {
            ctx.register_udf((*udf).clone());
        }

        // Register aggregate UDAFs
        for udaf in self.get_udafs() {
            ctx.register_udaf((*udaf).clone());
        }

        // Register scalar function aliases
        for (original, aliases) in self.get_udf_aliases() {
            #[allow(clippy::clone_on_ref_ptr)]
            if let Ok(udf) = ctx.udf(&original) {
                for alias in aliases {
                    ctx.register_udf(create_alias_udf(udf.clone(), alias));
                }
            }
        }

        // Register aggregate function aliases
        for (original, aliases) in self.get_udaf_aliases() {
            #[allow(clippy::clone_on_ref_ptr)]
            if let Ok(udaf) = ctx.udaf(&original) {
                for alias in aliases {
                    ctx.register_udaf(create_alias_udaf(udaf.clone(), alias));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    fn create_test_extension() -> ClickHouseContextExtension {
        let dictionary_schema = Arc::new(HashMap::new());
        ClickHouseContextExtension::new(dictionary_schema)
    }

    #[test]
    fn test_new_extension() {
        let dictionary_schema = Arc::new(HashMap::new());
        let extension = ClickHouseContextExtension::new(dictionary_schema.clone());
        assert!(Arc::ptr_eq(&extension.dictionary_schema, &dictionary_schema));
    }

    #[test]
    fn test_get_udfs() {
        let extension = create_test_extension();
        let udfs = extension.get_udfs();

        // Should have 6 UDFs: dictGet, toStartOfWeek, toStartOfMonth, clickhouse_eval, clickhouse, apply
        assert_eq!(udfs.len(), 6);

        let names: Vec<&str> = udfs.iter().map(|udf| udf.name()).collect();
        assert!(names.contains(&"dictGet"));
        assert!(names.contains(&"toStartOfWeek"));
        assert!(names.contains(&"toStartOfMonth"));
        assert!(names.contains(&"clickhouse_eval"));
        assert!(names.contains(&"clickhouse"));
        assert!(names.contains(&"apply"));
    }

    #[test]
    fn test_get_udafs() {
        let extension = create_test_extension();
        let udafs = extension.get_udafs();

        // Should have 1 UDAF: argMax
        assert_eq!(udafs.len(), 1);
        assert_eq!(udafs[0].name(), "argMax");
    }

    #[test]
    fn test_get_udf_aliases() {
        let extension = create_test_extension();
        let aliases = extension.get_udf_aliases();

        // Should have aliases for array_agg and to_date
        assert_eq!(aliases.len(), 2);
        assert!(aliases.contains_key("array_agg"));
        assert!(aliases.contains_key("to_date"));

        // Check array_agg aliases
        let array_agg_aliases = &aliases["array_agg"];
        assert!(array_agg_aliases.contains("groupArray"));

        // Check to_date aliases
        let to_date_aliases = &aliases["to_date"];
        assert!(to_date_aliases.contains("toDate"));
    }

    #[test]
    fn test_get_udaf_aliases() {
        let extension = create_test_extension();
        let aliases = extension.get_udaf_aliases();

        // Should have aliases for argMax
        assert_eq!(aliases.len(), 1);
        assert!(aliases.contains_key("argMax"));

        // Check argMax aliases
        let argmax_aliases = &aliases["argMax"];
        assert!(argmax_aliases.contains("argmax"));
    }

    #[test]
    fn test_register_all() {
        let ctx = SessionContext::new();
        let extension = create_test_extension();

        let result = extension.register_all(&ctx);
        assert!(result.is_ok());

        // Verify scalar UDFs are registered
        let state = ctx.state();
        let functions = state.scalar_functions();
        assert!(functions.contains_key("dictGet"));
        assert!(functions.contains_key("toStartOfWeek"));
        assert!(functions.contains_key("toStartOfMonth"));
        assert!(functions.contains_key("clickhouse_eval"));
        assert!(functions.contains_key("clickhouse"));
        assert!(functions.contains_key("apply"));

        // Verify aggregate UDAFs are registered
        let aggregate_functions = state.aggregate_functions();
        assert!(aggregate_functions.contains_key("argMax"));
    }

    #[test]
    fn test_register_all_with_aliases() {
        let ctx = SessionContext::new();

        let extension = create_test_extension();
        let result = extension.register_all(&ctx);
        assert!(result.is_ok());

        let state = ctx.state();

        // Verify ClickHouse-specific functions are registered
        let functions = state.scalar_functions();
        assert!(functions.contains_key("dictGet"), "dictGet should be registered");
        assert!(functions.contains_key("toStartOfWeek"), "toStartOfWeek should be registered");
        assert!(functions.contains_key("toStartOfMonth"), "toStartOfMonth should be registered");

        // Verify aggregate function and its alias are registered
        let aggregate_functions = state.aggregate_functions();
        assert!(aggregate_functions.contains_key("argMax"), "argMax should be registered");
        assert!(aggregate_functions.contains_key("argmax"), "argmax alias should be registered");
    }

    #[test]
    fn test_clone() {
        let extension = create_test_extension();
        let cloned = extension.clone();

        // Verify the dictionary schema is the same Arc
        assert!(Arc::ptr_eq(&extension.dictionary_schema, &cloned.dictionary_schema));
    }
}
