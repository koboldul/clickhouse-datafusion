//! `ClickHouse` aggregate function UDAFs for `DataFusion`.
//!
//! These are placeholder UDAFs that allow `DataFusion` to parse and plan queries
//! containing ClickHouse-specific aggregate functions. The actual execution happens
//! on the `ClickHouse` server.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, internal_err};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, Volatility, function::AccumulatorArgs,
};
use std::any::Any;

/// `ClickHouse` `argMax` aggregate function.
///
/// This is a placeholder UDAF for `ClickHouse`'s `argMax` function.
/// It allows `DataFusion` to parse and process the function, but actual
/// execution is delegated to `ClickHouse`.
///
/// `argMax(value, key)` returns the value corresponding to the maximum key.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ArgMax {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArgMax {
    pub fn new() -> Self {
        Self {
            // argMax takes two arguments: (value, key)
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("argmax")],
        }
    }
}

impl Default for ArgMax {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ArgMax {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "argMax"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // argMax returns the type of the first argument (value)
        if let Some(first) = arg_types.first() {
            Ok(first.clone())
        } else {
            datafusion::common::plan_err!("argMax expects at least one argument")
        }
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        internal_err!(
            "argMax is a placeholder UDAF for ClickHouse execution. \
             It should not be invoked in DataFusion."
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Create an `argMax` UDAF.
pub fn arg_max_udaf() -> datafusion::logical_expr::AggregateUDF {
    datafusion::logical_expr::AggregateUDF::from(ArgMax::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn test_arg_max_name() {
        let udaf = ArgMax::new();
        assert_eq!(udaf.name(), "argMax");
    }

    #[test]
    fn test_arg_max_signature() {
        let udaf = ArgMax::new();
        let sig = udaf.signature();
        // Should accept any 2 arguments
        assert_eq!(format!("{:?}", sig.type_signature), "Any(2)");
    }

    #[test]
    fn test_arg_max_return_type() {
        let udaf = ArgMax::new();
        // Should return the type of the first argument
        let result = udaf.return_type(&[DataType::Int64, DataType::Int32]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Int64);

        let result2 = udaf.return_type(&[DataType::Utf8, DataType::Float64]);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_arg_max_return_type_no_args() {
        let udaf = ArgMax::new();
        let result = udaf.return_type(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expects at least one argument"));
    }

    #[test]
    fn test_arg_max_aliases() {
        let udaf = ArgMax::new();
        assert_eq!(udaf.aliases(), &[String::from("argmax")]);
    }

    #[test]
    fn test_arg_max_udaf() {
        let udaf = arg_max_udaf();
        assert_eq!(udaf.name(), "argMax");
    }

    #[test]
    fn test_arg_max_default() {
        let udaf = ArgMax::default();
        assert_eq!(udaf.name(), "argMax");
    }

    #[test]
    fn test_arg_max_accumulator_returns_error() {
        use std::sync::Arc;
        use datafusion::logical_expr::function::AccumulatorArgs;

        let udaf = ArgMax::new();
        let schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("val", DataType::Int64, false),
            datafusion::arrow::datatypes::Field::new("key", DataType::Int32, false),
        ]);
        let return_field = Arc::new(
            datafusion::arrow::datatypes::Field::new("argMax", DataType::Int64, true),
        );
        let acc_args = AccumulatorArgs {
            return_field: return_field.clone(),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            name: "argMax",
            is_distinct: false,
            is_reversed: false,
            exprs: &[],
            expr_fields: &[],
        };
        let result = udaf.accumulator(acc_args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("placeholder"));
    }
}
