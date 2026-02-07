//! `ClickHouse` time function UDFs for `DataFusion`.
//!
//! These are placeholder UDFs that allow `DataFusion` to parse and plan queries
//! containing ClickHouse-specific time functions. The actual execution happens
//! on the `ClickHouse` server.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Result, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;

/// `ClickHouse` `toStartOfWeek` function.
///
/// This is a placeholder UDF for `ClickHouse`'s `toStartOfWeek` function.
/// It allows `DataFusion` to parse and process the function, but actual
/// execution is delegated to `ClickHouse`.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ToStartOfWeek {
    signature: Signature,
}

impl ToStartOfWeek {
    pub fn new() -> Self {
        Self {
            // Accepts timestamp types
            signature: Signature::exact(
                vec![DataType::Timestamp(TimeUnit::Second, None)],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ToStartOfWeek {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ToStartOfWeek {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "toStartOfWeek"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Returns a timestamp with millisecond precision
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "toStartOfWeek is a placeholder UDF for ClickHouse execution. \
             It should not be invoked in DataFusion."
        )
    }
}

/// `ClickHouse` `toStartOfMonth` function.
///
/// This is a placeholder UDF for `ClickHouse`'s `toStartOfMonth` function.
/// It allows `DataFusion` to parse and process the function, but actual
/// execution is delegated to `ClickHouse`.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ToStartOfMonth {
    signature: Signature,
}

impl ToStartOfMonth {
    pub fn new() -> Self {
        Self {
            // Accepts timestamp types
            signature: Signature::exact(
                vec![DataType::Timestamp(TimeUnit::Second, None)],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ToStartOfMonth {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ToStartOfMonth {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "toStartOfMonth"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Returns a timestamp with millisecond precision
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "toStartOfMonth is a placeholder UDF for ClickHouse execution. \
             It should not be invoked in DataFusion."
        )
    }
}

/// Create a `toStartOfWeek` UDF.
pub fn to_start_of_week_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::from(ToStartOfWeek::new())
}

/// Create a `toStartOfMonth` UDF.
pub fn to_start_of_month_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::from(ToStartOfMonth::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_to_start_of_week_name() {
        let udf = ToStartOfWeek::new();
        assert_eq!(udf.name(), "toStartOfWeek");
    }

    #[test]
    fn test_to_start_of_week_return_type() {
        let udf = ToStartOfWeek::new();
        let result = udf.return_type(&[DataType::Timestamp(TimeUnit::Second, None)]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn test_to_start_of_month_name() {
        let udf = ToStartOfMonth::new();
        assert_eq!(udf.name(), "toStartOfMonth");
    }

    #[test]
    fn test_to_start_of_month_return_type() {
        let udf = ToStartOfMonth::new();
        let result = udf.return_type(&[DataType::Timestamp(TimeUnit::Second, None)]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn test_to_start_of_week_udf() {
        let udf = to_start_of_week_udf();
        assert_eq!(udf.name(), "toStartOfWeek");
    }

    #[test]
    fn test_to_start_of_month_udf() {
        let udf = to_start_of_month_udf();
        assert_eq!(udf.name(), "toStartOfMonth");
    }

    #[test]
    fn test_to_start_of_week_default() {
        let udf = ToStartOfWeek::default();
        assert_eq!(udf.name(), "toStartOfWeek");
    }

    #[test]
    fn test_to_start_of_month_default() {
        let udf = ToStartOfMonth::default();
        assert_eq!(udf.name(), "toStartOfMonth");
    }

    #[test]
    fn test_to_start_of_week_invoke_returns_error() {
        let udf = ToStartOfWeek::new();
        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "test",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("placeholder UDF"));
    }

    #[test]
    fn test_to_start_of_month_invoke_returns_error() {
        let udf = ToStartOfMonth::new();
        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "test",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("placeholder UDF"));
    }
}
