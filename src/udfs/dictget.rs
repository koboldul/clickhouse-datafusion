//! `DictGet` UDF for `ClickHouse` dictionary lookups.
//!
//! This module provides the `dictGet` function which is a placeholder UDF that allows
//! `DataFusion` to parse and plan queries containing `ClickHouse` dictionary lookups.
//! The actual execution happens on the `ClickHouse` server.

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Type alias for dictionary schema mapping.
/// Maps `dictionary_name` -> (`column_name` -> `DataType`)
pub type DictionarySchemaMap = HashMap<String, HashMap<String, DataType>>;

/// `DictGet` UDF implementation for `ClickHouse` dictionary lookups.
///
/// This UDF accepts variadic arguments in the following formats:
/// - 3 args: `dictGet(dictionary_name, column_name, key)`
/// - 4 args: `dictGet(dictionary_name, column_name, key, timestamp)` for dated dictionaries
///
/// The return type is determined by looking up the column type in the dictionary schema map.
#[derive(Debug)]
pub struct DictGet {
    signature: Signature,
    dictionary_schema: Arc<DictionarySchemaMap>,
}

impl PartialEq for DictGet {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for DictGet {}

impl std::hash::Hash for DictGet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl DictGet {
    /// Create a new `DictGet` UDF with the provided dictionary schema map.
    pub fn new(dictionary_schema: Arc<DictionarySchemaMap>) -> Self {
        Self {
            // Signature accepts 3 or 4 arguments
            // For dated dictionaries, the 4th argument is typically a Timestamp
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Timestamp(TimeUnit::Second, None),
                    ]),
                ],
                Volatility::Immutable,
            ),
            dictionary_schema,
        }
    }

    /// Extract the return data type from the function arguments.
    ///
    /// This looks up the dictionary name and column name from the scalar arguments
    /// and returns the appropriate `DataType` from the schema map.
    fn data_type_from_args(&self, args: &ReturnFieldArgs<'_>) -> Result<DataType> {
        let arg_types: Vec<_> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let scalar_arguments = args.scalar_arguments;

        if arg_types.len() != 3 && arg_types.len() != 4 {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "dictGet needs 3 or 4 arguments, {} provided",
                arg_types.len()
            )));
        }

        // Extract dictionary name from first argument
        let Some(Some(ScalarValue::Utf8(Some(dictionary_name)))) =
            scalar_arguments.first().map(|v| v.as_ref())
        else {
            match scalar_arguments.first() {
                None => {
                    return Err(datafusion::common::DataFusionError::Plan(format!(
                        "dictGet requires its first argument to be a constant string, got non-constant {:?}",
                        &arg_types[0]
                    )));
                }
                Some(scalar_value) => {
                    return Err(datafusion::common::DataFusionError::Plan(format!(
                        "dictGet requires its first argument to be a constant string, got constant {scalar_value:?}"
                    )));
                }
            }
        };

        // Extract column name from second argument
        let Some(Some(ScalarValue::Utf8(Some(column_name)))) =
            scalar_arguments.get(1).map(|v| v.as_ref())
        else {
            match scalar_arguments.get(1) {
                None => {
                    return Err(datafusion::common::DataFusionError::Plan(format!(
                        "dictGet requires its second argument to be a constant string, got non-constant {:?}",
                        &arg_types[1]
                    )));
                }
                Some(scalar_value) => {
                    return Err(datafusion::common::DataFusionError::Plan(format!(
                        "dictGet requires its second argument to be a constant string, got constant {scalar_value:?}"
                    )));
                }
            }
        };

        // Lookup in dictionary schema map
        if let Some(dictionary_schema) = self.dictionary_schema.get(dictionary_name) {
            if let Some(data_type) = dictionary_schema.get(column_name) {
                Ok(data_type.clone())
            } else {
                Err(datafusion::common::DataFusionError::Plan(format!(
                    "Column '{column_name}' not found in dictionary '{dictionary_name}'"
                )))
            }
        } else {
            Err(datafusion::common::DataFusionError::Plan(format!(
                "Dictionary '{dictionary_name}' not found in schema map"
            )))
        }
    }
}

impl ScalarUDFImpl for DictGet {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "dictGet"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion::common::plan_err!("dictGet should return type from return_type_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let return_type = self.data_type_from_args(&args)?;
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "dictGet is a placeholder UDF for ClickHouse execution - it should not be invoked in DataFusion"
        )
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

/// Create a `DictGet` UDF with the provided dictionary schema map.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use std::collections::HashMap;
/// use clickhouse_datafusion::udfs::dictget::{dict_get_udf, DictionarySchemaMap};
/// use datafusion::arrow::datatypes::DataType;
///
/// let mut schema_map = HashMap::new();
/// let mut dict_columns = HashMap::new();
/// dict_columns.insert("name".to_string(), DataType::Utf8);
/// dict_columns.insert("age".to_string(), DataType::Int32);
/// schema_map.insert("users".to_string(), dict_columns);
///
/// let udf = dict_get_udf(Arc::new(schema_map));
/// ```
pub fn dict_get_udf(schema: Arc<DictionarySchemaMap>) -> ScalarUDF {
    ScalarUDF::from(DictGet::new(schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ReturnFieldArgs;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_schema() -> Arc<DictionarySchemaMap> {
        let mut schema_map = HashMap::new();

        // Create a test dictionary with various column types
        let dict_columns = HashMap::from([
            ("name".to_string(), DataType::Utf8),
            ("age".to_string(), DataType::Int32),
            ("score".to_string(), DataType::Float64),
            ("active".to_string(), DataType::Boolean),
        ]);

        schema_map.insert("test_dict".to_string(), dict_columns);

        Arc::new(schema_map)
    }

    #[test]
    fn test_dict_get_creation() {
        let schema = create_test_schema();
        let dict_get = DictGet::new(schema.clone());

        assert_eq!(dict_get.name(), "dictGet");
    }

    #[test]
    fn test_dict_get_udf_creation() {
        let schema = create_test_schema();
        let udf = dict_get_udf(schema);

        assert_eq!(udf.name(), "dictGet");
    }

    #[test]
    fn test_data_type_from_args_utf8() {
        let schema = create_test_schema();
        let dict_get = DictGet::new(schema);

        let field1 = Arc::new(Field::new("dict", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("column", DataType::Utf8, false));
        let field3 = Arc::new(Field::new("key", DataType::Utf8, false));

        let scalar = [
            Some(ScalarValue::Utf8(Some("test_dict".to_string()))),
            Some(ScalarValue::Utf8(Some("name".to_string()))),
            Some(ScalarValue::Utf8(Some("key123".to_string()))),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &[field1, field2, field3],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref(), scalar[2].as_ref()],
        };

        let result = dict_get.data_type_from_args(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_data_type_from_args_int32() {
        let schema = create_test_schema();
        let dict_get = DictGet::new(schema);

        let field1 = Arc::new(Field::new("dict", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("column", DataType::Utf8, false));
        let field3 = Arc::new(Field::new("key", DataType::Utf8, false));

        let scalar = [
            Some(ScalarValue::Utf8(Some("test_dict".to_string()))),
            Some(ScalarValue::Utf8(Some("age".to_string()))),
            Some(ScalarValue::Utf8(Some("key123".to_string()))),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &[field1, field2, field3],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref(), scalar[2].as_ref()],
        };

        let result = dict_get.data_type_from_args(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Int32);
    }

    #[test]
    fn test_data_type_from_args_dictionary_not_found() {
        let schema = create_test_schema();
        let dict_get = DictGet::new(schema);

        let field1 = Arc::new(Field::new("dict", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("column", DataType::Utf8, false));
        let field3 = Arc::new(Field::new("key", DataType::Utf8, false));

        let scalar = [
            Some(ScalarValue::Utf8(Some("nonexistent_dict".to_string()))),
            Some(ScalarValue::Utf8(Some("name".to_string()))),
            Some(ScalarValue::Utf8(Some("key123".to_string()))),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &[field1, field2, field3],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref(), scalar[2].as_ref()],
        };

        let result = dict_get.data_type_from_args(&args);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("Dictionary 'nonexistent_dict' not found")
        );
    }

    #[test]
    fn test_data_type_from_args_column_not_found() {
        let schema = create_test_schema();
        let dict_get = DictGet::new(schema);

        let field1 = Arc::new(Field::new("dict", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("column", DataType::Utf8, false));
        let field3 = Arc::new(Field::new("key", DataType::Utf8, false));

        let scalar = [
            Some(ScalarValue::Utf8(Some("test_dict".to_string()))),
            Some(ScalarValue::Utf8(Some("nonexistent_column".to_string()))),
            Some(ScalarValue::Utf8(Some("key123".to_string()))),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &[field1, field2, field3],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref(), scalar[2].as_ref()],
        };

        let result = dict_get.data_type_from_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Column 'nonexistent_column' not found"));
    }

    #[test]
    fn test_data_type_from_args_invalid_arg_count() {
        let schema = create_test_schema();
        let dict_get = DictGet::new(schema);

        let field1 = Arc::new(Field::new("dict", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("column", DataType::Utf8, false));

        let scalar = [
            Some(ScalarValue::Utf8(Some("test_dict".to_string()))),
            Some(ScalarValue::Utf8(Some("name".to_string()))),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &[field1, field2],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref()],
        };

        let result = dict_get.data_type_from_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("dictGet needs 3 or 4 arguments"));
    }

    #[test]
    fn test_return_field_from_args() {
        let schema = create_test_schema();
        let dict_get = DictGet::new(schema);

        let field1 = Arc::new(Field::new("dict", DataType::Utf8, false));
        let field2 = Arc::new(Field::new("column", DataType::Utf8, false));
        let field3 = Arc::new(Field::new("key", DataType::Utf8, false));

        let scalar = [
            Some(ScalarValue::Utf8(Some("test_dict".to_string()))),
            Some(ScalarValue::Utf8(Some("score".to_string()))),
            Some(ScalarValue::Utf8(Some("key123".to_string()))),
        ];

        let args = ReturnFieldArgs {
            arg_fields: &[field1, field2, field3],
            scalar_arguments: &[scalar[0].as_ref(), scalar[1].as_ref(), scalar[2].as_ref()],
        };

        let result = dict_get.return_field_from_args(args);
        assert!(result.is_ok());
        let field = result.unwrap();
        assert_eq!(field.name(), "dictGet");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(field.is_nullable());
    }
}
