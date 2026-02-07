//! Dictionary Catalog Provider for `ClickHouse` dictionaries.
//!
//! This module provides catalog and schema providers for managing `ClickHouse` dictionary metadata
//! within `DataFusion`'s catalog system. Dictionaries are special lookup tables in `ClickHouse` that
//! provide efficient key-value lookups.

use dashmap::DashMap;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::common::exec_datafusion_err;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata describing a `ClickHouse` dictionary.
#[derive(Debug, Clone)]
pub struct DictionaryMetadata {
    /// The name of the dictionary
    pub name: String,
    /// Column names mapped to their data types
    pub columns: HashMap<String, DataType>,
    /// The dictionary layout type
    pub layout: DictionaryLayout,
}

impl DictionaryMetadata {
    /// Create a new dictionary metadata instance.
    pub fn new(name: String, columns: HashMap<String, DataType>, layout: DictionaryLayout) -> Self {
        Self { name, columns, layout }
    }

    /// Get the schema for this dictionary.
    pub fn schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
            .collect();
        Arc::new(Schema::new(fields))
    }
}

/// Dictionary layout types supported by `ClickHouse`.
#[derive(Debug, Clone)]
pub enum DictionaryLayout {
    /// Hashed dictionary with a single primary key column.
    Hashed {
        /// Name of the primary key column
        primary_key: String,
    },
    /// Range hashed dictionary with time-based lookups.
    Range {
        /// Name of the primary key column
        primary_key: String,
        /// Name of the start time column
        start_time: String,
        /// Name of the end time column
        end_time: String,
    },
    /// Flat dictionary - stored in array
    Flat {
        /// Name of the primary key column
        primary_key: String,
    },
    /// Complex key hashed dictionary
    ComplexKeyHashed {
        /// Names of the key columns
        key_columns: Vec<String>,
    },
}

impl DictionaryLayout {
    /// Get the primary key column name(s) for this layout.
    pub fn key_columns(&self) -> Vec<&str> {
        match self {
            DictionaryLayout::Hashed { primary_key }
            | DictionaryLayout::Flat { primary_key }
            | DictionaryLayout::Range { primary_key, .. } => vec![primary_key.as_str()],
            DictionaryLayout::ComplexKeyHashed { key_columns } => {
                key_columns.iter().map(String::as_str).collect()
            }
        }
    }
}

/// A placeholder table provider for dictionary tables.
///
/// Since dictionaries are executed on `ClickHouse`, this provider only provides schema information
/// and does not support actual data scanning in `DataFusion`.
#[derive(Debug)]
struct DictionaryTableProvider {
    schema: SchemaRef,
    metadata: Arc<DictionaryMetadata>,
}

impl DictionaryTableProvider {
    fn new(metadata: Arc<DictionaryMetadata>) -> Self {
        let schema = metadata.schema();
        Self { schema, metadata }
    }
}

#[async_trait::async_trait]
impl TableProvider for DictionaryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(clippy::clone_on_ref_ptr)]
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(exec_datafusion_err!(
            "Dictionary '{}' is a ClickHouse dictionary and cannot be scanned directly in DataFusion. \
             Use dictGet() function for lookups.",
            self.metadata.name
        ))
    }
}

/// Schema provider that manages dictionary tables within a schema.
#[derive(Debug)]
pub struct DictionarySchemaProvider {
    dictionaries: HashMap<String, Arc<DictionaryMetadata>>,
}

impl DictionarySchemaProvider {
    /// Create a new dictionary schema provider.
    pub fn new(dictionaries: HashMap<String, Arc<DictionaryMetadata>>) -> Self {
        Self { dictionaries }
    }

    /// Add a dictionary to this schema.
    pub fn add_dictionary(&mut self, metadata: Arc<DictionaryMetadata>) {
        drop(self.dictionaries.insert(metadata.name.clone(), metadata));
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DictionarySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.dictionaries.keys().cloned().collect()
    }

    #[allow(clippy::clone_on_ref_ptr)]
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .dictionaries
            .get(name)
            .map(|metadata| Arc::new(DictionaryTableProvider::new(metadata.clone())) as _))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.dictionaries.contains_key(name)
    }
}

/// Catalog provider for managing dictionary schemas.
#[derive(Debug)]
pub struct DictionaryCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl DictionaryCatalogProvider {
    /// Create a new dictionary catalog provider.
    pub fn new() -> Self {
        Self { schemas: DashMap::new() }
    }

    /// Register a schema with this catalog.
    pub fn register_schema(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.insert(name.into(), schema)
    }
}

impl Default for DictionaryCatalogProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogProvider for DictionaryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|entry| entry.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|entry| Arc::clone(entry.value()))
    }
}

/// Catalog list implementation for dictionary catalogs.
#[derive(Debug)]
pub struct DictionaryCatalogList {
    catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl DictionaryCatalogList {
    /// Create a new dictionary catalog list.
    pub fn new() -> Self {
        Self { catalogs: DashMap::new() }
    }

    /// Register a catalog with this list.
    pub fn register_catalog(
        &self,
        name: impl Into<String>,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name.into(), catalog)
    }
}

impl Default for DictionaryCatalogList {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogProviderList for DictionaryCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|entry| entry.key().clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|entry| Arc::clone(entry.value()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;

    fn create_test_dictionary() -> DictionaryMetadata {
        let columns = HashMap::from([
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::Utf8),
            ("value".to_string(), DataType::Float64),
        ]);

        DictionaryMetadata::new(
            "test_dict".to_string(),
            columns,
            DictionaryLayout::Hashed { primary_key: "id".to_string() },
        )
    }

    #[test]
    fn test_dictionary_metadata_creation() {
        let dict = create_test_dictionary();
        assert_eq!(dict.name, "test_dict");
        assert_eq!(dict.columns.len(), 3);
        assert!(dict.columns.contains_key("id"));
        assert!(dict.columns.contains_key("name"));
        assert!(dict.columns.contains_key("value"));
    }

    #[test]
    fn test_dictionary_schema() {
        let dict = create_test_dictionary();
        let schema = dict.schema();
        assert_eq!(schema.fields().len(), 3);
    }

    #[test]
    fn test_dictionary_layout_hashed() {
        let layout = DictionaryLayout::Hashed { primary_key: "id".to_string() };
        let keys = layout.key_columns();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "id");
    }

    #[test]
    fn test_dictionary_layout_range() {
        let layout = DictionaryLayout::Range {
            primary_key: "id".to_string(),
            start_time: "start".to_string(),
            end_time: "end".to_string(),
        };
        let keys = layout.key_columns();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "id");
    }

    #[test]
    fn test_dictionary_layout_complex_key() {
        let layout = DictionaryLayout::ComplexKeyHashed {
            key_columns: vec!["id1".to_string(), "id2".to_string()],
        };
        let keys = layout.key_columns();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], "id1");
        assert_eq!(keys[1], "id2");
    }

    #[test]
    fn test_dictionary_schema_provider() {
        let dict = Arc::new(create_test_dictionary());
        let mut dictionaries = HashMap::new();
        drop(dictionaries.insert("test_dict".to_string(), dict.clone()));

        let provider = DictionarySchemaProvider::new(dictionaries);
        assert_eq!(provider.table_names().len(), 1);
        assert!(provider.table_exist("test_dict"));
        assert!(!provider.table_exist("nonexistent"));
    }

    #[test]
    fn test_dictionary_schema_provider_add() {
        let dict1 = Arc::new(create_test_dictionary());
        let mut provider = DictionarySchemaProvider::new(HashMap::new());

        provider.add_dictionary(dict1);
        assert_eq!(provider.table_names().len(), 1);
        assert!(provider.table_exist("test_dict"));
    }

    #[test]
    fn test_dictionary_catalog_provider() {
        let catalog = DictionaryCatalogProvider::new();
        assert_eq!(catalog.schema_names().len(), 0);

        let schema_provider: Arc<dyn SchemaProvider> =
            Arc::new(DictionarySchemaProvider::new(HashMap::new()));
        drop(catalog.register_schema("default", schema_provider));

        assert_eq!(catalog.schema_names().len(), 1);
        assert!(catalog.schema("default").is_some());
        assert!(catalog.schema("nonexistent").is_none());
    }

    #[test]
    fn test_dictionary_catalog_list() {
        let catalog_list = DictionaryCatalogList::new();
        assert_eq!(catalog_list.catalog_names().len(), 0);

        let catalog: Arc<dyn CatalogProvider> = Arc::new(DictionaryCatalogProvider::new());
        drop(catalog_list.register_catalog("dictionaries".to_string(), catalog));

        assert_eq!(catalog_list.catalog_names().len(), 1);
        assert!(catalog_list.catalog("dictionaries").is_some());
        assert!(catalog_list.catalog("nonexistent").is_none());
    }

    #[test]
    fn test_dictionary_layout_flat() {
        let layout = DictionaryLayout::Flat { primary_key: "id".to_string() };
        let keys = layout.key_columns();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "id");
    }

    #[tokio::test]
    async fn test_dictionary_schema_provider_table() {
        let dict = Arc::new(create_test_dictionary());
        let mut dictionaries = HashMap::new();
        drop(dictionaries.insert("test_dict".to_string(), dict));

        let provider = DictionarySchemaProvider::new(dictionaries);

        // Should return a table provider for existing dictionary
        let table = provider.table("test_dict").await.unwrap();
        assert!(table.is_some());
        let table = table.unwrap();
        assert_eq!(table.schema().fields().len(), 3);
        assert_eq!(table.table_type(), TableType::Base);

        // Should return None for non-existent dictionary
        let table = provider.table("nonexistent").await.unwrap();
        assert!(table.is_none());
    }

    #[tokio::test]
    async fn test_dictionary_table_provider_scan_returns_error() {
        let dict = Arc::new(create_test_dictionary());
        let mut dictionaries = HashMap::new();
        drop(dictionaries.insert("test_dict".to_string(), dict));

        let provider = DictionarySchemaProvider::new(dictionaries);
        let table = provider.table("test_dict").await.unwrap().unwrap();

        // scan() should return an error since dictionaries can't be scanned directly
        let ctx = datafusion::prelude::SessionContext::new();
        let result = table.scan(&ctx.state(), None, &[], None).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cannot be scanned directly"),
            "Expected 'cannot be scanned directly' in error: {err_msg}"
        );
    }

    #[test]
    fn test_dictionary_catalog_provider_default() {
        let catalog = DictionaryCatalogProvider::default();
        assert_eq!(catalog.schema_names().len(), 0);
    }

    #[test]
    fn test_dictionary_catalog_list_default() {
        let catalog_list = DictionaryCatalogList::default();
        assert_eq!(catalog_list.catalog_names().len(), 0);
    }

    #[test]
    fn test_dictionary_catalog_provider_register_replaces() {
        let catalog = DictionaryCatalogProvider::new();
        let schema1: Arc<dyn SchemaProvider> =
            Arc::new(DictionarySchemaProvider::new(HashMap::new()));
        let schema2: Arc<dyn SchemaProvider> =
            Arc::new(DictionarySchemaProvider::new(HashMap::new()));

        // First registration returns None
        let prev = catalog.register_schema("default", schema1);
        assert!(prev.is_none());

        // Second registration returns the previous provider
        let prev = catalog.register_schema("default", schema2);
        assert!(prev.is_some());
    }
}
