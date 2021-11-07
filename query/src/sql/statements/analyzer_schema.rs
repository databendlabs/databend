use common_datavalues::{DataSchemaRef, DataField};
use std::fmt::Debug;

pub struct AnalyzedSchema {
    columns: Vec<DataField>,
    name_prefix_parts: Vec<String>,
}

impl AnalyzedSchema {
    pub fn unnamed_schema(schema: DataSchemaRef) -> AnalyzedSchema {
        AnalyzedSchema {
            columns: schema.fields().clone(),
            name_prefix_parts: vec![],
        }
    }

    pub fn named_schema(alias: String, schema: DataSchemaRef) -> AnalyzedSchema {
        AnalyzedSchema {
            columns: schema.fields().clone(),
            name_prefix_parts: vec![alias],
        }
    }

    pub fn unnamed_table(db: String, table: String, schema: DataSchemaRef) -> AnalyzedSchema {
        AnalyzedSchema {
            columns: schema.fields().clone(),
            name_prefix_parts: vec![db, table],
        }
    }

    pub fn clone_with_new_fields(&self, new_fields: &[DataField]) -> AnalyzedSchema {
        // TODO: 新的schema, 一些中间计算结果, 新的fields会覆盖老得fields
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.columns.iter().any(|column| column.name() == column_name)
    }
}

impl Debug for AnalyzedSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        todo!()
    }
}
