use common_datavalues::{DataSchemaRef, DataField, DataType, DataSchema};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_exception::{Result, ErrorCode};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use common_planners::{col, Expression};
use crate::catalogs::Table;
use crate::sql::statements::QueryAnalyzeState;

#[derive(Clone)]
pub struct JoinedSchema {
    // Can be referenced by column name without ambiguity.
    short_name_columns: HashMap<String, JoinedColumnDesc>,
    // Reference by full name, short name may be ambiguous.
    tables_long_name_columns: Vec<JoinedTableDesc>,
}

impl JoinedSchema {
    pub fn none() -> JoinedSchema {
        JoinedSchema {
            short_name_columns: HashMap::new(),
            tables_long_name_columns: Vec::new(),
        }
    }

    pub fn from_table(table: Arc<dyn Table>, prefix: Vec<String>) -> Result<JoinedSchema> {
        let table_desc = JoinedTableDesc::from_table(table, prefix);
        Self::from_table_desc(table_desc)
    }

    pub fn from_subquery(state: QueryAnalyzeState, prefix: Vec<String>) -> Result<JoinedSchema> {
        let table_desc = JoinedTableDesc::from_subquery(state, prefix);
        Self::from_table_desc(table_desc)
    }

    fn from_table_desc(table_desc: JoinedTableDesc) -> Result<JoinedSchema> {
        let mut short_name_columns = HashMap::new();

        for column_desc in table_desc.get_columns_desc() {
            match short_name_columns.entry(column_desc.short_name.clone()) {
                Entry::Vacant(v) => { v.insert(column_desc.clone()); }
                Entry::Occupied(_) => {
                    return Err(ErrorCode::LogicalError(
                        format!("Logical error: same columns in {:?}, this is a bug.", table_desc.get_name_parts())
                    ));
                }
            };
        }

        Ok(JoinedSchema {
            short_name_columns,
            tables_long_name_columns: vec![table_desc],
        })
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.short_name_columns.contains_key(column_name)
    }

    pub fn get_tables_desc(&self) -> &[JoinedTableDesc] {
        &self.tables_long_name_columns
    }

    pub fn to_data_schema(&self) -> DataSchemaRef {
        let mut fields = Vec::with_capacity(self.short_name_columns.len());

        for table_desc in &self.tables_long_name_columns {
            for column_desc in table_desc.get_columns_desc() {
                fields.push(DataField::new(
                    &column_desc.column_name(),
                    column_desc.data_type.clone(),
                    column_desc.nullable,
                ));
            }
        }

        Arc::new(DataSchema::new(fields))
    }

    pub fn join(&self, _joined_schema: &JoinedSchema) -> Result<Arc<JoinedSchema>> {
        unimplemented!("")
    }
}

impl Debug for JoinedSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ambiguity_names = Vec::new();
        let mut short_names = Vec::with_capacity(self.short_name_columns.len());
        for table_desc in &self.tables_long_name_columns {
            for column_desc in table_desc.get_columns_desc() {
                match column_desc.is_ambiguity {
                    true => {
                        let mut name_parts = table_desc.get_name_parts().to_vec();
                        name_parts.push(column_desc.short_name.clone());
                        ambiguity_names.push(name_parts);
                    }
                    false => {
                        short_names.push(
                            column_desc.short_name.clone()
                        );
                    }
                }
            }
        }

        let mut debug_struct = f.debug_struct("QuerySchema");
        if !short_names.is_empty() {
            debug_struct.field("short_names", &short_names);
        }

        if !ambiguity_names.is_empty() {
            debug_struct.field("ambiguity_names", &ambiguity_names);
        }

        debug_struct.finish()
    }
}

#[derive(Clone)]
pub enum JoinedTableDesc {
    Table {
        table: Arc<dyn Table>,
        name_parts: Vec<String>,
        columns_desc: Vec<JoinedColumnDesc>,
    },
    Subquery {
        state: Arc<QueryAnalyzeState>,
        name_parts: Vec<String>,
        columns_desc: Vec<JoinedColumnDesc>,
    },
}

impl JoinedTableDesc {
    pub fn from_table(table: Arc<dyn Table>, prefix: Vec<String>) -> JoinedTableDesc {
        let schema = table.schema();
        let mut columns_desc = Vec::with_capacity(schema.fields().len());

        for data_field in schema.fields() {
            columns_desc.push(JoinedColumnDesc::from_field(data_field, false));
        }

        JoinedTableDesc::Table {
            table,
            columns_desc,
            name_parts: prefix,
        }
    }

    pub fn from_subquery(state: QueryAnalyzeState, prefix: Vec<String>) -> JoinedTableDesc {
        let schema = state.finalize_schema.clone();
        let mut columns_desc = Vec::with_capacity(schema.fields().len());

        for data_field in schema.fields() {
            columns_desc.push(JoinedColumnDesc::from_field(data_field, false));
        }

        JoinedTableDesc::Subquery {
            state: Arc::new(state),
            columns_desc,
            name_parts: prefix,
        }
    }

    pub fn get_name_parts(&self) -> &[String] {
        match self {
            JoinedTableDesc::Table { name_parts, .. } => name_parts,
            JoinedTableDesc::Subquery { name_parts, .. } => name_parts,
        }
    }

    pub fn get_columns_desc(&self) -> &[JoinedColumnDesc] {
        match self {
            JoinedTableDesc::Table { columns_desc, .. } => columns_desc,
            JoinedTableDesc::Subquery { columns_desc, .. } => columns_desc,
        }
    }
}

#[derive(Clone)]
pub struct JoinedColumnDesc {
    pub short_name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_ambiguity: bool,
}

impl JoinedColumnDesc {
    pub fn from_field(field: &DataField, is_ambiguity: bool) -> JoinedColumnDesc {
        JoinedColumnDesc {
            short_name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            is_ambiguity,
        }
    }

    pub fn create(alias: &str, data_type: DataType, nullable: bool) -> JoinedColumnDesc {
        JoinedColumnDesc {
            short_name: alias.to_string(),
            data_type,
            nullable,
            is_ambiguity: false,
        }
    }

    pub fn column_name(&self) -> String {
        match self.is_ambiguity {
            true => unimplemented!(),
            false => self.short_name.clone()
        }
    }
}

