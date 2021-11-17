use common_datavalues::{DataSchemaRef, DataField, DataType, DataSchema};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_exception::{Result, ErrorCode};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

#[derive(Clone)]
pub struct AnalyzeQuerySchema {
    // Can be referenced by column name without ambiguity.
    short_name_columns: HashMap<String, AnalyzeQueryColumnDesc>,
    // Reference by full name, short name may be ambiguous.
    tables_long_name_columns: Vec<AnalyzeQueryTableDesc>,
}

impl AnalyzeQuerySchema {
    pub fn none() -> AnalyzeQuerySchema {
        AnalyzeQuerySchema {
            short_name_columns: HashMap::new(),
            tables_long_name_columns: Vec::new(),
        }
    }

    pub fn from_schema(schema: DataSchemaRef, prefix: Vec<String>) -> Result<AnalyzeQuerySchema> {
        let table_desc = AnalyzeQueryTableDesc::from_schema(schema, prefix);
        let mut short_name_columns = HashMap::new();

        for column_desc in &table_desc.columns_desc {
            match short_name_columns.entry(column_desc.short_name.clone()) {
                Entry::Vacant(v) => { v.insert(column_desc.clone()); }
                Entry::Occupied(_) => {
                    return Err(ErrorCode::LogicalError(
                        format!("Logical error: same columns in {:?}, this is a bug.", table_desc.name_parts)
                    ));
                }
            };
        }

        Ok(AnalyzeQuerySchema {
            short_name_columns,
            tables_long_name_columns: vec![table_desc],
        })
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.short_name_columns.contains_key(column_name)
    }

    pub fn get_column_by_fullname(&self, fullname: &[String]) -> Option<&AnalyzeQueryColumnDesc> {
        for table_desc in &self.tables_long_name_columns {
            if table_desc.name_parts.len() < fullname.len()
                && table_desc.name_parts[..] == fullname[0..fullname.len() - 1] {
                for column_desc in &table_desc.columns_desc {
                    if column_desc.short_name == fullname[fullname.len() - 1] {
                        return Some(column_desc);
                    }
                }
            }
        }

        // TODO: We need to check whether they are references to payload types when we support complex types.
        // Such as: CREATE TABLE test(a STRUCT(b tinyint)); SELECT a.b FROM test;
        None
    }

    pub fn add_projection(&mut self, desc: AnalyzeQueryColumnDesc, replace: bool) -> Result<()> {
        let short_name = desc.short_name.clone();

        if replace || !self.short_name_columns.contains_key(&short_name) {
            self.short_name_columns.insert(short_name.clone(), desc);
        }

        for table_desc in &mut self.tables_long_name_columns {
            for column_desc in &mut table_desc.columns_desc {
                if !column_desc.is_ambiguity && *column_desc.short_name == short_name {
                    column_desc.is_ambiguity = true;
                }
            }
        }

        Ok(())
    }

    pub fn to_data_schema(&self) -> DataSchemaRef {
        let mut fields = Vec::with_capacity(self.short_name_columns.len());

        for table_desc in &self.tables_long_name_columns {
            for column_desc in &table_desc.columns_desc {
                fields.push(DataField::new(
                    &column_desc.column_name(),
                    column_desc.data_type.clone(),
                    column_desc.nullable,
                ));
            }
        }

        Arc::new(DataSchema::new(fields))
    }

    pub fn join(&self, _joined_schema: &AnalyzeQuerySchema) -> Result<Arc<AnalyzeQuerySchema>> {
        unimplemented!("")
    }
}

impl Debug for AnalyzeQuerySchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ambiguity_names = Vec::new();
        let mut short_names = Vec::with_capacity(self.short_name_columns.len());
        for table_desc in &self.tables_long_name_columns {
            for column_desc in &table_desc.columns_desc {
                match column_desc.is_ambiguity {
                    true => {
                        let mut name_parts = table_desc.name_parts.clone();
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
struct AnalyzeQueryTableDesc {
    name_parts: Vec<String>,
    columns_desc: Vec<AnalyzeQueryColumnDesc>,
}

impl AnalyzeQueryTableDesc {
    pub fn from_schema(schema: DataSchemaRef, prefix: Vec<String>) -> AnalyzeQueryTableDesc {
        let mut columns_desc = Vec::with_capacity(schema.fields().len());

        for data_field in schema.fields() {
            columns_desc.push(AnalyzeQueryColumnDesc::from_field(data_field, false));
        }

        AnalyzeQueryTableDesc {
            name_parts: prefix,
            columns_desc,
        }
    }
}

#[derive(Clone)]
pub struct AnalyzeQueryColumnDesc {
    short_name: String,
    data_type: DataType,
    nullable: bool,
    is_ambiguity: bool,
}

impl AnalyzeQueryColumnDesc {
    pub fn from_field(field: &DataField, is_ambiguity: bool) -> AnalyzeQueryColumnDesc {
        AnalyzeQueryColumnDesc {
            short_name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            is_ambiguity,
        }
    }

    pub fn create(alias: &str, data_type: DataType, nullable: bool) -> AnalyzeQueryColumnDesc {
        AnalyzeQueryColumnDesc {
            short_name: alias.to_string(),
            data_type,
            nullable,
            is_ambiguity: false,
        }
    }

    pub fn column_name(&self) -> String {
        // TODO: Full name may still be ambiguous in the join? for example:
        // SELECT * FROM
        // (SELECT column_a FROM table_a),
        // (SELECT column_a FROM table_b),
        // (SELECT 'failure' AS `table_b.column_a` FROM table_c)
        match self.is_ambiguity {
            true => unimplemented!(),
            false => self.short_name.clone()
        }
    }
}

