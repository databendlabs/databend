use common_datavalues::{DataSchemaRef, DataField, DataType, DataSchema};
use std::fmt::{Debug, Formatter};
use sqlparser::ast::{ObjectName, TableAlias};
use std::sync::Arc;
use crate::sessions::{DatabendQueryContextRef, DatabendQueryContext};
use common_exception::{Result, ErrorCode};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use common_planners::Expression;

#[derive(Clone)]
pub struct AnalyzeQuerySchema {
    // Can be referenced by column name without ambiguity.
    short_name_columns: HashMap<String, AnalyzeQueryColumnDesc>,
    // Reference by full name, short name may be ambiguous.
    tables_long_name_columns: Vec<AnalyzeQueryTableDesc>,
}

impl AnalyzeQuerySchema {
    pub async fn from_dummy_table(ctx: DatabendQueryContextRef) -> Result<AnalyzeQuerySchema> {
        // We cannot reference field by name, such as `SELECT system.one.dummy`
        let dummy_table = ctx.get_table("system", "one")?;
        let dummy_table_dsc = AnalyzeQueryTableDesc::from_schema(dummy_table.schema(), vec![]);
        let mut short_name_columns = HashMap::new();

        for column_desc in &dummy_table_dsc.columns_desc {
            match short_name_columns.entry(column_desc.short_name.clone()) {
                Entry::Vacant(v) => { v.insert(column_desc.clone()); },
                Entry::Occupied(v) => {
                    return Err(ErrorCode::LogicalError(
                        "Logical error: same columns in `system`.`one` table, this is a bug."
                    ));
                }
            };
        }

        Ok(AnalyzeQuerySchema {
            short_name_columns,
            tables_long_name_columns: vec![dummy_table_dsc],
        })
    }

    pub async fn from_table(ctx: DatabendQueryContextRef, name: &ObjectName, alias: &Option<TableAlias>) -> Result<AnalyzeQuerySchema> {
        let (database, table) = Self::resolve_table(name, &ctx)?;

        let resolved_table = ctx.get_table(&database, &table)?;
        // match alias {
        //     None => {
        //         let schema = read_table.schema();
        //         // let analyzed_schema = AnalyzedSchema::unnamed_table(database, table, schema);
        //         let table_schema = TableSchema::Default { database, table: resolved_table, schema };
        //         self.tables_schema.push(table_schema);
        //     }
        //     Some(table_alias) => {
        //         let alias = table_alias.name.value.clone();
        //         // let analyzed_schema = AnalyzedSchema::named_table(alias, schema);
        //         self.tables_schema.push(TableSchema::Named(alias, read_table.schema()));
        //     }
        // }
        unimplemented!("")
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.short_name_columns.contains_key(column_name)
    }

    pub fn get_column_by_fullname(&self, fullname: &[String]) -> Option<&AnalyzeQueryColumnDesc> {
        for table_desc in &self.tables_long_name_columns {
            if table_desc.name_parts.len() > fullname.len()
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

impl AnalyzeQuerySchema {
    fn resolve_table(name: &ObjectName, ctx: &DatabendQueryContext) -> Result<(String, String)> {
        match name.0.len() {
            0 => Err(ErrorCode::SyntaxException("Table name is empty")),
            1 => Ok((ctx.get_current_database(), name.0[0].value.clone())),
            2 => Ok((name.0[0].value.clone(), name.0[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException("Table name must be [`db`].`table`"))
        }
    }
}

impl Debug for AnalyzeQuerySchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
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

