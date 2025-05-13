// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_cast::cast;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use databend_common_exception::ErrorCode;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchemaRef;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[derive(Debug)]
enum SchemaComparison {
    Equivalent,
    NameChangesOnly,
    Different,
}

#[derive(Debug, Clone)]
enum ColumnSource {
    PassThrough {
        source_index: usize,
    },
    Promote {
        target_type: arrow_schema::DataType,
        source_index: usize,
    },
}

#[derive(Debug, Clone)]
enum BatchTransform {
    PassThrough,
    Modify {
        target_schema: Arc<Schema>,
        operations: Vec<ColumnSource>,
    },
    ModifySchema {
        target_schema: Arc<Schema>,
    },
}

#[derive(Clone)]
pub struct RecordBatchTransformer {
    match_by_field_name: bool,
    target_schema: SchemaRef,
    table_field_mapping: BTreeMap<ColumnId, usize>,

    transforms: Option<BatchTransform>,
}

impl RecordBatchTransformer {
    pub fn build(table_schema: TableSchemaRef) -> Self {
        let target_schema: Schema = table_schema.as_ref().into();

        let mut table_field_mapping = BTreeMap::new();
        for (i, field) in table_schema.fields().iter().enumerate() {
            table_field_mapping.insert(field.column_id(), i);
        }

        RecordBatchTransformer {
            match_by_field_name: false,
            target_schema: Arc::new(target_schema),
            table_field_mapping,
            transforms: None,
        }
    }

    pub fn match_by_field_name(&mut self, match_by_field_name: bool) {
        self.match_by_field_name = match_by_field_name;
    }

    pub fn process_record_batch(
        &mut self,
        record_batch: RecordBatch,
    ) -> databend_common_exception::Result<RecordBatch> {
        let batch = match &self.transforms {
            Some(BatchTransform::PassThrough) => record_batch,
            Some(BatchTransform::Modify {
                target_schema,
                operations,
            }) => {
                let options = RecordBatchOptions::default()
                    .with_match_field_names(false)
                    .with_row_count(Some(record_batch.num_rows()));
                let columns = self.transform_columns(record_batch.columns(), operations)?;
                RecordBatch::try_new_with_options(target_schema.clone(), columns, &options)?
            }
            Some(BatchTransform::ModifySchema { target_schema }) => {
                record_batch.with_schema(target_schema.clone())?
            }
            None => {
                self.transforms = Some(Self::generate_batch_transform(
                    record_batch.schema_ref(),
                    &self.target_schema,
                    &self.table_field_mapping,
                    self.match_by_field_name,
                )?);

                self.process_record_batch(record_batch)?
            }
        };
        Ok(batch)
    }

    fn compare_schemas(source: &SchemaRef, target: &SchemaRef) -> SchemaComparison {
        if source.fields().len() != target.fields().len() {
            return SchemaComparison::Different;
        }
        let mut names_changed = false;

        for (source_field, target_field) in source.fields().iter().zip(target.fields().iter()) {
            if source_field.data_type() != target_field.data_type()
                || source_field.is_nullable() != target_field.is_nullable()
            {
                return SchemaComparison::Different;
            }

            if source_field.name() != target_field.name() {
                names_changed = true;
            }
        }
        if names_changed {
            SchemaComparison::NameChangesOnly
        } else {
            SchemaComparison::Equivalent
        }
    }

    fn transform_columns(
        &self,
        columns: &[Arc<dyn Array>],
        operations: &[ColumnSource],
    ) -> databend_common_exception::Result<Vec<Arc<dyn Array>>> {
        if columns.is_empty() {
            return Ok(columns.to_vec());
        }
        operations
            .iter()
            .map(|op| {
                Ok(match op {
                    ColumnSource::PassThrough { source_index } => columns[*source_index].clone(),

                    ColumnSource::Promote {
                        target_type,
                        source_index,
                    } => cast(&*columns[*source_index], target_type)?,
                })
            })
            .collect()
    }

    pub fn build_field_id_to_arrow_schema_map(
        source_schema: &SchemaRef,
    ) -> databend_common_exception::Result<HashMap<ColumnId, (FieldRef, usize)>> {
        let mut field_id_to_source_schema = HashMap::new();
        for (source_field_idx, source_field) in source_schema.fields.iter().enumerate() {
            let this_field_id = source_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .ok_or_else(|| ErrorCode::InvalidDate("field id not present in parquet metadata"))?
                .parse()
                .map_err(|e| {
                    ErrorCode::InvalidDate(format!("field id not parseable as an column id: {}", e))
                })?;

            field_id_to_source_schema
                .insert(this_field_id, (source_field.clone(), source_field_idx));
        }
        Ok(field_id_to_source_schema)
    }

    fn generate_batch_transform(
        source: &SchemaRef,
        target: &SchemaRef,
        table_field_mapping: &BTreeMap<ColumnId, usize>,
        match_by_field_name: bool,
    ) -> databend_common_exception::Result<BatchTransform> {
        match Self::compare_schemas(source, target) {
            SchemaComparison::Equivalent => Ok(BatchTransform::PassThrough),
            SchemaComparison::NameChangesOnly => Ok(BatchTransform::ModifySchema {
                target_schema: target.clone(),
            }),
            SchemaComparison::Different => Ok(BatchTransform::Modify {
                operations: Self::generate_transform_operations(
                    source,
                    target,
                    table_field_mapping,
                    match_by_field_name,
                )?,
                target_schema: target.clone(),
            }),
        }
    }

    fn generate_transform_operations(
        source: &SchemaRef,
        target: &SchemaRef,
        table_field_mapping: &BTreeMap<ColumnId, usize>,
        match_by_field_name: bool,
    ) -> databend_common_exception::Result<Vec<ColumnSource>> {
        let mut sources = Vec::with_capacity(table_field_mapping.len());

        if match_by_field_name {
            for target_field in target.fields() {
                let target_type = target_field.data_type();
                let Some((source_index, source_field)) = source.fields().find(target_field.name())
                else {
                    return Err(ErrorCode::TableSchemaMismatch(format!(
                        "The field with field name: {} does not exist in the source schema: {:#?}.",
                        target_field.name(),
                        source
                    )));
                };

                sources.push(
                    if source_field
                        .data_type()
                        .equals_datatype(target_field.data_type())
                    {
                        ColumnSource::PassThrough { source_index }
                    } else {
                        ColumnSource::Promote {
                            target_type: target_type.clone(),
                            source_index,
                        }
                    },
                )
            }
        } else {
            let source_map = Self::build_field_id_to_arrow_schema_map(source)?;

            for (field_id, target_index) in table_field_mapping.iter() {
                let target_field = target.field(*target_index);
                let target_type = target_field.data_type();

                let Some((source_field, source_index)) = source_map.get(field_id) else {
                    return Err(ErrorCode::TableSchemaMismatch(format!("The field with field_id: {field_id} does not exist in the source schema: {:#?}.", source)));
                };

                sources.push(
                    if source_field
                        .data_type()
                        .equals_datatype(target_field.data_type())
                    {
                        ColumnSource::PassThrough {
                            source_index: *source_index,
                        }
                    } else {
                        ColumnSource::Promote {
                            target_type: target_type.clone(),
                            source_index: *source_index,
                        }
                    },
                )
            }
        }
        Ok(sources)
    }
}
