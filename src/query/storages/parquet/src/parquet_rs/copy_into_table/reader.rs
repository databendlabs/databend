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

use std::sync::Arc;

use databend_common_arrow::arrow::compute::cast::can_cast_types;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_storage::parquet_rs::infer_schema_with_extension;
use opendal::Operator;
use parquet::file::metadata::FileMetaData;

use crate::parquet_rs::parquet_reader::policy::ReadPolicyBuilder;
use crate::parquet_rs::parquet_reader::policy::ReadPolicyImpl;
use crate::parquet_rs::schema::arrow_to_table_schema;
use crate::InMemoryRowGroup;
use crate::ParquetRSReaderBuilder;
use crate::ParquetRSRowGroupPart;
use crate::ReadSettings;

pub struct RowGroupReaderForCopy {
    row_group_reader_builder: Box<dyn ReadPolicyBuilder>,
    output_projection: Vec<Expr>,
}

impl RowGroupReaderForCopy {
    pub async fn build_reader(
        &self,
        part: &ParquetRSRowGroupPart,
        op: Operator,
        read_settings: &ReadSettings,
        batch_size: usize,
    ) -> Result<Option<ReadPolicyImpl>> {
        let row_group = InMemoryRowGroup::new(
            &part.location,
            op.clone(),
            &part.meta,
            None,
            read_settings.max_gap_size,
            read_settings.max_range_size,
        );
        let mut _sorter = None;
        self.row_group_reader_builder
            .build(row_group, None, &mut _sorter, batch_size)
            .await
    }

    pub fn output_projection(&self) -> &[Expr] {
        &self.output_projection
    }

    pub fn try_create(
        location: &str,
        ctx: Arc<dyn TableContext>,
        op: Operator,
        file_metadata: &FileMetaData,
        output_schema: TableSchemaRef,
        default_values: Vec<Scalar>,
    ) -> Result<RowGroupReaderForCopy> {
        let arrow_schema = infer_schema_with_extension(file_metadata)?;
        let schema_descr = file_metadata.schema_descr_ptr();
        let parquet_table_schema = arrow_to_table_schema(&arrow_schema)?;
        let mut output_projection = vec![];

        let mut num_inputs = 0;
        for (i, to_field) in output_schema.fields().iter().enumerate() {
            let field_name = to_field.name();
            let expr = match parquet_table_schema
                .fields()
                .iter()
                .position(|f| f.name() == field_name)
            {
                Some(pos) => {
                    let from_field = parquet_table_schema.field(pos);
                    let expr = Expr::ColumnRef {
                        span: None,
                        id: num_inputs,
                        data_type: from_field.data_type().into(),
                        display_name: from_field.name().clone(),
                    };

                    // find a better way to do check cast
                    if from_field.data_type == to_field.data_type {
                        expr
                    } else if can_cast_types(
                        ArrowField::from(from_field).data_type(),
                        ArrowField::from(from_field).data_type(),
                    ) {
                        Expr::Cast {
                            span: None,
                            is_try: false,
                            expr: Box::new(expr),
                            dest_type: to_field.data_type().into(),
                        }
                    } else {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Cannot cast column {} from {:?} to {:?}",
                            field_name,
                            from_field.data_type(),
                            to_field.data_type()
                        )));
                    }
                }
                None => Expr::Constant {
                    span: None,
                    scalar: default_values[i].clone(),
                    data_type: to_field.data_type().into(),
                },
            };
            if !matches!(expr, Expr::Constant { .. }) {
                num_inputs += 1;
            }
            output_projection.push(expr);
        }
        if num_inputs == 0 {
            return Err(ErrorCode::BadBytes(format!(
                "not column name match in parquet file {location}",
            )));
        }

        let mut reader_builder = ParquetRSReaderBuilder::create_with_parquet_schema(
            ctx,
            op,
            Arc::new(parquet_table_schema),
            schema_descr,
        );
        reader_builder.build_output()?;

        let row_group_reader_builder = reader_builder.create_no_prefetch_policy_builder()?;
        let reader = RowGroupReaderForCopy {
            row_group_reader_builder,
            output_projection,
        };
        Ok(reader)
    }
}
