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

use databend_common_exception::Result;
use databend_common_expression::row::RowConverter as CommonConverter;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use match_template::match_template;

use super::RowConverter;
use super::Rows;
use super::SimpleRowConverter;
use super::SimpleRowsAsc;
use super::SimpleRowsDesc;
use crate::sort::CommonRows;

pub fn convert_rows(
    schema: DataSchemaRef,
    sort_desc: &[SortColumnDescription],
    data: DataBlock,
) -> Result<Column> {
    let num_rows = data.num_rows();

    if sort_desc.len() == 1 {
        let sort_type = schema.field(sort_desc[0].offset).data_type();
        let asc = sort_desc[0].asc;

        let offset = sort_desc[0].offset;
        let columns = &data.columns()[offset..offset + 1];

        match_template! {
        T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
        match sort_type {
            DataType::T => {
                if asc {
                    convert_columns::<SimpleRowsAsc<T>,SimpleRowConverter<_>>(schema, sort_desc, columns, num_rows)
                } else {
                    convert_columns::<SimpleRowsDesc<T>,SimpleRowConverter<_>>(schema, sort_desc, columns, num_rows)
                }
            },
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => {
                    if asc {
                        convert_columns::<SimpleRowsAsc<NumberType<NUM_TYPE>>,SimpleRowConverter<_>>(schema, sort_desc, columns, num_rows)
                    } else {
                        convert_columns::<SimpleRowsDesc<NumberType<NUM_TYPE>>,SimpleRowConverter<_>>(schema, sort_desc, columns, num_rows)
                    }
                }
            }),
            _ => convert_columns::<CommonRows, CommonConverter>(schema, sort_desc, columns, num_rows),
            }
        }
    } else {
        let columns = sort_desc
            .iter()
            .map(|desc| data.get_by_offset(desc.offset).to_owned())
            .collect::<Vec<_>>();
        convert_columns::<CommonRows, CommonConverter>(schema, sort_desc, &columns, num_rows)
    }
}

fn convert_columns<R: Rows, C: RowConverter<R>>(
    schema: DataSchemaRef,
    sort_desc: &[SortColumnDescription],
    columns: &[BlockEntry],
    num_rows: usize,
) -> Result<Column> {
    let mut converter = C::create(sort_desc, schema)?;
    let rows = C::convert(&mut converter, columns, num_rows)?;
    Ok(rows.to_column())
}

pub fn select_row_type(visitor: &mut impl RowsTypeVisitor) {
    let sort_desc = visitor.sort_desc();
    if sort_desc.len() == 1 {
        let schema = visitor.schema();
        let sort_type = schema.field(sort_desc[0].offset).data_type();
        let asc = sort_desc[0].asc;

        match_template! {
        T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
        match sort_type {
            DataType::T => {
                if asc {
                    visitor.visit_type::<SimpleRowsAsc<T>>()
                } else {
                    visitor.visit_type::<SimpleRowsDesc<T>>()
                }
            },
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => {
                    if asc {
                        visitor.visit_type::<SimpleRowsAsc<NumberType<NUM_TYPE>>>()
                    } else {
                        visitor.visit_type::<SimpleRowsDesc<NumberType<NUM_TYPE>>>()
                    }
                }
            }),
            _ => visitor.visit_type::<CommonRows>()
            }
        }
    } else {
        visitor.visit_type::<CommonRows>()
    }
}

pub trait RowsTypeVisitor {
    fn schema(&self) -> DataSchemaRef;

    fn sort_desc(&self) -> &[SortColumnDescription];

    fn visit_type<R: Rows + 'static>(&mut self);
}
