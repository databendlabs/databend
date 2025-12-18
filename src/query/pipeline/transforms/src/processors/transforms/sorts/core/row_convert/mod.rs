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

use std::fmt::Debug;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortField;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use match_template::match_template;

use super::Rows;

/// Convert columns to rows.
pub trait RowConverter<T: Rows>
where Self: Sized + Debug
{
    fn create(
        sort_columns_descriptions: &[SortColumnDescription],
        output_schema: DataSchemaRef,
    ) -> Result<Self>;
    fn convert(&self, columns: &[BlockEntry], num_rows: usize) -> Result<T>;

    fn convert_data_block(
        &self,
        sort_desc: &[SortColumnDescription],
        data_block: &DataBlock,
    ) -> Result<T> {
        let order_by_cols = sort_desc
            .iter()
            .map(|desc| data_block.get_by_offset(desc.offset).clone())
            .collect::<Vec<_>>();
        self.convert(&order_by_cols, data_block.num_rows())
    }

    fn support_data_type(d: &DataType) -> bool;
}

pub fn convert_rows(
    schema: DataSchemaRef,
    sort_desc: &[SortColumnDescription],
    data: DataBlock,
    enable_fixed_rows: bool,
) -> Result<Column> {
    struct ConvertRowsVisitor<'a> {
        schema: DataSchemaRef,
        sort_desc: &'a [SortColumnDescription],
        data: DataBlock,
    }

    impl RowsTypeVisitor for ConvertRowsVisitor<'_> {
        type Result = Result<Column>;
        fn schema(&self) -> DataSchemaRef {
            self.schema.clone()
        }

        fn sort_desc(&self) -> &[SortColumnDescription] {
            self.sort_desc
        }

        fn visit_type<R, C>(&mut self) -> Self::Result
        where
            R: Rows + 'static,
            C: RowConverter<R> + Send + 'static,
        {
            let columns = self
                .sort_desc
                .iter()
                .map(|desc| self.data.get_by_offset(desc.offset).to_owned())
                .collect::<Vec<_>>();

            let converter = C::create(self.sort_desc, self.schema.clone())?;
            let rows = C::convert(&converter, &columns, self.data.num_rows())?;
            Ok(rows.to_column())
        }
    }

    let mut visitor = ConvertRowsVisitor {
        schema: schema.clone(),
        sort_desc,
        data,
    };

    select_row_type(&mut visitor, enable_fixed_rows)
}

fn select_row_type_old<V>(visitor: &mut V) -> V::Result
where V: RowsTypeVisitor {
    match &visitor.sort_desc() {
        &[desc] => {
            let schema = visitor.schema();
            let sort_type = schema.field(desc.offset).data_type();
            let asc = desc.asc;

            match_template! {
            T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
            match sort_type {
                DataType::T => {
                    if asc {
                        visitor.visit_type::<SimpleRowsAsc<T>, SimpleRowConverter<T>>()
                    } else {
                        visitor.visit_type::<SimpleRowsDesc<T>, SimpleRowConverter<T>>()
                    }
                },
                DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => {
                        if asc {
                            visitor.visit_type::<SimpleRowsAsc<NumberType<NUM_TYPE>>, SimpleRowConverter<NumberType<NUM_TYPE>>>()
                        } else {
                            visitor.visit_type::<SimpleRowsDesc<NumberType<NUM_TYPE>>, SimpleRowConverter<NumberType<NUM_TYPE>>>()
                        }
                    }
                }),
                _ => visitor.visit_type::<VariableRows, VariableRowConverter>()
                }
            }
        }
        _ => visitor.visit_type::<VariableRows, VariableRowConverter>(),
    }
}

fn select_row_type_enable_fixed_rows<V>(visitor: &mut V) -> V::Result
where V: RowsTypeVisitor {
    let sort_desc = visitor.sort_desc();
    if let &[desc] = &sort_desc {
        let schema = visitor.schema();
        let sort_type = schema.field(desc.offset).data_type();
        let asc = desc.asc;

        match_template! {
        T = [ Boolean => BooleanType, Date => DateType, Timestamp => TimestampType, String => StringType, Interval => IntervalType ],
        match sort_type {
            DataType::T => {
                return if asc {
                    visitor.visit_type::<SimpleRowsAsc<T>, SimpleRowConverter<T>>()
                } else {
                    visitor.visit_type::<SimpleRowsDesc<T>, SimpleRowConverter<T>>()
                }
            },
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => {
                    return if asc {
                        visitor.visit_type::<SimpleRowsAsc<NumberType<NUM_TYPE>>, SimpleRowConverter<NumberType<NUM_TYPE>>>()
                    } else {
                        visitor.visit_type::<SimpleRowsDesc<NumberType<NUM_TYPE>>, SimpleRowConverter<NumberType<NUM_TYPE>>>()
                    }
                }
            }),
            _ => ()
            }
        }
    }

    let schema = visitor.schema();

    let sort_fields = sort_desc
        .iter()
        .map(|d| {
            let data_type = schema.field(d.offset).data_type();
            SortField::new_with_options(data_type.clone(), d.asc, d.nulls_first)
        })
        .collect::<Vec<_>>();

    match choose_encode_method(&sort_fields) {
        Some(length) => match length.div_ceil(8) {
            1 => visitor.visit_type::<FixedRows<1>, FixedRowConverter<1>>(),
            2 => visitor.visit_type::<FixedRows<2>, FixedRowConverter<2>>(),
            3 => visitor.visit_type::<FixedRows<3>, FixedRowConverter<3>>(),
            4 => visitor.visit_type::<FixedRows<4>, FixedRowConverter<4>>(),
            5 => visitor.visit_type::<FixedRows<5>, FixedRowConverter<5>>(),
            6 => visitor.visit_type::<FixedRows<6>, FixedRowConverter<6>>(),
            _ => visitor.visit_type::<VariableRows, VariableRowConverter>(),
        },
        None => visitor.visit_type::<VariableRows, VariableRowConverter>(),
    }
}

pub fn select_row_type<V>(visitor: &mut V, enable_fixed_rows: bool) -> V::Result
where V: RowsTypeVisitor {
    if enable_fixed_rows {
        select_row_type_enable_fixed_rows(visitor)
    } else {
        select_row_type_old(visitor)
    }
}

pub trait RowsTypeVisitor {
    type Result;
    fn schema(&self) -> DataSchemaRef;

    fn sort_desc(&self) -> &[SortColumnDescription];

    fn visit_type<R, C>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static;
}

pub fn order_field_type(
    schema: &DataSchema,
    desc: &[SortColumnDescription],
    enable_fixed_rows: bool,
) -> DataType {
    struct OrderFieldTypeVisitor<'a> {
        schema: DataSchemaRef,
        sort_desc: &'a [SortColumnDescription],
    }

    impl RowsTypeVisitor for OrderFieldTypeVisitor<'_> {
        type Result = DataType;
        fn schema(&self) -> DataSchemaRef {
            self.schema.clone()
        }

        fn sort_desc(&self) -> &[SortColumnDescription] {
            self.sort_desc
        }

        fn visit_type<R, C>(&mut self) -> Self::Result
        where
            R: Rows + 'static,
            C: RowConverter<R> + Send + 'static,
        {
            R::data_type()
        }
    }

    assert!(!desc.is_empty());
    let mut visitor = OrderFieldTypeVisitor {
        schema: schema.clone().into(),
        sort_desc: desc,
    };

    select_row_type(&mut visitor, enable_fixed_rows)
}

fn null_sentinel(nulls_first: bool) -> u8 {
    if nulls_first { 0 } else { 0xFF }
}

mod fixed;
mod fixed_encode;
mod simple;
#[cfg(test)]
mod test_util;
mod variable;
mod variable_encode;

pub use self::fixed::*;
pub use self::simple::*;
pub use self::variable::*;
