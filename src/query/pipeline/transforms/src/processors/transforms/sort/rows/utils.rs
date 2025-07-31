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
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
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

    select_row_type(&mut visitor)
}

pub fn select_row_type<V>(visitor: &mut V) -> V::Result
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
                _ => visitor.visit_type::<CommonRows, CommonConverter>()
                }
            }
        }
        _ => visitor.visit_type::<CommonRows, CommonConverter>(),
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

pub fn order_field_type(schema: &DataSchema, desc: &[SortColumnDescription]) -> DataType {
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

    select_row_type(&mut visitor)
}
