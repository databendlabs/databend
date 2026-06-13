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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use match_template::match_template;

use super::Rows;
use super::utils::ORDER_COL_NAME;
use super::utils::has_order_field;

#[derive(Debug, Clone)]
struct RowSortField {
    offset: usize,
    data_type: DataType,
    asc: bool,
    nulls_first: bool,
}

impl RowSortField {
    fn new(offset: usize, data_type: DataType, asc: bool, nulls_first: bool) -> Self {
        Self {
            offset,
            data_type,
            asc,
            nulls_first,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimpleRowsSupport {
    Unsupported,
    Legacy,
    FixedOnly,
}

#[derive(Clone)]
pub struct SortKeyDescription {
    column_desc: Arc<[SortColumnDescription]>,
    schema: DataSchemaRef,
    enable_fixed_rows: bool,
    simple_rows_support: SimpleRowsSupport,
}

impl SortKeyDescription {
    pub fn new(
        column_desc: Arc<[SortColumnDescription]>,
        schema: DataSchemaRef,
        enable_fixed_rows: bool,
    ) -> Result<Self> {
        if has_order_field(&schema) {
            return Err(ErrorCode::Internal(
                "SortPipelineBuilder expects schema without order column".to_string(),
            ));
        }
        let num_fields = schema.num_fields();
        assert!(!column_desc.is_empty());
        for col_desc in column_desc.iter() {
            if col_desc.offset >= num_fields {
                return Err(ErrorCode::Internal(format!(
                    "Sort column offset {} is out of bounds for schema with {} fields",
                    col_desc.offset, num_fields
                )));
            }
        }
        let simple_rows_support = Self::compute_simple_rows_support(&column_desc, &schema);
        Ok(Self {
            column_desc,
            schema,
            enable_fixed_rows,
            simple_rows_support,
        })
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn sort_column_desc(&self) -> Arc<[SortColumnDescription]> {
        self.column_desc.clone()
    }

    pub fn uses_source_sort_col(&self) -> bool {
        if self.enable_fixed_rows {
            self.simple_rows_support != SimpleRowsSupport::Unsupported
        } else {
            self.simple_rows_support == SimpleRowsSupport::Legacy
        }
    }

    pub fn sort_row_offset(&self) -> usize {
        if self.uses_source_sort_col() {
            debug_assert_eq!(self.column_desc.len(), 1);
            self.column_desc[0].offset
        } else {
            self.schema.fields().len()
        }
    }

    pub fn order_field_type(&self) -> DataType {
        struct OrderFieldTypeVisitor {
            key_desc: SortKeyDescription,
        }

        impl RowsTypeVisitor for OrderFieldTypeVisitor {
            type Result = DataType;
            fn sort_key_desc(&self) -> SortKeyDescription {
                self.key_desc.clone()
            }

            fn visit_type<R>(&mut self) -> Self::Result
            where
                R: Rows + 'static,
                R::Converter: Send + 'static,
            {
                R::data_type()
            }
        }

        let mut visitor = OrderFieldTypeVisitor {
            key_desc: self.clone(),
        };

        select_row_type(&mut visitor, self.enable_fixed_rows)
    }

    pub fn schema_with_order_col(&self) -> DataSchemaRef {
        if self.uses_source_sort_col() {
            return self.schema.clone();
        }

        DataSchema::new_ref(
            self.schema
                .fields
                .iter()
                .cloned()
                .chain(Some(DataField::new(
                    ORDER_COL_NAME,
                    self.order_field_type(),
                )))
                .collect(),
        )
    }

    pub fn strip_order_col_schema(
        column_desc: Arc<[SortColumnDescription]>,
        schema_with_order_col: DataSchemaRef,
        enable_fixed_rows: bool,
    ) -> Result<DataSchemaRef> {
        if has_order_field(&schema_with_order_col) {
            let order_field = schema_with_order_col.fields().last().unwrap();
            let mut fields = schema_with_order_col.fields().clone();
            fields.pop();
            let key_desc = Self::new(column_desc, DataSchema::new_ref(fields), enable_fixed_rows)?;
            let expected = key_desc.order_field_type();
            if order_field.data_type() != &expected {
                return Err(ErrorCode::Internal(format!(
                    "Sort order column type mismatch, expected {:?}, got {:?}",
                    expected,
                    order_field.data_type()
                )));
            }
            Ok(key_desc.schema)
        } else {
            let key_desc = Self::new(
                column_desc,
                schema_with_order_col.clone(),
                enable_fixed_rows,
            )?;
            if key_desc.uses_source_sort_col() {
                Ok(schema_with_order_col)
            } else {
                Err(ErrorCode::Internal(format!(
                    "Sort schema should contain `{ORDER_COL_NAME}` as the last field"
                )))
            }
        }
    }

    fn into_single_sort_offset(self, asc: bool) -> usize {
        assert!(self.column_desc.len() == 1);
        assert_eq!(self.column_desc[0].asc, asc);
        self.column_desc[0].offset
    }

    fn into_checked_sort_fields(
        self,
        support_data_type: impl Fn(&DataType) -> bool,
    ) -> Result<Box<[RowSortField]>> {
        let Self {
            column_desc,
            schema,
            ..
        } = self;
        let sort_fields = column_desc
            .iter()
            .map(|d| {
                let data_type = schema.field(d.offset).data_type();
                RowSortField::new(d.offset, data_type.clone(), d.asc, d.nulls_first)
            })
            .collect::<Vec<_>>();

        if let Some(field) = sort_fields
            .iter()
            .find(|f| !support_data_type(&f.data_type))
        {
            return Err(ErrorCode::Unimplemented(format!(
                "Row format is not yet support for {field:?} in {sort_fields:?}",
            )));
        }

        Ok(sort_fields.into_boxed_slice())
    }

    fn collect_sort_entries(
        sort_fields: &[RowSortField],
        data_block: &DataBlock,
    ) -> Vec<BlockEntry> {
        sort_fields
            .iter()
            .map(|field| data_block.get_by_offset(field.offset).clone())
            .collect()
    }

    fn compute_simple_rows_support(
        column_desc: &[SortColumnDescription],
        schema: &DataSchema,
    ) -> SimpleRowsSupport {
        let Some(desc) = column_desc.first() else {
            return SimpleRowsSupport::Unsupported;
        };
        if column_desc.len() != 1 {
            return SimpleRowsSupport::Unsupported;
        }

        let sort_type = schema.field(desc.offset).data_type();
        match sort_type {
            DataType::Date | DataType::Timestamp | DataType::String | DataType::Number(_) => {
                SimpleRowsSupport::Legacy
            }
            DataType::Boolean | DataType::Interval => SimpleRowsSupport::FixedOnly,
            _ => SimpleRowsSupport::Unsupported,
        }
    }
}

/// Convert columns to rows.
pub trait RowConverter<T: Rows>
where Self: Sized + Debug
{
    fn new(desc: SortKeyDescription) -> Result<Self>;
    fn convert(&self, data_block: &DataBlock) -> Result<T>;
    fn support_data_type(d: &DataType) -> bool;
}

pub fn convert_rows(
    schema: DataSchemaRef,
    sort_desc: &[SortColumnDescription],
    data: DataBlock,
    enable_fixed_rows: bool,
) -> Result<Column> {
    struct ConvertRowsVisitor {
        key_desc: SortKeyDescription,
        data: DataBlock,
    }

    impl RowsTypeVisitor for ConvertRowsVisitor {
        type Result = Result<Column>;
        fn sort_key_desc(&self) -> SortKeyDescription {
            self.key_desc.clone()
        }

        fn visit_type<R>(&mut self) -> Self::Result
        where
            R: Rows + 'static,
            R::Converter: Send + 'static,
        {
            let converter = R::Converter::new(self.key_desc.clone())?;
            let rows = converter.convert(&self.data)?;
            Ok(rows.to_column())
        }
    }

    let mut visitor = ConvertRowsVisitor {
        key_desc: SortKeyDescription::new(Arc::from(sort_desc), schema, enable_fixed_rows)?,
        data,
    };

    select_row_type(&mut visitor, enable_fixed_rows)
}

fn select_row_type_old<V>(visitor: &mut V) -> V::Result
where V: RowsTypeVisitor {
    let key_desc = visitor.sort_key_desc();
    if key_desc.simple_rows_support != SimpleRowsSupport::Legacy {
        return visitor.visit_type::<VariableRows>();
    }
    match key_desc.column_desc.as_ref() {
        [desc] => {
            let schema = key_desc.schema();
            let sort_type = schema.field(desc.offset).data_type();
            let asc = desc.asc;

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
                _ => visitor.visit_type::<VariableRows>()
                }
            }
        }
        _ => visitor.visit_type::<VariableRows>(),
    }
}

fn select_row_type_enable_fixed_rows<V>(visitor: &mut V) -> V::Result
where V: RowsTypeVisitor {
    let key_desc = visitor.sort_key_desc();
    let sort_desc = key_desc.column_desc.as_ref();
    if key_desc.simple_rows_support != SimpleRowsSupport::Unsupported
        && let [desc] = sort_desc
    {
        let schema = key_desc.schema();
        let sort_type = schema.field(desc.offset).data_type();
        let asc = desc.asc;

        match_template! {
        T = [ Boolean => BooleanType, Date => DateType, Timestamp => TimestampType, String => StringType, Interval => IntervalType ],
        match sort_type {
            DataType::T => {
                return if asc {
                    visitor.visit_type::<SimpleRowsAsc<T>>()
                } else {
                    visitor.visit_type::<SimpleRowsDesc<T>>()
                }
            },
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => {
                    return if asc {
                        visitor.visit_type::<SimpleRowsAsc<NumberType<NUM_TYPE>>>()
                    } else {
                        visitor.visit_type::<SimpleRowsDesc<NumberType<NUM_TYPE>>>()
                    }
                }
            }),
            _ => ()
            }
        }
    }

    let schema = key_desc.schema();

    let sort_fields = sort_desc
        .iter()
        .map(|d| {
            let data_type = schema.field(d.offset).data_type();
            RowSortField::new(d.offset, data_type.clone(), d.asc, d.nulls_first)
        })
        .collect::<Vec<_>>();

    match choose_encode_method(&sort_fields) {
        Some(length) => match length.div_ceil(8) {
            1 => visitor.visit_type::<FixedRows<1>>(),
            2 => visitor.visit_type::<FixedRows<2>>(),
            3 => visitor.visit_type::<FixedRows<3>>(),
            4 => visitor.visit_type::<FixedRows<4>>(),
            5 => visitor.visit_type::<FixedRows<5>>(),
            6 => visitor.visit_type::<FixedRows<6>>(),
            _ => visitor.visit_type::<VariableRows>(),
        },
        None => visitor.visit_type::<VariableRows>(),
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
    fn sort_key_desc(&self) -> SortKeyDescription;

    fn visit_type<R>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        R::Converter: Send + 'static;
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
