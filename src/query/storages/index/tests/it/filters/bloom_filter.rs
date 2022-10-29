// Copyright 2022 Datafuse Labs.
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
//

use std::collections::HashSet;

use common_datablocks::DataBlock;
use common_datavalues::BooleanType;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::StringType;
use common_datavalues::ToDataType;
use common_exception::Result;
use common_storages_index::BlockFilter;
use common_storages_index::FilterEvalResult;

#[test]
fn test_column_type_support() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("u", u8::to_data_type()),
        DataField::new("u16", u16::to_data_type()),
        DataField::new("u32", u32::to_data_type()),
        DataField::new("u64", u64::to_data_type()),
        DataField::new("i", i8::to_data_type()),
        DataField::new("i16", i16::to_data_type()),
        DataField::new("i32", i32::to_data_type()),
        DataField::new("i64", i64::to_data_type()),
        DataField::new("b", BooleanType::new_impl()),
        DataField::new("s", StringType::new_impl()),
    ]);

    let cols = schema
        .fields()
        .iter()
        .map(|f| {
            f.data_type()
                .create_constant_column(&f.data_type().default_value(), 1)
        })
        .collect::<Result<Vec<_>>>()?;

    use common_datavalues::DataType;
    let block = DataBlock::create(schema.clone(), cols);

    let supported_types: HashSet<DataTypeImpl> = HashSet::from_iter(vec![
        StringType::new_impl(),
        u8::to_data_type(),
        i8::to_data_type(),
        u16::to_data_type(),
        i16::to_data_type(),
        u32::to_data_type(),
        i32::to_data_type(),
        u64::to_data_type(),
        i64::to_data_type(),
    ]);
    let index = BlockFilter::try_create(&[&block])?;

    // String type and 8 integral types are supported
    assert_eq!(supported_types.len(), index.filter_block.columns().len());

    // check index columns
    schema.fields().iter().for_each(|field| {
        let col_name = BlockFilter::build_filter_column_name(field.name());
        let maybe_index_col = index.filter_block.try_column_by_name(&col_name);
        if supported_types.contains(field.data_type()) {
            assert!(maybe_index_col.is_ok(), "check field {}", field.name())
        } else {
            assert!(maybe_index_col.is_err(), "check field {}", field.name())
        }
    });

    // check applicable
    schema.fields().iter().for_each(|field| {
        // type of input data value does not matter here, will be casted during filtering
        let value = DataValue::Boolean(true);
        let col_name = field.name().as_str();
        let data_type = field.data_type();
        let r = index.find(col_name, value, data_type).unwrap();
        if supported_types.contains(field.data_type()) {
            assert_ne!(
                r,
                FilterEvalResult::NotApplicable,
                "check applicable field {}",
                field.name()
            )
        } else {
            assert_eq!(
                r,
                FilterEvalResult::NotApplicable,
                "check applicable field {}",
                field.name()
            )
        }
    });
    Ok(())
}
