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

use arrow_array::RecordBatch;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use jiff::tz::TimeZone;

pub fn read_record_batch_to_variant_column(
    record_batch: RecordBatch,
    builder: &mut BinaryColumnBuilder,
    tz: &TimeZone,
    typ: &TableDataType,
    schema: &DataSchema,
) -> databend_common_exception::Result<()> {
    let mut columns = Vec::with_capacity(record_batch.columns().len());
    for (array, field) in record_batch.columns().iter().zip(schema.fields()) {
        columns.push(Column::from_arrow_rs(array.clone(), field.data_type())?)
    }
    let column = Column::Tuple(columns);
    for scalar in column.iter() {
        cast_scalar_to_variant(scalar, tz, &mut builder.data, Some(typ));
        builder.commit_row()
    }
    Ok(())
}

pub fn record_batch_to_variant_block(
    record_batch: RecordBatch,
    tz: &TimeZone,
    typ: &TableDataType,
    schema: &DataSchema,
) -> databend_common_exception::Result<DataBlock> {
    let mut builder = BinaryColumnBuilder::with_capacity(
        record_batch.num_rows(),
        record_batch.get_array_memory_size(),
    );
    read_record_batch_to_variant_column(record_batch, &mut builder, tz, typ, schema)?;
    let column = builder.build();
    let num_rows = column.len();
    Ok(DataBlock::new(
        vec![Column::Variant(column).into()],
        num_rows,
    ))
}
