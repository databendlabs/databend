// Copyright 2023 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::ValueType;
use common_expression::Column;
use opendal::Operator;

#[async_trait::async_trait]
pub trait DeleteMarkReader {
    async fn read_delete_mark(&self, dal: Operator, size: u64) -> Result<Arc<Bitmap>>;
}

pub(super) fn deserialize_bitmap(
    bytes: &[u8],
    num_rows: usize,
    column_descriptor: &ColumnDescriptor,
) -> Result<Bitmap> {
    let descriptor = column_descriptor.descriptor.clone();
    let page_meta_data = PageMetaData {
        column_start: 0,
        num_values: num_rows as i64,
        compression: Compression::Uncompressed,
        descriptor,
    };

    let page_reader = PageReader::new_with_page_meta(
        std::io::Cursor::new(bytes), // we can not use &[u8] as Reader here, lifetime not valid
        page_meta_data,
        Arc::new(|_, _| true),
        vec![],
        usize::MAX,
    );

    let decompressor = BasicDecompressor::new(page_reader, vec![]);
    let column_type = column_descriptor.descriptor.primitive_type.clone();
    let filed_name = column_descriptor.path_in_schema[0].to_owned();
    let field = ArrowField::new(filed_name, DataType::Boolean, false);
    let mut array_iter = column_iter_to_arrays(
        vec![decompressor],
        vec![&column_type],
        field,
        None,
        num_rows,
    )?;
    if let Some(array) = array_iter.next() {
        let array = array?;
        let col = Column::from_arrow(array.as_ref(), &common_expression::types::DataType::Boolean);

        let bitmap = BooleanType::try_downcast_column(&col).ok_or_else(|| {
            ErrorCode::Internal("unexpected exception: load delete mark raw data as boolean failed")
        })?;

        Ok(bitmap)
    } else {
        Err(ErrorCode::StorageOther(
            "delete mark data not available as expected",
        ))
    }
}
