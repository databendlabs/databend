//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::native::write::NativeWriter;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use opendal::Operator;
use storages_common_blocks::blocks_to_parquet;
use storages_common_table_meta::meta::ColumnMeta;

use crate::fuse_table::FuseStorageFormat;
use crate::io::write::WriteSettings;
use crate::operations::util;

pub fn write_block(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: DataBlock,
    buf: &mut Vec<u8>,
) -> Result<(u64, HashMap<ColumnId, ColumnMeta>)> {
    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            let result =
                blocks_to_parquet(schema, vec![block], buf, write_settings.table_compression)?;
            let meta = util::column_parquet_metas(&result.1, schema)?;
            Ok((result.0, meta))
        }
        FuseStorageFormat::Native => {
            let arrow_schema = schema.to_arrow();
            let mut writer = NativeWriter::new(
                buf,
                arrow_schema,
                common_arrow::native::write::WriteOptions {
                    compression: write_settings.table_compression.into(),
                    max_page_size: Some(write_settings.max_page_size),
                },
            );

            let batch = ArrowChunk::try_from(block)?;

            writer.start()?;
            writer.write(&batch)?;
            writer.finish()?;

            let leaf_column_ids = schema.to_leaf_column_ids();
            let mut metas = HashMap::with_capacity(writer.metas.len());
            for (idx, meta) in writer.metas.iter().enumerate() {
                // use column id as key instead of index
                let column_id = leaf_column_ids.get(idx).unwrap();
                metas.insert(*column_id, ColumnMeta::Native(meta.clone()));
            }

            Ok((writer.total_size() as u64, metas))
        }
    }
}

/// Take ownership here to avoid extra copy.
pub async fn write_data(data: Vec<u8>, data_accessor: &Operator, location: &str) -> Result<()> {
    let o = data_accessor.object(location);
    o.write(data).await?;

    Ok(())
}
