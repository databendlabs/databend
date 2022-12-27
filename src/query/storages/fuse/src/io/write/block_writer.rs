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

use backon::ExponentialBackoff;
use backon::Retryable;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::native::write::PaWriter;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_storages_common::blocks_to_parquet;
use common_storages_table_meta::meta::ColumnId;
use common_storages_table_meta::meta::ColumnMeta;
use opendal::Operator;
use tracing::warn;

use crate::fuse_table::FuseStorageFormat;
use crate::io::write::WriteSettings;
use crate::operations::util;

pub fn write_block(
    write_settings: &WriteSettings,
    block: DataBlock,
    buf: &mut Vec<u8>,
) -> Result<(u64, HashMap<ColumnId, ColumnMeta>)> {
    let schema = block.schema().clone();

    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            let result =
                blocks_to_parquet(&schema, vec![block], buf, write_settings.table_compression)?;
            let meta = util::column_metas(&result.1)?;
            Ok((result.0, meta))
        }
        FuseStorageFormat::Native => {
            let arrow_schema = block.schema().as_ref().to_arrow();
            let mut writer = PaWriter::new(
                buf,
                arrow_schema,
                common_arrow::native::write::WriteOptions {
                    compression: common_arrow::native::Compression::from(
                        write_settings.table_compression,
                    ),
                    max_page_size: Some(write_settings.native_max_page_size),
                },
            );

            let batch = Chunk::try_from(block)?;

            writer.start()?;
            writer.write(&batch)?;
            writer.finish()?;

            let metas = writer
                .metas
                .iter()
                .enumerate()
                .map(|(idx, meta)| {
                    (
                        idx as ColumnId,
                        ColumnMeta::new(meta.offset, meta.length, meta.num_values),
                    )
                })
                .collect();
            Ok((writer.total_size() as u64, metas))
        }
    }
}

pub async fn write_data(data: &[u8], data_accessor: &Operator, location: &str) -> Result<()> {
    let object = data_accessor.object(location);

    { || object.write(data) }
        .retry(ExponentialBackoff::default().with_jitter())
        .when(|err| err.is_temporary())
        .notify(|err, dur| {
            warn!(
                "fuse table block writer write_data retry after {}s for error {:?}",
                dur.as_secs(),
                err
            )
        })
        .await?;

    Ok(())
}
