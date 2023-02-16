// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_expression::DataBlock;
use storages_common_table_meta::meta::BlockMeta;

use crate::io::read::ReadSettings;
use crate::io::BlockReader;

impl BlockReader {
    /// Read a parquet file and convert to DataBlock.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_parquet_by_meta(
        &self,
        settings: &ReadSettings,
        meta: &BlockMeta,
    ) -> Result<DataBlock> {
        let columns_meta = &meta.col_metas;

        // Get the merged IO read result.
        let merge_io_read_result = self
            .read_columns_data_by_merge_io(settings, &meta.location.0, columns_meta)
            .await?;

        // Get the columns chunk.
        let column_chunks = merge_io_read_result.columns_chunks()?;

        let num_rows = meta.row_count as usize;

        self.deserialize_parquet_chunks_with_buffer(
            &meta.location.0,
            num_rows,
            &meta.compression,
            columns_meta,
            column_chunks,
            None,
        )
    }
}
