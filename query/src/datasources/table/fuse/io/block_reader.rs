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
//

use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::decompress;
use common_arrow::arrow::io::parquet::read::page_stream_to_array;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::parquet::read::get_page_stream;
use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::IntoSeries;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Part;
use futures::StreamExt;

// TODO move these to a dedicated mod
mod cache_keys {
    use std::sync::Arc;

    use common_cache::LruCache;
    use common_infallible::Mutex;

    #[derive(PartialEq, Eq, Hash)]
    #[allow(dead_code)]
    pub struct BlockMetaCacheKey {
        block_id: String,
    }

    #[derive(PartialEq, Eq, Hash)]
    #[allow(dead_code)]
    pub struct BlockColKey {
        block_id: String,
        col_id: u32,
    }

    #[allow(dead_code)]
    pub type BlockColCache = Arc<Mutex<LruCache<BlockColKey, Vec<u8>>>>;
    #[allow(dead_code)]
    pub type BlockMetaCache = Arc<Mutex<LruCache<BlockMetaCacheKey, Vec<u8>>>>;
}

pub async fn do_read(
    part: Part,
    data_accessor: Arc<dyn DataAccessor>,
    projection: Vec<usize>,
    arrow_schema: ArrowSchema,
) -> Result<DataBlock> {
    let loc = &part.name;
    let col_num = projection.len();
    // TODO pass in parquet file len
    let mut reader = data_accessor.get_input_stream(loc, None)?;
    // TODO cache parquet meta
    let metadata = read_metadata_async(&mut reader)
        .await
        .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

    // we only put one page in the a parquet file (reference xxx)
    let row_group = 0;
    let cols = projection
        .clone()
        .into_iter()
        .map(|idx| (metadata.row_groups[row_group].column(idx).clone(), idx));

    let fields = arrow_schema.fields();

    use futures::TryStreamExt;
    let stream = futures::stream::iter(cols).map(|(col_meta, idx)| {
        let data_accessor = data_accessor.clone();
        async move {
            let mut reader = data_accessor.get_input_stream(loc, None)?;
            // TODO cache block column
            let col_pages = get_page_stream(&col_meta, &mut reader, vec![], Arc::new(|_, _| true))
                .await
                .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
            let pages = col_pages.map(|compressed_page| decompress(compressed_page?, &mut vec![]));
            // QUOTE(from arrow2): deserialize the pages. This is CPU bounded and SHOULD be done in a dedicated thread pool (e.g. Rayon)
            let array =
                page_stream_to_array(pages, &col_meta, fields[idx].data_type.clone()).await?;
            let array: Arc<dyn common_arrow::arrow::array::Array> = array.into();
            Ok::<_, ErrorCode>(DataColumn::Array(array.into_series()))
        }
    });

    // TODO configuration of the buffer size
    let buffer_size = 10;
    let n = std::cmp::min(buffer_size, col_num);
    let data_cols = stream.buffered(n).try_collect().await?;

    let schema = DataSchema::from(arrow_schema);
    let block = DataBlock::create(Arc::new(schema.project(projection)), data_cols);
    Ok(block)
}
