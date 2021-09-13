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
use common_cache::LruCache;
use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::IntoSeries;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::Part;
use common_runtime::tokio::sync::mpsc::Sender;
use futures::StreamExt;

use crate::datasources::dal::DataAccessor;
use crate::datasources::fuse_table::util::location_gen::block_location;

#[derive(PartialEq, Eq, Hash)]
pub struct BlockMetaCacheKey {
    block_id: String,
}

#[derive(PartialEq, Eq, Hash)]
pub struct BlockColKey {
    block_id: String,
    col_id: u32,
}

pub type BlockColCache = Arc<Mutex<LruCache<BlockColKey, Vec<u8>>>>;
pub type BlockMetaCache = Arc<Mutex<LruCache<BlockMetaCacheKey, Vec<u8>>>>;

#[allow(dead_code)]
pub struct BlockReader {
    meta_cache: BlockMetaCache,
    block_col_cache: BlockColCache,
    data_accessor: Arc<dyn DataAccessor>,
}

#[allow(dead_code)]
impl BlockReader {
    pub async fn read_block(
        &self,
        _part: Part,
        _data_accessor: Arc<dyn DataAccessor>,
        _projection: Vec<usize>,
        _sender: Sender<Result<DataBlock>>,
        _arrow_schema: ArrowSchema,
    ) -> Result<()> {
        Ok(())
    }
}

pub(crate) async fn read_part(
    part: Part,
    data_accessor: Arc<dyn DataAccessor>,
    projection: Vec<usize>,
    sender: Sender<Result<DataBlock>>,
    arrow_schema: &ArrowSchema,
) -> Result<()> {
    let loc = block_location(&part.name);
    // TODO pass in parquet file len
    let mut reader = data_accessor.get_input_stream(&loc, None).await?;
    let metadata = read_metadata_async(&mut reader)
        .await
        .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

    // only onw page in the the parquet
    let row_group = 0;
    let cols = projection
        .iter()
        .map(|idx| (metadata.row_groups[row_group].column(*idx), *idx));

    let fields = arrow_schema.fields();
    let mut arrays: Vec<Arc<dyn common_arrow::arrow::array::Array>> = vec![];
    for (col_meta, idx) in cols {
        // NOTE: here the page filter is !Send
        let pages = get_page_stream(col_meta, &mut reader, vec![], Arc::new(|_, _| true))
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
        let pages = pages.map(|compressed_page| decompress(compressed_page?, &mut vec![]));
        // QUOTE(from arrow2): deserialize the pages. This is CPU bounded and SHOULD be done in a dedicated thread pool (e.g. Rayon)
        let array = page_stream_to_array(
            pages,
            &metadata.row_groups[0].columns()[idx],
            fields[idx].data_type.clone(),
        )
        .await?;
        arrays.push(array.into());
    }

    let ser = arrays
        .into_iter()
        .map(|a| DataColumn::Array(a.into_series()))
        .collect::<Vec<_>>();

    let block = DataBlock::create(Arc::new(DataSchema::from(arrow_schema)), ser);
    sender
        .send(Ok(block))
        .await
        .map_err(|e| ErrorCode::BrokenChannel(e.to_string()))?;

    Ok(())
}
