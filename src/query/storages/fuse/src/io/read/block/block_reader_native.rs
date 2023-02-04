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

use std::fs::File;
use std::io::Cursor;
use std::ops::Range;
use std::os::unix::prelude::FileExt;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::native::read::deserialize;
use common_arrow::native::ColumnMeta as NativeColumnMeta;
use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Value;
use opendal::raw::build_rooted_abs_path;
use opendal::Object;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::ColumnMeta;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::metrics::metrics_inc_remote_io_read_bytes;
use crate::metrics::metrics_inc_remote_io_read_milliseconds;
use crate::metrics::metrics_inc_remote_io_read_parts;
use crate::metrics::metrics_inc_remote_io_seeks;

#[derive(Clone)]
pub enum ReaderData {
    Bytes(Arc<Vec<u8>>),
    File(Arc<File>),
}

// Native storage format
impl BlockReader {
    pub async fn async_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<Vec<(usize, PagesReader)>> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let part = FusePartInfo::from_part(&part)?;
        let mut join_handlers = Vec::with_capacity(self.project_indices.len());

        for (index, (column_id, field, _)) in self.project_indices.iter() {
            if let Some(column_meta) = part.columns_meta.get(column_id) {
                join_handlers.push(Self::read_native_columns_data(
                    self.operator.object(&part.location),
                    *index,
                    column_meta,
                    &part.range,
                    field.data_type().clone(),
                ));

                // Perf
                {
                    let (_, len) = column_meta.offset_length();
                    metrics_inc_remote_io_seeks(1);
                    metrics_inc_remote_io_read_bytes(len);
                }
            }
        }

        let start = Instant::now();
        let res = futures::future::try_join_all(join_handlers).await;

        // Perf.
        {
            metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);
        }

        res
    }

    pub async fn read_native_columns_data(
        o: Object,
        index: usize,
        meta: &ColumnMeta,
        range: &Option<Range<usize>>,
        data_type: common_arrow::arrow::datatypes::DataType,
    ) -> Result<(usize, PagesReader)> {
        use backon::ExponentialBackoff;
        use backon::Retryable;

        let mut meta = meta.as_native().unwrap().clone();
        if let Some(range) = range {
            meta = meta.slice(range.start, range.end);
        }

        let reader = {
            || async {
                o.range_read(meta.offset..meta.offset + meta.total_len())
                    .await
            }
        }
        .retry(ExponentialBackoff::default())
        .when(|err| err.is_temporary())
        .await?;

        let reader = PagesReader::new(ReaderData::Bytes(Arc::new(reader)), meta, o, data_type);
        Ok((index, reader))
    }

    pub fn sync_read_native_columns_data(
        &self,
        part: PartInfoPtr,
    ) -> Result<Vec<(usize, PagesReader)>> {
        let part = FusePartInfo::from_part(&part)?;

        let mut results = Vec::with_capacity(self.project_indices.len());
        let object = self.operator.object(&part.location);
        let abs_path = build_rooted_abs_path(self.operator.metadata().root(), &part.location);

        let cache = CacheManager::instance().get_fd_cache();
        let file = if let Some(cache) = cache {
            if let Some(file) = cache.get(&abs_path) {
                file.clone()
            } else {
                let file = Arc::new(File::open(&abs_path).unwrap());
                cache.put(abs_path.clone(), file.clone());
                file
            }
        } else {
            Arc::new(File::open(&abs_path).unwrap())
        };

        for (index, (column_id, field, _)) in self.project_indices.iter() {
            if let Some(column_meta) = part.columns_meta.get(column_id) {
                let result = Self::sync_read_native_column(
                    object.clone(),
                    file.clone(),
                    *index,
                    column_meta,
                    &part.range,
                    field.data_type().clone(),
                );

                results.push(result?);
            }
        }

        Ok(results)
    }

    pub fn sync_read_native_column(
        o: Object,
        fd: Arc<File>,
        index: usize,
        column_meta: &ColumnMeta,
        range: &Option<Range<usize>>,
        data_type: common_arrow::arrow::datatypes::DataType,
    ) -> Result<(usize, PagesReader)> {
        let mut column_meta = column_meta.as_native().unwrap().clone();
        if let Some(range) = range {
            column_meta = column_meta.slice(range.start, range.end);
        }

        let reader = PagesReader::new(ReaderData::File(fd), column_meta, o, data_type);
        Ok((index, reader))
    }

    pub fn build_block(&self, chunks: Vec<(usize, Box<dyn Array>)>) -> Result<DataBlock> {
        let mut entries = Vec::with_capacity(chunks.len());
        // they are already the leaf columns without inner
        // TODO support tuple in native storage
        let mut rows = 0;
        for (index, (_, _, f)) in self.project_indices.iter() {
            if let Some(array) = chunks.iter().find(|c| c.0 == *index).map(|c| c.1.clone()) {
                entries.push(BlockEntry {
                    data_type: f.clone(),
                    value: Value::Column(Column::from_arrow(array.as_ref(), f)),
                });
                rows = array.len();
            }
        }
        Ok(DataBlock::new(entries, rows))
    }
}

pub struct PagesReader {
    pub(crate) data: ReaderData,
    pub(crate) meta: NativeColumnMeta,

    pub(crate) page_id: usize,
    pub(crate) _object: Object,
    pub(crate) data_type: ArrowType,
}

impl PagesReader {
    pub fn new(
        data: ReaderData,
        meta: NativeColumnMeta,
        object: Object,
        data_type: ArrowType,
    ) -> Self {
        Self {
            data,
            meta,
            _object: object,
            data_type,
            page_id: 0,
        }
    }

    pub fn skip_page(&mut self) {
        self.page_id += 1;
    }

    pub fn has_next(&self) -> bool {
        self.page_id < self.meta.pages.len()
    }

    pub fn next_array(&mut self) -> Result<Box<dyn Array>> {
        self.page_id += 1;
        read_page(
            self.data.clone(),
            self.page_id - 1,
            &self.meta,
            self.data_type.clone(),
        )
    }
}

fn read_page(
    data: ReaderData,
    page_id: usize,
    meta: &NativeColumnMeta,
    data_type: ArrowType,
) -> Result<Box<dyn Array>> {
    let page = &meta.pages[page_id as usize];
    let offset: u64 = meta
        .pages
        .iter()
        .take(page_id)
        .map(|p| p.length)
        .sum::<u64>()
        + meta.offset;

    let mut scatch = vec![];
  
    let result = match data {
        ReaderData::Bytes(bytes) => {
            let reader = &bytes.as_slice()[offset as usize..(offset + page.length) as usize];
            let mut reader = Cursor::new(reader);
            deserialize::read(
                &mut reader,
                data_type,
                page.num_values as usize,
                &mut scatch,
            )
        }
        ReaderData::File(file) => {
            let mut buf = vec![0; page.length as usize];
            let _ = file.read_at(buf.as_mut(), offset);
            let buf = Arc::new(buf);
            let reader = buf.as_slice();
            let mut reader = Cursor::new(reader);
            deserialize::read(
                &mut reader,
                data_type,
                page.num_values as usize,
                &mut scatch,
            )
        }
    }?;

    Ok(result)
}
