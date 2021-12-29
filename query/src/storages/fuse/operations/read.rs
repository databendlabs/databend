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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::parquet::metadata::FileMetaData;
use common_dal::DataAccessor;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Extras;
use common_streams::ParquetSource;
use common_streams::SendableDataBlockStream;
use common_streams::Source;
use common_tracing::tracing_futures::Instrument;
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::sessions::QueryContext;
use crate::storages::fuse::FuseTable;

static META_CACHE: OnceCell<RwLock<HashMap<String, FileMetaData>>> = OnceCell::new();

impl FuseTable {
    #[inline]
    pub async fn do_read(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let default_proj = || {
            (0..self.table_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        let projection = if let Some(Extras {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            default_proj()
        };

        let bite_size = ctx.get_settings().get_parallel_read_threads()?;
        let ctx_clone = ctx.clone();
        let iter =
            std::iter::from_fn(
                move || match ctx_clone.clone().try_get_partitions(bite_size) {
                    Err(_) => None,
                    Ok(parts) if parts.is_empty() => None,
                    Ok(parts) => Some(parts),
                },
            )
            .flatten();
        let da = ctx.get_data_accessor()?;
        let arrow_schema = self.table_info.schema().to_arrow();
        let table_schema = Arc::new(DataSchema::from(arrow_schema));

        let part_stream = futures::stream::iter(iter);

        // fallback to stream combinator from async_stream, since
        // 1. when using `stream!`, the trace always contains a unclosed call-span (of ParquetSource::read)
        // 2. later, when `bit_size` is larger than one, the async reads could be buffered

        let dedicated_decompression_thread_pool =
            ctx.get_settings().get_decompress_in_thread_pool()? == 1;
        let stream = part_stream
            .map(move |part| {
                let da = da.clone();
                let table_schema = table_schema.clone();
                let projection = projection.clone();
                async move {
                    let parts = part.name.split("-").collect::<Vec<_>>();
                    if parts.len() != 2 {
                        return Err(ErrorCode::LogicalError("invalid part format"));
                    }
                    let name = parts[0];
                    let size = parts[1].parse::<u64>()?;
                    let cache = META_CACHE.get_or_init(|| {
                        common_infallible::RwLock::new(std::collections::HashMap::new())
                    });

                    let meta = { cache.read().get(name).cloned() };

                    let parquet_meta = match meta {
                        Some(m) => m,
                        _ => {
                            let mut reader = da.get_input_stream(name, Some(size))?;
                            let parquet_meta = read_metadata_async(&mut reader)
                                .await
                                .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
                            cache.write().insert(name.to_owned(), parquet_meta.clone());
                            parquet_meta
                        }
                    };

                    let mut source = ParquetSource::with_meta(
                        da,
                        name.to_owned(),
                        table_schema,
                        projection,
                        parquet_meta,
                        size,
                    );
                    if dedicated_decompression_thread_pool {
                        source.enable_decompress_in_pool()
                    }
                    source
                        .read()
                        .await
                        .map_err(|e| {
                            ErrorCode::ParquetError(format!(
                                "fail to read block {}, {}",
                                name,
                                e.to_string()
                            ))
                        })?
                        .ok_or_else(|| {
                            ErrorCode::ParquetError(format!(
                                "reader returns None for block {}",
                                name,
                            ))
                        })
                }
            })
            .buffer_unordered(bite_size as usize) // buffer_unordered?
            .instrument(common_tracing::tracing::Span::current());
        Ok(Box::pin(stream))
    }
}
