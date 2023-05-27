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

use std::io::SeekFrom;
use std::ops::Range;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
// use common_arrow::arrow::datatypes::Schema as ArrowSchema;
// use common_arrow::arrow::io::parquet::read as pread;
// use common_arrow::arrow::io::parquet::read::read_metadata_async;
// use common_arrow::parquet::metadata::ParquetMetaData;
use common_base::runtime::execute_futures_in_parallel;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::BoxFuture;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::AsyncSeek;
use futures::AsyncSeekExt;
use futures::FutureExt;
use futures::TryFutureExt;
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::parquet_to_arrow_schema_by_columns;
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::footer::decode_footer;
use parquet::file::footer::decode_metadata;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::FOOTER_SIZE;

#[async_backtrace::framed]
pub async fn read_parquet_schema_async(operator: &Operator, path: &str) -> Result<ArrowSchema> {
    let metadata = read_metadata_async(operator, path).await?;
    let schema = parquet_to_arrow_schema_by_columns(
        &metadata.file_metadata().schema_descr_ptr(),
        ProjectionMask::all(),
        None,
    )
    .map_err(|err| ErrorCode::ParquetFileInvalid(format!("{:?}", err)))?;
    Ok(schema)
}

async fn read_parquet_metas_batch(
    file_infos: Vec<(String, u64)>,
    op: Operator,
) -> Result<Vec<Arc<ParquetMetaData>>> {
    // todo(youngsofun): we should use size in StageFileInfo, but parquet2 do not have the interface for now
    let mut metas = vec![];
    for (path, _size) in file_infos {
        metas.push(read_metadata_async(&op, &path).await?);
    }
    Ok(metas)
}

#[async_backtrace::framed]
pub async fn read_parquet_metas_in_parallel(
    op: Operator,
    file_infos: Vec<(String, u64)>,
    thread_nums: usize,
    permit_nums: usize,
) -> Result<Vec<Arc<ParquetMetaData>>> {
    let batch_size = 1000;
    if file_infos.len() <= batch_size {
        read_parquet_metas_batch(file_infos, op.clone()).await
    } else {
        let mut chunks = file_infos.chunks(batch_size);

        let tasks = std::iter::from_fn(move || {
            chunks
                .next()
                .map(|location| read_parquet_metas_batch(location.to_vec(), op.clone()))
        });

        let result = execute_futures_in_parallel(
            tasks,
            thread_nums,
            permit_nums,
            "read-parquet-metas-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<Vec<_>>>>()?
        .into_iter()
        .flatten()
        .collect();

        Ok(result)
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage
#[derive(Clone, Debug)]
pub struct ParquetObjectReader {
    op: Operator,
    location: String,
}

impl ParquetObjectReader {
    pub fn new(op: Operator, location: String) -> Self {
        Self { op, location }
    }
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.op
                .range_read(&self.location, range.start as u64..range.end as u64)
                .await
                .map(Bytes::from)
                .map_err(|err| ParquetError::General(format!("{:?}", err)))
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        read_metadata_async(&self.op, &self.location)
            .map_err(|err| ParquetError::General(format!("{:?}", err)))
            .boxed()
    }
}

async fn read_metadata_async(op: &Operator, location: &str) -> Result<Arc<ParquetMetaData>> {
    const FOOTER_SIZE_I64: i64 = FOOTER_SIZE as i64;

    let mut reader = op.reader(location).await?;
    reader.seek(SeekFrom::End(-FOOTER_SIZE_I64)).await?;

    let mut buf = [0_u8; FOOTER_SIZE];
    reader.read_exact(&mut buf).await?;

    let metadata_len =
        decode_footer(&buf).map_err(|err| ErrorCode::ParquetFileInvalid(format!("{:?}", err)))?;

    reader
        .seek(SeekFrom::End(-FOOTER_SIZE_I64 - metadata_len as i64))
        .await?;

    let mut buf = Vec::with_capacity(metadata_len);
    reader.take(metadata_len as _).read_to_end(&mut buf).await?;

    Ok(Arc::new(decode_metadata(&buf).map_err(|err| {
        ErrorCode::ParquetFileInvalid(format!("{:?}", err))
    })?))
}
