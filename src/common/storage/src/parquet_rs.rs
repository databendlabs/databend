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

use arrow_schema::Schema as ArrowSchema;
use common_base::runtime::execute_futures_in_parallel;
use common_base::runtime::GLOBAL_MEM_STAT;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::BlockingOperator;
use opendal::Operator;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::footer::decode_footer;
use parquet::file::footer::decode_metadata;
use parquet::file::metadata::ParquetMetaData;

pub const FOOTER_SIZE: u64 = 8;

fn convert_error(path: &str, e: parquet::errors::ParquetError) -> ErrorCode {
    ErrorCode::BadBytes(format!(
        "Failed to decode parquet metadata from file {}, error: {}",
        path, e
    ))
}

#[async_backtrace::framed]
pub async fn read_parquet_schema_async_rs(operator: &Operator, path: &str) -> Result<ArrowSchema> {
    let meta = read_metadata_async(path, operator, None)
        .await
        .map_err(|e| {
            ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
        })?;

    infer_schema_with_extension(&meta)
}

pub fn infer_schema_with_extension(meta: &ParquetMetaData) -> Result<ArrowSchema> {
    let meta = meta.file_metadata();
    let arrow_schema = parquet_to_arrow_schema(meta.schema_descr(), meta.key_value_metadata())
        .map_err(ErrorCode::from_std_error)?;
    // todo: Convert data types to extension types using meta information.
    // Mainly used for types such as Variant and Bitmap,
    // as they have the same physical type as String.
    Ok(arrow_schema)
}

#[allow(dead_code)]
async fn read_parquet_metas_batch(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    max_memory_usage: u64,
) -> Result<Vec<ParquetMetaData>> {
    let mut metas = vec![];
    for (path, size) in file_infos {
        metas.push(read_metadata_async(&path, &op, Some(size)).await?)
    }
    let used = GLOBAL_MEM_STAT.get_memory_usage();
    if max_memory_usage as i64 - used < 100 * 1024 * 1024 {
        Err(ErrorCode::Internal(format!(
            "not enough memory to load parquet file metas, max_memory_usage = {}, used = {}.",
            max_memory_usage, used
        )))
    } else {
        Ok(metas)
    }
}

#[async_backtrace::framed]
pub async fn read_parquet_metas_in_parallel(
    op: Operator,
    file_infos: Vec<(String, u64)>,
    thread_nums: usize,
    permit_nums: usize,
    max_memory_usage: u64,
) -> Result<Vec<ParquetMetaData>> {
    let batch_size = 100;
    if file_infos.len() <= batch_size {
        read_parquet_metas_batch(file_infos, op.clone(), max_memory_usage).await
    } else {
        let mut chunks = file_infos.chunks(batch_size);

        let tasks = std::iter::from_fn(move || {
            chunks.next().map(|location| {
                read_parquet_metas_batch(location.to_vec(), op.clone(), max_memory_usage)
            })
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

/// Layout of Parquet file
/// +---------------------------+-----+---+
/// |      Rest of file         |  B  | A |
/// +---------------------------+-----+---+
/// where A: parquet footer, B: parquet metadata.
///
/// The reader first reads DEFAULT_FOOTER_SIZE bytes from the end of the file.
/// If it is not enough according to the length indicated in the footer, it reads more bytes.
pub async fn read_metadata_async(
    path: &str,
    operator: &Operator,
    file_size: Option<u64>,
) -> Result<ParquetMetaData> {
    let file_size = match file_size {
        None => operator.stat(path).await?.content_length(),
        Some(n) => n,
    };
    check_footer_size(file_size)?;
    let footer = operator
        .range_read(path, (file_size - FOOTER_SIZE)..file_size)
        .await?;
    let metadata_len = decode_footer(&footer[0..8].try_into().unwrap())
        .map_err(|e| convert_error(path, e))? as u64;
    check_meta_size(file_size, metadata_len)?;
    let metadata = operator
        .range_read(path, (file_size - FOOTER_SIZE - metadata_len)..file_size)
        .await?;
    decode_metadata(&metadata).map_err(|e| convert_error(path, e))
}

#[allow(dead_code)]
pub fn read_metadata(
    path: &str,
    operator: &BlockingOperator,
    file_size: u64,
) -> Result<ParquetMetaData> {
    check_footer_size(file_size)?;
    let footer = operator.range_read(path, (file_size - FOOTER_SIZE)..file_size)?;
    let metadata_len = decode_footer(&footer[0..8].try_into().unwrap())
        .map_err(|e| convert_error(path, e))? as u64;
    check_meta_size(file_size, metadata_len)?;
    let metadata =
        operator.range_read(path, (file_size - FOOTER_SIZE - metadata_len)..file_size)?;
    decode_metadata(&metadata).map_err(|e| convert_error(path, e))
}

/// check file is large enough to hold footer
fn check_footer_size(file_size: u64) -> Result<()> {
    if file_size < FOOTER_SIZE {
        Err(ErrorCode::BadBytes(
            "Invalid Parquet file. Size is smaller than footer.",
        ))
    } else {
        Ok(())
    }
}

/// check file is large enough to hold metadata
fn check_meta_size(file_size: u64, metadata_len: u64) -> Result<()> {
    if metadata_len + FOOTER_SIZE > file_size {
        Err(ErrorCode::BadBytes(format!(
            "Invalid Parquet file. Reported metadata length of {} + {} byte footer, but file is only {} bytes",
            metadata_len, FOOTER_SIZE, file_size
        )))
    } else {
        Ok(())
    }
}
