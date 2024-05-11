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

use databend_common_arrow::arrow::datatypes::DataType as ArrowType;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::parquet::read as pread;
use databend_common_arrow::parquet::metadata::FileMetaData;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use opendal::Operator;

#[async_backtrace::framed]
pub async fn read_parquet_schema_async(operator: &Operator, path: &str) -> Result<ArrowSchema> {
    let meta = operator.stat(path).await?;
    let reader = operator.reader(path).await?;
    let meta = pread::read_metadata_async(reader, meta.content_length())
        .await
        .map_err(|e| {
            ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
        })?;

    infer_schema_with_extension(&meta)
}

pub fn infer_schema_with_extension(meta: &FileMetaData) -> Result<ArrowSchema> {
    let arrow_schema = pread::infer_schema(meta)?;
    // Convert data types to extension types using meta information.
    // Mainly used for types such as Variant and Bitmap,
    // as they have the same physical type as String.
    if let Some(metas) = meta.key_value_metadata() {
        let mut new_fields = arrow_schema.fields.clone();
        for (i, field) in arrow_schema.fields.iter().enumerate() {
            for meta in metas {
                if field.name == meta.key {
                    let data_type = ArrowType::Extension(
                        meta.value.clone().unwrap(),
                        Box::new(field.data_type.clone()),
                        None,
                    );
                    let new_field =
                        ArrowField::new(field.name.clone(), data_type, field.is_nullable);
                    new_fields[i] = new_field;
                    break;
                }
            }
        }
        Ok(new_fields.into())
    } else {
        Ok(arrow_schema)
    }
}

async fn read_parquet_metas_batch(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    max_memory_usage: u64,
) -> Result<Vec<FileMetaData>> {
    // todo(youngsofun): we should use size in StageFileInfo, but parquet2 do not have the interface for now
    let mut metas = vec![];
    for (path, size) in file_infos {
        let reader = op.reader(&path).await?;
        metas.push(pread::read_metadata_async(reader, size).await?)
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
    num_threads: usize,
    max_memory_usage: u64,
) -> Result<Vec<FileMetaData>> {
    let num_files = file_infos.len();
    let mut tasks = Vec::with_capacity(num_threads);

    // Equally distribute the tasks
    for i in 0..num_threads {
        let begin = num_files * i / num_threads;
        let end = num_files * (i + 1) / num_threads;
        if begin == end {
            continue;
        }
        let file_infos = file_infos[begin..end].to_vec();
        let op = op.clone();
        tasks.push(read_parquet_metas_batch(file_infos, op, max_memory_usage));
    }

    let now = std::time::Instant::now();
    log::info!("begin read {} parquet file metas", num_files);
    let result = execute_futures_in_parallel(
        tasks,
        num_threads,
        num_threads * 2,
        "read-parquet-metas-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<Vec<_>>>>()?
    .into_iter()
    .flatten()
    .collect();
    let elapsed = now.elapsed();
    log::info!(
        "end read {} parquet file metas, use {} secs",
        num_files,
        elapsed.as_secs_f32()
    );
    Ok(result)
}
