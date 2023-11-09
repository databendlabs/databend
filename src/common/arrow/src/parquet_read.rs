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

use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::AsyncSeek;
use futures::AsyncSeekExt;
use parquet2::metadata::ColumnChunkMetaData;
use parquet2::metadata::RowGroupMetaData;

use crate::arrow::datatypes::Field;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::to_deserializer;
use crate::arrow::io::parquet::read::ArrayIter;

fn get_field_columns<'a>(
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Vec<&'a ColumnChunkMetaData> {
    columns
        .iter()
        .filter(|x| x.descriptor().path_in_schema[0] == field_name)
        .collect()
}

async fn _read_single_column_async<R>(
    reader: &mut R,
    meta: &ColumnChunkMetaData,
) -> Result<Vec<u8>>
where
    R: AsyncRead + AsyncSeek + Send + Unpin,
{
    let (start, len) = meta.byte_range();
    reader.seek(std::io::SeekFrom::Start(start)).await?;
    let mut chunk = vec![0; len as usize];
    reader.read_exact(&mut chunk).await?;
    Result::Ok(chunk)
}

pub async fn read_columns_async<'a, R: AsyncRead + AsyncSeek + Send + Unpin>(
    reader: &mut R,
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Result<Vec<(&'a ColumnChunkMetaData, Vec<u8>)>> {
    let col_metas = get_field_columns(columns, field_name);
    let mut cols = Vec::with_capacity(col_metas.len());
    for meta in col_metas {
        cols.push((meta, _read_single_column_async(reader, meta).await?))
    }
    Ok(cols)
}

// used when we can not use arrow::io::parquet::read::read_columns_many_async which need a factory of reader
pub async fn read_columns_many_async<'a, R: AsyncRead + AsyncSeek + Send + Unpin>(
    reader: &mut R,
    row_group: &RowGroupMetaData,
    fields: Vec<&Field>,
    chunk_size: Option<usize>,
) -> Result<Vec<ArrayIter<'a>>> {
    let mut arrays = Vec::with_capacity(fields.len());
    for field in fields {
        let columns = read_columns_async(reader, row_group.columns(), &field.name).await?;
        arrays.push(to_deserializer(
            columns,
            field.to_owned(),
            row_group.num_rows(),
            chunk_size,
            None,
        )?);
    }
    Ok(arrays)
}
