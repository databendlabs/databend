// Copyright [2021] [Jorge C Leitao]
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

use std::io::Write;

#[cfg(feature = "async")]
use futures::AsyncWrite;
use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
#[cfg(feature = "async")]
use parquet_format_safe::thrift::protocol::TCompactOutputStreamProtocol;

use super::serialize::serialize_column_index;
use super::serialize::serialize_offset_index;
use crate::error::Result;
#[allow(unused_imports)]
pub use crate::metadata::KeyValue;
use crate::write::page::PageWriteSpec;

pub fn write_column_index<W: Write>(writer: &mut W, pages: &[PageWriteSpec]) -> Result<u64> {
    let index = serialize_column_index(pages)?;
    let mut protocol = TCompactOutputProtocol::new(writer);
    Ok(index.write_to_out_protocol(&mut protocol)? as u64)
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub async fn write_column_index_async<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    pages: &[PageWriteSpec],
) -> Result<u64> {
    let index = serialize_column_index(pages)?;
    let mut protocol = TCompactOutputStreamProtocol::new(writer);
    Ok(index.write_to_out_stream_protocol(&mut protocol).await? as u64)
}

pub fn write_offset_index<W: Write>(writer: &mut W, pages: &[PageWriteSpec]) -> Result<u64> {
    let index = serialize_offset_index(pages)?;
    let mut protocol = TCompactOutputProtocol::new(&mut *writer);
    Ok(index.write_to_out_protocol(&mut protocol)? as u64)
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub async fn write_offset_index_async<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    pages: &[PageWriteSpec],
) -> Result<u64> {
    let index = serialize_offset_index(pages)?;
    let mut protocol = TCompactOutputStreamProtocol::new(&mut *writer);
    Ok(index.write_to_out_stream_protocol(&mut protocol).await? as u64)
}
