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

use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_compat::CompatExt;
use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncSeek;
use tokio::io::AsyncWriteExt;

use crate::AsyncSeekableReader;
use crate::DataAccessor;
use crate::InputStream;

pub struct Memory {
    bytes: Vec<u8>,
}

impl Memory {
    pub fn new(bytes: Vec<u8>) -> Memory {
        Memory { bytes }
    }
}

#[async_trait::async_trait]
impl DataAccessor for Memory {
    fn get_input_stream(&self, _path: &str, _stream_len: Option<u64>) -> Result<InputStream> {
        let cursor = Cursor::new(self.bytes.clone());
        Ok(Box::new(cursor.compat()))
    }

    async fn put(&self, path: &str, content: Vec<u8>) -> common_exception::Result<()> {
        Err(ErrorCode::UnImplement(
            "put fn is not supported for memory dal",
        ))
    }

    async fn put_stream(
        &self,
        path: &str,
        input_stream: Box<
            dyn Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>
                + Send
                + Unpin
                + 'static,
        >,
        _stream_len: usize,
    ) -> common_exception::Result<()> {
        Err(ErrorCode::UnImplement(
            "put_stream fn is not supported for memory dal",
        ))
    }
}
