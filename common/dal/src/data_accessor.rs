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

use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::sync::mpsc::channel;
use std::sync::Arc;

use common_base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::stream::Stream;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::AsyncSeek;
use serde::de::DeserializeOwned;

use crate::Local;
use crate::StorageScheme;
use crate::S3;

pub type Bytes = Vec<u8>;

pub trait AsyncSeekableReader: futures::AsyncRead + futures::AsyncSeek {}

impl<T> AsyncSeekableReader for T where T: AsyncRead + AsyncSeek {}

pub type InputStream = Box<dyn AsyncSeekableReader + Send + Unpin>;

pub trait SeekableReader: Read + Seek {}

impl<T> SeekableReader for T where T: Read + Seek {}

#[async_trait::async_trait]
pub trait DataAccessor: Send + Sync {
    fn get_reader(&self, path: &str, len: Option<u64>) -> Result<Box<dyn SeekableReader>>;

    fn get_writer(&self, path: &str) -> Result<Box<dyn Write>>;

    fn get_input_stream(&self, path: &str, stream_len: Option<u64>) -> Result<InputStream>;

    async fn get(&self, path: &str) -> Result<Bytes>;

    async fn put(&self, path: &str, content: Vec<u8>) -> Result<()>;

    async fn put_stream(
        &self,
        path: &str,
        input_stream: Box<
            dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
        >,
        stream_len: usize,
    ) -> Result<()>;

    async fn read(&self, location: &str) -> Result<Vec<u8>> {
        let mut input_stream = self.get_input_stream(location, None)?;
        let mut buffer = vec![];
        input_stream.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }
}

#[derive(Clone)]
pub struct ObjectAccessor {
    data_accessor: Arc<dyn DataAccessor>,
}

impl ObjectAccessor {
    /// Create an ObjectAccessor upon a raw data accessor.
    pub fn new(data_accessor: Arc<dyn DataAccessor>) -> ObjectAccessor {
        ObjectAccessor { data_accessor }
    }

    /// Async read an object.
    pub async fn read_obj<T: DeserializeOwned>(&self, loc: &str) -> Result<T> {
        let bytes = self.data_accessor.read(loc).await?;
        let r = serde_json::from_slice::<T>(&bytes)?;
        Ok(r)
    }

    /// Sync read object.
    pub fn blocking_read_obj<T, S>(&self, runtime: &S, loc: &str) -> Result<T>
    where
        T: DeserializeOwned,
        S: TrySpawn,
    {
        let bytes = self.blocking_read(runtime, loc)?;
        let r = serde_json::from_slice::<T>(&bytes)?;
        Ok(r)
    }

    // Sync read raw data.
    fn blocking_read<S: TrySpawn>(&self, runtime: &S, loc: &str) -> Result<Vec<u8>> {
        let (tx, rx) = channel();
        let location = loc.to_string();
        let da = self.data_accessor.clone();
        runtime.try_spawn(async move {
            let res = da.read(&location).await;
            let _ = tx.send(res);
        })?;

        rx.recv().map_err(ErrorCode::from_std_error)?
    }
}

/// Methods to build a DataAccessor.
///
/// It also provides a simple default implementation.
pub trait DataAccessorBuilder: Sync + Send {
    fn build(&self, scheme: &StorageScheme) -> Result<Arc<dyn DataAccessor>> {
        match scheme {
            StorageScheme::S3 => Ok(Arc::new(S3::fake_new())),
            StorageScheme::LocalFs => Ok(Arc::new(Local::new("/tmp"))),
            _ => todo!(),
        }
    }
}

/// A default DataAccessorBuilder impl.
pub struct DefaultDataAccessorBuilder {}

impl DataAccessorBuilder for DefaultDataAccessorBuilder {}
