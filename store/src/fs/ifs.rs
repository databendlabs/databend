// Copyright 2020 Datafuse Labs.
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

use async_trait::async_trait;
use common_exception::exception;

use crate::fs::ListResult;

/// Abstract storage layer API.
#[async_trait]
pub trait FileSystem
where Self: Sync + Send
{
    /// Add file atomically.
    /// AKA put_if_absent
    async fn add(&self, path: &str, data: &[u8]) -> common_exception::Result<()>;

    /// read all bytes from a file
    async fn read_all(&self, path: &str) -> exception::Result<Vec<u8>>;

    /// List dir and returns directories and files.
    async fn list(&self, prefix: &str) -> common_exception::Result<ListResult>;

    // async fn read(
    //     path: &str,
    //     offset: usize,
    //     length: usize,
    //     buf: &mut [u8],
    // ) -> common_exception::Result<usize>;
}
