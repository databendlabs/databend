// Copyright 2022 Datafuse Labs.
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

use crate::error::Result;
use crate::ops::io::Reader;
use crate::ops::Object;
use crate::ops::OpDelete;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;

#[async_trait]
pub trait Accessor: Send + Sync {
    /// Read data from the underlying storage into input writer.
    async fn read(&self, args: &OpRead) -> Result<Reader> {
        let _ = args;
        unimplemented!()
    }
    /// Write data from input reader to the underlying storage.
    async fn write(&self, r: Reader, args: &OpWrite) -> Result<usize> {
        let (_, _) = (r, args);
        unimplemented!()
    }
    /// Invoke the `stat` operation on the specified path.
    async fn stat(&self, args: &OpStat) -> Result<Object> {
        let _ = args;
        unimplemented!()
    }
    /// `Delete` will invoke the `delete` operation.
    ///
    /// ## Behavior
    ///
    /// - `Delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `Delete` will return `Ok(())` if the path is deleted successfully or not exist.
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let _ = args;
        unimplemented!()
    }
}
