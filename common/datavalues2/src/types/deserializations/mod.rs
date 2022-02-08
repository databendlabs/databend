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

use common_exception::Result;

use crate::prelude::*;

mod boolean;
mod date;
mod date_time;
mod null;
mod nullable;
mod number;
mod string;

pub use boolean::*;
pub use date::*;
pub use date_time::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
pub use string::*;

pub trait TypeDeserializer: Send + Sync {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn de_default(&mut self);
    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()>;
    /// If error occurrs, append a null by default
    fn de_text(&mut self, reader: &[u8]) -> Result<()>;

    fn de_null(&mut self) -> bool {
        false
    }

    fn finish_to_column(&mut self) -> ColumnRef;
}
