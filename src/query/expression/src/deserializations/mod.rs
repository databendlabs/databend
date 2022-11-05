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

use common_io::prelude::*;

mod boolean;
mod date;
mod empty_array;
mod null;
mod nullable;
mod number;
mod string;
mod timestamp;
mod variant;

pub use boolean::*;
pub use date::*;
pub use empty_array::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
pub use string::*;
pub use timestamp::*;
pub use variant::*;

use crate::Column;
use crate::Scalar;

pub trait TypeDeserializer: Send + Sync {
    fn memory_size(&self) -> usize;

    fn de_default(&mut self, format: &FormatSettings);

    fn append_data_value(&mut self, value: Scalar, format: &FormatSettings) -> Result<(), String>;

    /// Note this method will return err only when inner builder is empty.
    fn pop_data_value(&mut self) -> Result<Scalar, String>;

    fn finish_to_column(&mut self) -> Column;
}
