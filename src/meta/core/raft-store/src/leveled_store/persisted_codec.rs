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

//! Convert one type to another type for this crate to convert between 3rd party types.

use std::io;

/// Convert one type `Self` to type `T` for persisting on disk for this crate to convert between 3rd party types.
pub trait PersistedCodec<T>
where Self: Sized
{
    /// Convert `Self` to `T`.
    fn encode_to(self) -> Result<T, io::Error>;

    /// Parse `T` back to `Self`.
    fn decode_from(value: T) -> Result<Self, io::Error>;
}
