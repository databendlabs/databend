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

use std::io;

use futures_util::stream::BoxStream;

pub mod compact;
pub mod expirable;
pub(crate) mod impls;
pub mod map_api;
pub mod map_api_ro;
pub mod map_key;
pub mod map_value;
pub mod marked;
pub mod seq_value;
pub mod util;

pub use crate::expirable::Expirable;
pub use crate::map_api::MapApi;
pub use crate::map_api_ro::MapApiRO;
pub use crate::map_key::MapKey;
pub use crate::map_value::MapValue;
pub use crate::marked::Marked;

/// Represents a transition from one state to another.
/// The tuple contains the initial state and the resulting state.
pub type Transition<T> = (T, T);

/// A boxed stream that yields `Result` of key-value pairs or an `io::Error`.
/// The stream is 'static to ensure it can live for the entire duration of the program.
pub type IOResultStream<T> = BoxStream<'static, Result<T, io::Error>>;

/// A Marked value type of key type.
/// `M` represents the meta information associated with the value.
pub type MarkedOf<K, M> = Marked<M, <K as MapKey<M>>::V>;

/// A key-value pair used in a map.
/// `M` represents the meta information associated with the value.
pub type MapKV<K, M> = (K, MarkedOf<K, M>);

/// A stream of result of key-value returned by `range()`.
/// `M` represents the meta information associated with the value.
pub type KVResultStream<K, M> = IOResultStream<MapKV<K, M>>;
