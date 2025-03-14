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

//! Defines the key behavior of the map.

use std::fmt;

use crate::map_value::MapValue;

/// MapKey defines the behavior of a key in a map.
///
/// It is `Clone` to let MapApi clone a range of key.
/// It is `Unpin` to let MapApi extract a key from pinned data, such as a stream.
/// And it only accepts `static` value for simplicity.
///
/// `M` is the metadata type associated with the value `V`.
pub trait MapKey<M>: Clone + Ord + fmt::Debug + Send + Sync + Unpin + 'static {
    type V: MapValue;
}

mod impls {
    use super::MapKey;

    impl<M> MapKey<M> for String {
        type V = Vec<u8>;
    }
}
