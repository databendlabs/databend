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

mod api;
mod dir_name;
mod helper;
mod item;
mod key;
mod key_builder;
mod key_codec;
mod key_parser;
mod message;
mod prefix;
mod test_suite;
mod value;
mod value_with_name;

pub(crate) mod testing;

pub use api::ApiBuilder;
pub use api::AsKVApi;
pub use api::KVApi;
pub use api::KVStream;
pub use dir_name::DirName;
pub use item::Item;
pub use item::NonEmptyItem;
pub use key::Key;
pub use key::KeyError;
pub use key_builder::KeyBuilder;
pub use key_codec::KeyCodec;
pub use key_parser::KeyParser;
pub use message::GetKVReply;
pub use message::GetKVReq;
pub use message::ListKVReply;
pub use message::ListKVReq;
pub use message::MGetKVReply;
pub use message::MGetKVReq;
pub use message::UpsertKVReply;
pub use message::UpsertKVReq;
pub use prefix::prefix_to_range;
pub use test_suite::TestSuite;
pub use value::Value;
pub use value_with_name::ValueWithName;
