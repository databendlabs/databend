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

#![feature(type_alias_impl_trait)]
#![allow(clippy::uninlined_format_args)]
#![feature(impl_trait_in_assoc_type)]
#![feature(cursor_remaining)]
#![feature(box_patterns)]
#![allow(clippy::diverging_sub_expression)]

mod async_source;
mod blocks_source;
mod empty_source;
mod one_block_source;
mod stream_source;
mod sync_source;
mod sync_source_receiver;

mod prefetch_async_source;

pub use async_source::AsyncSource;
pub use async_source::AsyncSourcer;
pub use blocks_source::BlocksSource;
pub use empty_source::EmptySource;
pub use one_block_source::OneBlockSource;
pub use prefetch_async_source::PrefetchAsyncSource;
pub use prefetch_async_source::PrefetchAsyncSourcer;
pub use stream_source::AsyncStreamSource;
pub use stream_source::StreamSource;
pub use stream_source::StreamSourceNoSkipEmpty;
pub use sync_source::SyncSource;
pub use sync_source::SyncSourcer;
pub use sync_source_receiver::SyncReceiverSource;
