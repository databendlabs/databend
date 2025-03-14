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

//! The client watch a key range and get notified when the key range changes.

mod desc;
pub mod dispatch;
pub mod event_filter;
mod id;
pub mod key_range;
pub mod type_config;
pub mod util;
pub mod watch_stream;

use std::collections::Bound;

pub use event_filter::EventFilter;

pub use self::desc::WatchDesc;
pub use self::id::WatcherId;
use self::type_config::ErrorOf;
use self::type_config::KeyOf;
use self::type_config::ResponseOf;
use self::type_config::ValueOf;

pub type KeyRange<C> = (Bound<KeyOf<C>>, Bound<KeyOf<C>>);
pub type WatchResult<C> = Result<ResponseOf<C>, ErrorOf<C>>;
pub type KVResult<C> = Result<(KeyOf<C>, ValueOf<C>), std::io::Error>;
