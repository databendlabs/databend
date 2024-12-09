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

mod command;
mod desc;
mod id;
mod stream;
mod stream_sender;
mod subscriber;
mod subscriber_handle;

use std::collections::Bound;

pub use desc::WatchDesc;
pub use id::WatcherId;
pub(crate) use stream::WatchStream;
pub(crate) use stream_sender::StreamSender;
pub(crate) use subscriber::EventSubscriber;
pub(crate) use subscriber_handle::SubscriberHandle;

pub(crate) type KeyRange = (Bound<String>, Bound<String>);
