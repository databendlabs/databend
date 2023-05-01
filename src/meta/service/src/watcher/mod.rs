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

mod watcher_manager;
mod watcher_stream;

pub(crate) use watcher_manager::DispatcherSender;
pub(crate) use watcher_manager::EventDispatcher;
pub use watcher_manager::EventDispatcherHandle;
pub use watcher_manager::WatcherId;
pub use watcher_manager::WatcherSender;
pub use watcher_stream::WatchStream;
pub use watcher_stream::WatchStreamHandle;
pub use watcher_stream::Watcher;
