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

use databend_common_meta_types::Change;
use futures::future::BoxFuture;

use crate::watcher::subscriber::EventSubscriber;

/// An event sent to EventDispatcher.
#[allow(clippy::type_complexity)]
pub(crate) enum Command {
    /// Submit a kv change event to dispatcher
    KVChange(Change<Vec<u8>, String>),

    /// Send a fn to [`EventSubscriber`] to run it.
    ///
    /// The function will be called with a mutable reference to the dispatcher.
    Request {
        req: Box<dyn FnOnce(&mut EventSubscriber) + Send + 'static>,
    },

    /// Send a fn to [`EventSubscriber`] to run it asynchronously.
    RequestAsync {
        req: Box<dyn FnOnce(&mut EventSubscriber) -> BoxFuture<'static, ()> + Send + 'static>,
    },
}
