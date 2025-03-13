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

use databend_common_meta_types::SeqV;
use futures::future::BoxFuture;

use crate::watcher::dispatch::Dispatcher;

/// A command sent to [`Dispatcher`].
#[allow(clippy::type_complexity)]
pub enum Command {
    /// Submit a key-value update event to dispatcher.
    Update(Update),

    /// Send a fn to [`Dispatcher`] to run it.
    ///
    /// The function will be called with a mutable reference to the dispatcher.
    Request {
        req: Box<dyn FnOnce(&mut Dispatcher) + Send + 'static>,
    },

    /// Send a fn to [`Dispatcher`] to run it asynchronously.
    RequestAsync {
        req: Box<dyn FnOnce(&mut Dispatcher) -> BoxFuture<'static, ()> + Send + 'static>,
    },
}

/// An update event for a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub key: String,
    pub before: Option<SeqV>,
    pub after: Option<SeqV>,
}

impl Update {
    pub fn new(key: String, before: Option<SeqV>, after: Option<SeqV>) -> Self {
        Self { key, before, after }
    }
}
