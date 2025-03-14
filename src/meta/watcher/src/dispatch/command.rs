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

use futures::future::BoxFuture;

use crate::dispatch::Dispatcher;
use crate::type_config::KVChange;
use crate::type_config::TypeConfig;

/// A command sent to [`Dispatcher`].
#[allow(clippy::type_complexity)]
pub enum Command<C>
where C: TypeConfig
{
    /// Submit a key-value update event to dispatcher.
    Update(KVChange<C>),

    /// Send a fn to [`Dispatcher`] to run it.
    ///
    /// The function will be called with a mutable reference to the dispatcher.
    Func {
        req: Box<dyn FnOnce(&mut Dispatcher<C>) + Send + 'static>,
    },

    /// Send a fn to [`Dispatcher`] to run it asynchronously.
    AsyncFunc {
        req: Box<dyn FnOnce(&mut Dispatcher<C>) -> BoxFuture<'static, ()> + Send + 'static>,
    },

    /// Send a future to [`Dispatcher`] to run it.
    Future(BoxFuture<'static, ()>),
}
