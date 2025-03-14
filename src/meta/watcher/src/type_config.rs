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

use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::io;

pub type KVChange<C> = (KeyOf<C>, Option<ValueOf<C>>, Option<ValueOf<C>>);

pub type KeyOf<C> = <C as TypeConfig>::Key;
pub type ValueOf<C> = <C as TypeConfig>::Value;
pub type ResponseOf<C> = <C as TypeConfig>::Response;
pub type ErrorOf<C> = <C as TypeConfig>::Error;

pub trait TypeConfig
where Self: Debug + Clone + Copy + Sized + 'static
{
    type Key: Debug + Clone + Ord + Send + Sync + 'static;

    type Value: Debug + Clone + Send + Sync + 'static;

    type Response: Send + 'static;
    type Error: Error + Send + 'static;

    /// Create a response instance from a key-value change.
    fn new_response(change: KVChange<Self>) -> Self::Response;

    /// Create an error when the data source returns io::Error
    fn data_error(error: io::Error) -> Self::Error;

    /// Update the watcher count metrics by incrementing or decrementing by the given value.
    ///
    /// # Arguments
    /// * `delta` - The change in watcher count (positive for increment, negative for decrement)
    fn update_watcher_metrics(delta: i64);

    /// Spawn a task in the provided runtime.
    fn spawn<T>(fut: T)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static;
}
