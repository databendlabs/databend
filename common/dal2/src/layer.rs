// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use crate::Accessor;

/// Layer is used to intercept the operations on the underlying storage.
///
/// Struct that implement this trait must accept input `Arc<dyn Accessor>` as inner,
/// and returns a new `Arc<dyn Accessor>` as output.
///
/// All functions in `Accessor` requires `&self`, so it's implementor's responsibility
/// to maintain the internal mutability. Please also keep in mind that `Accessor`
/// requires `Send` and `Sync`.
///
/// # Examples
///
/// ```
/// use dal2::{Accessor, Layer};
///
/// struct Trace {
///     inner: Arc<dyn Accessor>,
/// }
///
/// impl Accessor for Trace {}
///
/// impl Layer for Trace {
///     fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
///         Arc::new(Trace { inner })
///     }
/// }
/// ```
pub trait Layer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor>;
}

impl<T: Layer> Layer for Arc<T> {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        self.as_ref().layer(inner)
    }
}
