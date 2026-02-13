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

//! Defines the adapter for the `sub_cache` crate to work with the Meta service.

use std::future::Future;

use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::SeqV;
use sub_cache::TypeConfig;

use crate::meta_client_source::MetaClientSource;

#[derive(Debug, Clone, Default)]
pub struct MetaCacheTypes;

impl TypeConfig for MetaCacheTypes {
    type Value = SeqV;

    type Source = MetaClientSource;

    fn value_seq(value: &Self::Value) -> u64 {
        value.seq
    }

    fn spawn<F>(future: F, name: impl ToString)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        DatabendRuntime::spawn(future, Some(name));
    }
}
