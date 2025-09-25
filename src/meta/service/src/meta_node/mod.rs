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

//! MetaNode is the container of all meta service logic, without the service layer.

pub mod errors;
pub mod meta_handle;
#[allow(clippy::module_inception)]
pub mod meta_node;
pub mod meta_node_builder;
pub mod meta_node_kv_api_impl;
pub mod meta_node_status;
pub mod meta_worker;
