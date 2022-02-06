// Copyright 2021 Datafuse Labs.
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

pub mod cache;
pub mod fuse;
pub mod github;
pub mod index;
pub mod memory;
pub mod null;
pub mod system;

mod storage_context;
mod storage_factory;
mod storage_table;
mod storage_table_read_plan;

pub use storage_context::StorageContext;
pub use storage_factory::StorageCreator;
pub use storage_factory::StorageDescription;
pub use storage_factory::StorageFactory;
pub use storage_table::Table;
pub use storage_table_read_plan::ToReadDataSourcePlan;
