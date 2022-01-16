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

mod catalog;
mod catalog_context;
mod table_id_ranges;
mod table_memory_meta;

mod backends;
mod impls;

pub use backends::MetaBackend;
pub use catalog::Catalog;
pub use catalog_context::CatalogContext;
pub use impls::DatabaseCatalog;
pub use impls::ImmutableCatalog;
pub use impls::MutableCatalog;
pub use table_id_ranges::*;
pub use table_memory_meta::InMemoryMetas;
