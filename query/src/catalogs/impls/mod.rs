// Copyright 2020 Datafuse Labs.
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
//

pub use database_catalog::DatabaseCatalog;

pub use crate::catalogs::meta_id_ranges::LOCAL_TBL_ID_BEGIN;
pub use crate::catalogs::meta_id_ranges::SYS_TBL_ID_BEGIN;
pub use crate::catalogs::meta_id_ranges::SYS_TBL_ID_END;

mod catalog;
mod database_catalog;
pub(crate) mod meta_backends;
pub(crate) mod util;
