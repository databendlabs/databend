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

use std::sync::Arc;

use common_metatypes::MetaId;
use common_metatypes::MetaVersion;

use crate::catalogs::Table;
use crate::catalogs::TableFunction;

//#[deprecated]
pub type TableMeta = Meta<Arc<dyn Table>>;
//#[deprecated]
pub type TableFunctionMeta = Meta<Arc<dyn TableFunction>>;

/// The wrapper of the T with meta version.
#[derive(Debug, Clone)]
pub struct Meta<T> {
    t: T,
    id: MetaId,
    version: Option<MetaVersion>,
}

impl<T> Meta<T> {
    pub fn create(t: T, id: MetaId) -> Self {
        Self::with_version(t, id, None)
    }

    pub fn with_version(t: T, id: MetaId, version: Option<MetaVersion>) -> Self {
        Self { t, id, version }
    }

    pub fn meta_ver(&self) -> Option<MetaVersion> {
        self.version
    }

    pub fn meta_id(&self) -> MetaId {
        self.id
    }

    pub fn raw(&self) -> &T {
        &self.t
    }
}
