//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use common_base::BlockingWait;
use common_exception::ErrorCode;
use common_meta_embedded::MetaEmbedded;

use crate::catalogs::backends::impls::MetaCached;
use crate::catalogs::backends::impls::MetaSync;

pub struct MetaEmbeddedSync {
    pub meta_sync: MetaSync,
}

impl MetaEmbeddedSync {
    pub fn create() -> Result<Self, ErrorCode> {
        // TODO(xp): This can only be used for test: data will be removed when program quit.
        let meta_embedded = MetaEmbedded::new_temp().wait(None)??;
        let meta_cached = MetaCached::create(Arc::new(meta_embedded));
        let meta_sync = MetaSync::create(Arc::new(meta_cached), Some(Duration::from_millis(5000)));
        let meta = MetaEmbeddedSync { meta_sync };
        Ok(meta)
    }
}

impl Deref for MetaEmbeddedSync {
    type Target = MetaSync;

    fn deref(&self) -> &Self::Target {
        &self.meta_sync
    }
}
