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

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_pipeline::core::InputError;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use parking_lot::RwLock;

type OnErrorMap = Arc<DashMap<String, HashMap<u16, InputError>>>;

pub struct CopyState {
    on_error_map: RwLock<Option<OnErrorMap>>,
    on_error_mode: RwLock<Option<OnErrorMode>>,
    copy_status: Arc<CopyStatus>,
}

impl Default for CopyState {
    fn default() -> Self {
        Self {
            on_error_map: RwLock::new(None),
            on_error_mode: RwLock::new(None),
            copy_status: Default::default(),
        }
    }
}

impl CopyState {
    pub fn add_file_status(&self, file_path: &str, file_status: FileStatus) {
        self.copy_status.add_chunk(file_path, file_status);
    }

    pub fn copy_status(&self) -> Arc<CopyStatus> {
        self.copy_status.clone()
    }

    pub fn set_on_error_map(&self, map: OnErrorMap) {
        *self.on_error_map.write() = Some(map);
    }

    pub fn get_on_error_map(&self) -> Option<OnErrorMap> {
        self.on_error_map.read().as_ref().cloned()
    }

    pub fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        self.on_error_mode.read().clone()
    }

    pub fn set_on_error_mode(&self, mode: OnErrorMode) {
        *self.on_error_mode.write() = Some(mode);
    }

    pub fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>> {
        let on_error_map = self.get_on_error_map()?;
        if on_error_map.is_empty() {
            return None;
        }

        let mut errors = HashMap::<String, ErrorCode>::new();
        on_error_map
            .iter()
            .for_each(|x: RefMulti<String, HashMap<u16, InputError>>| {
                if let Some(max_v) = x.value().iter().max_by_key(|entry| entry.1.num) {
                    errors.insert(x.key().to_string(), max_v.1.err.clone());
                }
            });
        Some(errors)
    }
}
