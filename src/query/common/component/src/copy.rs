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
use std::sync::RwLock;

use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use databend_common_exception::ErrorCode;
use databend_common_expression::RemoteExpr;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_pipeline::core::InputError;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;

type OnErrorMap = Arc<DashMap<String, HashMap<u16, InputError>>>;

/// The options of the running COPY, set while the query is planned and read while it runs.
#[derive(Default)]
struct CopySettings {
    on_error_map: Option<OnErrorMap>,
    on_error_mode: Option<OnErrorMode>,
    /// The cluster key of a COPY with `SORTED_BY_CLUSTER_KEY = TRUE`: when set, each input file's
    /// order is preserved.
    file_sort_keys: Option<Arc<[RemoteExpr<String>]>>,
}

#[derive(Default)]
pub struct CopyState {
    settings: RwLock<CopySettings>,
    // Updated for every chunk by the decoding threads, so it keeps its own concurrent map
    // instead of sharing the lock of the settings.
    copy_status: Arc<CopyStatus>,
}

impl CopyState {
    pub fn add_file_status(&self, file_path: &str, file_status: FileStatus) {
        self.copy_status.add_chunk(file_path, file_status);
    }

    pub fn copy_status(&self) -> Arc<CopyStatus> {
        self.copy_status.clone()
    }

    pub fn set_on_error_map(&self, map: OnErrorMap) {
        self.settings.write().unwrap().on_error_map = Some(map);
    }

    pub fn get_on_error_map(&self) -> Option<OnErrorMap> {
        self.settings.read().unwrap().on_error_map.clone()
    }

    pub fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        self.settings.read().unwrap().on_error_mode.clone()
    }

    pub fn set_on_error_mode(&self, mode: OnErrorMode) {
        self.settings.write().unwrap().on_error_mode = Some(mode);
    }

    pub fn set_file_sort_keys(&self, keys: Option<Vec<RemoteExpr<String>>>) {
        self.settings.write().unwrap().file_sort_keys = keys.map(Into::into);
    }

    /// The cluster key of the running COPY, if `SORTED_BY_CLUSTER_KEY` requires preserving the
    /// input order.
    pub fn get_file_sort_keys(&self) -> Option<Arc<[RemoteExpr<String>]>> {
        self.settings.read().unwrap().file_sort_keys.clone()
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
