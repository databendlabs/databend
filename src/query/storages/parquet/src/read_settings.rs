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

use databend_common_exception::Result;
use databend_common_settings::Settings;

#[derive(Clone, Copy)]
pub struct ReadSettings {
    pub max_gap_size: u64,
    pub max_range_size: u64,
    pub parquet_fast_read_bytes: u64,
    pub enable_cache: bool,
}

impl Default for ReadSettings {
    fn default() -> Self {
        Self {
            max_gap_size: 48,
            max_range_size: 512 * 1024,
            parquet_fast_read_bytes: u64::MAX,
            enable_cache: false,
        }
    }
}

impl ReadSettings {
    pub fn from_settings(settings: &Settings) -> Result<ReadSettings> {
        Ok(ReadSettings {
            max_gap_size: settings.get_storage_io_min_bytes_for_seek()?,
            max_range_size: settings.get_storage_io_max_page_bytes_for_read()?,
            parquet_fast_read_bytes: settings.get_parquet_fast_read_bytes()?,
            enable_cache: false,
        })
    }

    pub fn with_enable_cache(self, enable_cache: bool) -> Self {
        Self {
            enable_cache,
            ..self
        }
    }
}
