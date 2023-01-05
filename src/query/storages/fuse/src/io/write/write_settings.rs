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

use common_storages_table_meta::table::TableCompression;

use crate::FuseStorageFormat;

pub struct WriteSettings {
    pub storage_format: FuseStorageFormat,
    pub table_compression: TableCompression,
    pub native_max_page_size: usize,
}

impl Default for WriteSettings {
    fn default() -> Self {
        WriteSettings {
            storage_format: FuseStorageFormat::Parquet,
            table_compression: Default::default(),
            native_max_page_size: 8192,
        }
    }
}
