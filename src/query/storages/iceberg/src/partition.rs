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

use databend_common_storages_parquet::ParquetFilePart;
use databend_common_storages_parquet::ParquetPart;

pub(crate) fn convert_file_scan_task(task: iceberg::scan::FileScanTask) -> ParquetPart {
    let file = ParquetFilePart {
        file: task.data_file_path.clone(),
        compressed_size: task.length,
        estimated_uncompressed_size: task.length * 5,
    };
    ParquetPart::ParquetFile(file)
}
