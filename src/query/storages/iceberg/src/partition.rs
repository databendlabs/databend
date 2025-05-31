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

use databend_common_catalog::plan::PartInfo;
use databend_common_storages_parquet::DeleteTask;
use databend_common_storages_parquet::DeleteType;
use databend_common_storages_parquet::ParquetFilePart;
use databend_common_storages_parquet::ParquetPart;
use databend_storages_common_stage::SingleFilePartition;
use iceberg::spec::DataContentType;
use iceberg::spec::DataFileFormat;

pub(crate) fn convert_file_scan_task(task: iceberg::scan::FileScanTask) -> Box<dyn PartInfo> {
    match task.data_file_format {
        DataFileFormat::Orc => {
            let part = SingleFilePartition {
                path: task.data_file_path.clone(),
                size: task.length as usize,
            };
            Box::new(part)
        }
        DataFileFormat::Parquet => {
            let fn_part = || ParquetFilePart {
                file: task.data_file_path.clone(),
                compressed_size: task.length,
                estimated_uncompressed_size: task.length * 5,
                dedup_key: format!("{}_{}", task.data_file_path, task.length),
                bucket_option: None,
            };

            if !task.deletes.is_empty() {
                Box::new(ParquetPart::FileWithDeletes {
                    inner: fn_part(),
                    deletes: task
                        .deletes
                        .iter()
                        .filter_map(|file| {
                            let ty = match file.file_type {
                                DataContentType::Data => return None,
                                DataContentType::PositionDeletes => DeleteType::Position,
                                DataContentType::EqualityDeletes => DeleteType::Equality,
                            };
                            Some(DeleteTask {
                                path: file.file_path.clone(),
                                ty,
                                equality_ids: file.equality_ids.clone(),
                            })
                        })
                        .collect(),
                })
            } else {
                Box::new(ParquetPart::File(fn_part()))
            }
        }
        _ => unimplemented!(),
    }
}
