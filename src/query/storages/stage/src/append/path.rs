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

use databend_common_compress::CompressAlgorithm;
use databend_storages_common_stage::CopyIntoLocationInfo;

pub fn unload_path(
    info: &CopyIntoLocationInfo,
    query_id: &str,
    group_id: usize,
    batch_id: usize,
    compression: Option<CompressAlgorithm>,
    partition: Option<&str>,
) -> String {
    let format_name =
        format!("{:?}", info.stage.file_format_params.get_type()).to_ascii_lowercase();

    let suffix: &str = &compression
        .map(|c| format!(".{}", c.extension()))
        .unwrap_or_default();

    let path = &info.path;
    if info.options.use_raw_path {
        return path.to_string();
    }

    let query_id = if info.options.include_query_id {
        format!("{query_id}_")
    } else {
        "".to_string()
    };
    if path.ends_with("data_") {
        return format!(
            "{}{}{:0>4}_{:0>8}.{}{}",
            path, query_id, group_id, batch_id, format_name, suffix
        );
    }
    let (path, sep) = if path == "/" {
        ("", "")
    } else if path.ends_with('/') {
        (path.as_str(), "")
    } else {
        (path.as_str(), "/")
    };
    let mut full_path = String::with_capacity(
        path.len() + sep.len() + partition.map(|p| p.len() + 1).unwrap_or(0) + query_id.len() + 32,
    );
    full_path.push_str(path);
    full_path.push_str(sep);
    if let Some(partition) = partition {
        full_path.push_str(partition);
        if !partition.ends_with('/') {
            full_path.push('/');
        }
    }
    full_path.push_str("data_");
    full_path.push_str(&query_id);
    full_path.push_str(&format!(
        "{:0>4}_{:0>8}.{}{}",
        group_id, batch_id, format_name, suffix
    ));
    full_path
}
