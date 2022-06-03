// Copyright 2022 Datafuse Labs.
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

use std::io;

use common_exception::ErrorCode;
use common_exception::Result;
use futures::StreamExt;
use opendal::ObjectMode;
use opendal::Operator;

/// Get the files in the path, if the path is not exist, return an empty list.
pub async fn operator_list_files(op: &Operator, path: &str) -> Result<Vec<String>> {
    let mut list: Vec<String> = vec![];

    let o = op.object(path);

    // return an empty list if not exist
    let meta = match o.metadata().await {
        Ok(meta) => meta,
        Err(e) => {
            return match e.kind() {
                io::ErrorKind::NotFound => Ok(Vec::new()),
                _ => Err(e.into()),
            }
        }
    };
    match meta.mode() {
        ObjectMode::FILE => {
            list.push(o.path());
        }
        ObjectMode::DIR => {
            let mut objects = op.object(path).list().await?;
            while let Some(object) = objects.next().await {
                let name = object?.name();
                list.push(name);
            }
        }
        other => {
            return Err(ErrorCode::StorageOther(format!(
                "S3 list() can not handle the object mode: {:?}",
                other
            )))
        }
    }

    Ok(list)
}
