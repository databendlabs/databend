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

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

/// Note: assuming table attribute will NOT be renamed.
/// which is not realistic, we should have column_id for each relation attribute.  
#[allow(dead_code)]
pub fn project_col_idx(schema: &DataSchemaRef, projection: &DataSchemaRef) -> Result<Vec<usize>> {
    let col_map = schema
        .fields()
        .iter()
        .enumerate()
        .fold(HashMap::new(), |mut v, (i, item)| {
            v.insert(item.name().to_string(), i);
            v
        });

    let mut proj_idx = vec![];

    for col in projection.fields() {
        let name = col.name();
        if let Some(idx) = col_map.get(col.name()) {
            proj_idx.push(*idx)
        } else {
            return Err(ErrorCode::IllegalSchema(format!(
                "column [{}] specified in projection, but does not exist in schema",
                name
            )));
        }
    }
    Ok(proj_idx)
}
