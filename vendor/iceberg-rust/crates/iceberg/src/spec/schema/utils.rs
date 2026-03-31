// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use crate::{Error, ErrorKind, Result};

pub fn try_insert_field<V>(map: &mut HashMap<i32, V>, field_id: i32, value: V) -> Result<()> {
    map.insert(field_id, value).map_or_else(
        || Ok(()),
        |_| {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Found duplicate 'field.id' {field_id}. Field ids must be unique."),
            ))
        },
    )
}
