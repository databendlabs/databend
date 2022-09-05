//  Copyright 2022 Datafuse Labs.
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

use std::fmt::Display;

/// Constructs the prefix path which covers all the data of of a give table identity
pub fn table_storage_prefix(database_id: impl Display, table_id: impl Display) -> String {
    format!("{}/{}", database_id, table_id)
}

/// Constructs the prefix path which covers all the data of of a give table identity
pub fn database_storage_prefix(database_id: impl Display) -> String {
    format!("{}", database_id)
}
