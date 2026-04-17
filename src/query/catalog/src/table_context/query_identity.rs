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

use std::collections::HashSet;

use crate::query_kind::QueryKind;

pub trait TableContextQueryIdentity: Send + Sync {
    fn attach_query_str(&self, kind: QueryKind, query: String);

    fn attach_query_hash(&self, text_hash: String, parameterized_hash: String);

    fn get_query_str(&self) -> String;

    fn get_query_parameterized_hash(&self) -> String;

    fn get_query_text_hash(&self) -> String;

    fn get_last_query_id(&self, index: i32) -> Option<String>;

    fn get_query_id_history(&self) -> HashSet<String>;
}
