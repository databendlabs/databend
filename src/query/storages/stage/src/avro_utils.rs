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

pub(crate) fn avro_tuple_field_names(fields_name: &[String]) -> Vec<String> {
    let mut used = HashSet::with_capacity(fields_name.len());
    fields_name
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let candidate = if is_valid_avro_name(name) {
                name.clone()
            } else {
                format!("field_{idx}")
            };
            if used.insert(candidate.clone()) {
                return candidate;
            }

            let mut disambiguated = format!("field_{idx}");
            while !used.insert(disambiguated.clone()) {
                disambiguated = format!("field_{idx}_{}", used.len());
            }
            disambiguated
        })
        .collect()
}

fn is_valid_avro_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !matches!(first, 'A'..='Z' | 'a'..='z' | '_') {
        return false;
    }
    chars.all(|c| matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_'))
}
