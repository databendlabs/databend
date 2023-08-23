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

use syn::{Data, DeriveInput, Fields, FieldsNamed};

/// Parses the tokens_input to a DeriveInput and returns the struct name from which it derives and
/// the named fields
pub(crate) fn parse_named_fields<'a>(
    input: &'a DeriveInput,
    current_derive: &str,
) -> &'a FieldsNamed {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named_fields) => named_fields,
            _ => panic!(
                "derive({}) works only for structs with named fields. Tuples don't need derive.",
                current_derive
            ),
        },
        _ => panic!("derive({}) works only on structs!", current_derive),
    }
}
