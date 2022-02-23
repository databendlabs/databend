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

// TODO
// simplify those mods
// Simple delete statement only
// - no USING
// - no sub-queries in selection

mod delete_normalizer;
mod query_delete_analyzer;

pub use delete_normalizer::DeleteNormalizer;
pub use query_delete_analyzer::DeleteSchemaAnalyzer;
