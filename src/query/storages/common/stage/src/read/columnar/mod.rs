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

pub mod arrow_to_variant;
mod internal_columns;
mod projection;

pub use arrow_to_variant::read_record_batch_to_variant_column;
pub use arrow_to_variant::record_batch_to_variant_block;
pub use internal_columns::add_internal_columns;
pub use projection::project_columnar;
