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

mod copy;
mod table;

pub use copy::build_append_data_pipeline;
pub use copy::build_upsert_copied_files_to_meta_req;
pub use copy::CopyPlanType;
pub use table::build_append2table_with_commit_pipeline;
pub use table::build_append2table_without_commit_pipeline;
pub use table::build_fill_missing_columns_pipeline;
