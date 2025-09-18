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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub enum DropContext {
    /// Normal drop operation without ownership cleanup
    #[default]
    Normal,
    /// Drop during CREATE OR REPLACE operation, requires ownership cleanup
    Replace { catalog_name: String },
}
