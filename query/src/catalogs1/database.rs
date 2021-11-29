// Copyright 2021 Datafuse Labs.
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

use dyn_clone::DynClone;

#[async_trait::async_trait]
pub trait Database: DynClone + Sync + Send {
    /// Database name.
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct DefaultDatabase {
    db_name: String,
}

impl DefaultDatabase {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }
}

impl Database for DefaultDatabase {
    fn name(&self) -> &str {
        &self.db_name
    }
}
