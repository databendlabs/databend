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

#[derive(Clone, Debug)]
pub struct Query {
    sql: String,
    id: String,
}

#[allow(dead_code)]
impl Query {
    pub fn new(sql: impl AsRef<str>) -> Self {
        Self {
            sql: sql.as_ref().to_string(),
            id: "".to_string(),
        }
    }

    #[must_use]
    pub fn id(self, id: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().to_string(),
            ..self
        }
    }

    pub(crate) fn get_sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn get_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn map_sql<F>(self, f: F) -> Self
    where F: Fn(&str) -> String {
        Self {
            sql: f(&self.sql),
            ..self
        }
    }
}

impl<T> From<T> for Query
where T: AsRef<str>
{
    fn from(source: T) -> Self {
        Self::new(source)
    }
}
