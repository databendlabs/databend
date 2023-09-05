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

use common_expression::TableSchemaRef;
use rand::Rng;

#[derive(Clone, Debug)]
pub(crate) struct Table {
    pub name: String,
    pub schema: TableSchemaRef,
}

impl Table {
    pub fn new(name: String, schema: TableSchemaRef) -> Self {
        Self { name, schema }
    }
}

pub(crate) struct SqlGenerator<'a, R: Rng> {
    pub(crate) tables: Vec<Table>,
    pub(crate) rng: &'a mut R,
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn new(rng: &'a mut R, tables: Vec<Table>) -> Self {
        SqlGenerator { tables, rng }
    }
}
