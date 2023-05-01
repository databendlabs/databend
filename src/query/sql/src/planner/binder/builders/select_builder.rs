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

//! some useful utils to create inner SQLs

use core::fmt::Write;

pub struct SelectBuilder {
    from: String,
    columns: Vec<String>,
    filters: Vec<String>,
    order_bys: Vec<String>,
}

impl SelectBuilder {
    pub fn from(table_name: &str) -> SelectBuilder {
        SelectBuilder {
            from: table_name.to_owned(),
            columns: vec![],
            filters: vec![],
            order_bys: vec![],
        }
    }
    pub fn with_column(&mut self, col_name: impl Into<String>) -> &mut Self {
        self.columns.push(col_name.into());
        self
    }

    pub fn with_filter(&mut self, col_name: impl Into<String>) -> &mut Self {
        self.filters.push(col_name.into());
        self
    }

    pub fn with_order_by(&mut self, order_by: &str) -> &mut Self {
        self.order_bys.push(order_by.to_owned());
        self
    }

    pub fn build(self) -> String {
        let mut query = String::new();
        let mut columns = String::new();
        let s = self.columns.join(",");
        if s.is_empty() {
            write!(columns, "*").unwrap();
        } else {
            write!(columns, "{s}").unwrap();
        }

        let mut order_bys = String::new();
        let s = self.order_bys.join(",");
        if s.is_empty() {
            write!(order_bys, "{s}").unwrap();
        } else {
            write!(order_bys, "ORDER BY {s}").unwrap();
        }

        let mut filters = String::new();
        let s = self.filters.join(" and ");
        if !s.is_empty() {
            write!(filters, "where {s}").unwrap();
        } else {
            write!(filters, "").unwrap();
        }

        let from = self.from;
        write!(query, "SELECT {columns} FROM {from} {filters} {order_bys} ").unwrap();
        query
    }
}
