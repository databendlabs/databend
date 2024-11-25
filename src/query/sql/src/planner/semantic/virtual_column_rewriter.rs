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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::RelOperator;
use crate::IndexType;

pub(crate) struct VirtualColumnRewriter {
    /// Mapping: (table index) -> (derived virtual column indices)
    /// This is used to add virtual column indices to Scan plan
    virtual_column_indices: HashMap<IndexType, Vec<IndexType>>,
}

impl VirtualColumnRewriter {
    pub(crate) fn new(virtual_column_indices: HashMap<IndexType, Vec<IndexType>>) -> Self {
        Self {
            virtual_column_indices,
        }
    }

    // Add the indices of the virtual columns to the Scan plan of the table
    // to read the virtual columns at the storage layer.
    #[recursive::recursive]
    pub(crate) fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();

        if let RelOperator::Scan(mut scan) = (*s_expr.plan).clone() {
            if let Some(indices) = self.virtual_column_indices.remove(&scan.table_index) {
                for index in indices {
                    scan.columns.insert(index);
                }
                s_expr.plan = Arc::new(scan.into());
            }
        }

        if !s_expr.children.is_empty() {
            let mut children = Vec::with_capacity(s_expr.children.len());
            for child in s_expr.children.iter() {
                children.push(Arc::new(self.rewrite(child)?));
            }
            s_expr.children = children;
        }

        Ok(s_expr)
    }
}
