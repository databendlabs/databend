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

use crate::optimizer::SExpr;
use crate::plans::ScalarExpr;
use crate::BindContext;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;

#[derive(Clone, Debug)]
pub struct SubqueryDesc {
    pub input_expr: SExpr,
    pub outer_columns: ColumnSet,
    pub predicate_columns: ColumnSet,
    pub index: IndexType,
    pub predicate: ScalarExpr,
}

#[derive(Clone, Debug)]
pub struct DeletePlan {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,
    pub metadata: MetadataRef,
    pub bind_context: Box<BindContext>,
    pub selection: Option<ScalarExpr>,
    pub subquery_desc: Option<SubqueryDesc>,
}
