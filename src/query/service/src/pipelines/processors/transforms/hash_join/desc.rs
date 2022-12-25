// Copyright 2022 Datafuse Labs.
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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::Expr;
use common_expression::RawExpr;
use common_functions::scalars::FunctionFactory;
use common_sql::executor::HashJoin;
use common_sql::executor::PhysicalScalar;
use common_sql::IndexType;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::sql::plans::JoinType;

pub const JOIN_MAX_CHUNK_SIZE: usize = 65535;

#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
pub enum MarkerKind {
    True,
    False,
    Null,
}

pub struct MarkJoinDesc {
    pub(crate) marker_index: Option<IndexType>,
    pub(crate) has_null: RwLock<bool>,
}

pub struct JoinState {
    /// Record rows in build side that are matched with rows in probe side.
    /// It's order-sensitive, aligned with the order of rows in merged chunk.
    pub(crate) build_indexes: RwLock<Vec<RowPtr>>,
    pub(crate) rest_build_indexes: RwLock<Vec<RowPtr>>,
    pub(crate) rest_probe_chunks: RwLock<Vec<Chunk>>,
    pub(crate) validity: RwLock<MutableBitmap>,
}

impl JoinState {
    pub fn create() -> Result<Self> {
        Ok(JoinState {
            build_indexes: RwLock::new(Vec::with_capacity(JOIN_MAX_CHUNK_SIZE)),
            rest_build_indexes: RwLock::new(Vec::with_capacity(JOIN_MAX_CHUNK_SIZE)),
            rest_probe_chunks: RwLock::new(Vec::with_capacity(JOIN_MAX_CHUNK_SIZE)),
            validity: RwLock::new(MutableBitmap::with_capacity(JOIN_MAX_CHUNK_SIZE)),
        })
    }
}

pub struct HashJoinDesc {
    pub(crate) build_keys: Vec<Expr>,
    pub(crate) probe_keys: Vec<Expr>,
    pub(crate) join_type: JoinType,
    pub(crate) other_predicate: Option<Expr>,
    pub(crate) marker_join_desc: MarkJoinDesc,
    /// Whether the Join are derived from correlated subquery.
    pub(crate) from_correlated_subquery: bool,
    pub(crate) join_state: JoinState,
}

impl HashJoinDesc {
    pub fn create(join: &HashJoin) -> Result<HashJoinDesc> {
        let other_predicate = Self::join_predicate(&join.non_equi_conditions)?;

        let build_keys: Result<Vec<Expr>> = join.build_keys.iter().map(|k| k.as_expr()).collect();
        let probe_keys: Result<Vec<Expr>> = join.probe_keys.iter().map(|k| k.as_expr()).collect();

        Ok(HashJoinDesc {
            join_type: join.join_type.clone(),
            build_keys: build_keys?,
            probe_keys: probe_keys?,
            other_predicate,
            marker_join_desc: MarkJoinDesc {
                has_null: RwLock::new(false),
                marker_index: join.marker_index,
            },
            from_correlated_subquery: join.from_correlated_subquery,
            join_state: JoinState::create()?,
        })
    }

    fn join_predicate(non_equi_conditions: &[PhysicalScalar]) -> Result<Option<Expr>> {
        if non_equi_conditions.is_empty() {
            return Ok(None);
        }

        let mut condition = non_equi_conditions[0].clone().as_raw_expr();

        for other_condition in non_equi_conditions.iter().skip(1) {
            condition = RawExpr::FunctionCall {
                span: None,
                name: "and".to_string(),
                params: vec![],
                args: vec![condition, other_condition.as_raw_expr()],
            };
        }

        Ok(Some(condition))
    }
}
