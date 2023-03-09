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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use common_catalog::plan::RuntimeFilterId;
use common_catalog::table_context::RuntimeFilter;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use itertools::sorted;
use parking_lot::RwLock;

const VALUE_SET_THRESH_HOLD: usize = 1024;

pub struct RuntimeFilterCollector {
    // For each runtime filter id, it'll have a filter expr used by pruning during scan table.
    pub filter_exprs: RwLock<HashMap<RuntimeFilterId, RemoteExpr<String>>>,
    pub min_value: RwLock<HashMap<RuntimeFilterId, Scalar>>,
    pub max_value: RwLock<HashMap<RuntimeFilterId, Scalar>>,
    pub value_sets: RwLock<HashMap<RuntimeFilterId, Vec<Scalar>>>,
    pub total_count: AtomicUsize,
}

impl RuntimeFilterCollector {
    pub fn new() -> Self {
        Self {
            filter_exprs: Default::default(),
            min_value: RwLock::new(HashMap::new()),
            max_value: RwLock::new(HashMap::new()),
            value_sets: Default::default(),
            total_count: Default::default(),
        }
    }

    // Construct runtime filters by min/max values
    fn construct_filters(&self) -> Result<()> {
        if self.total_count.load(Ordering::Relaxed) < VALUE_SET_THRESH_HOLD {
            return self.in_expr();
        }
        self.min_max()
    }

    // Construct InExpr if total_count <= VALUE_SET_THRESH_HOLD
    fn in_expr(&self) -> Result<()> {
        Ok(())
    }

    // Construct min-max Expr
    fn min_max(&self) -> Result<()> {
        Ok(())
    }

    fn clear(&self) {
        let mut filter_exprs = self.filter_exprs.write();
        filter_exprs.clear();
        self.total_count.store(0, Ordering::Relaxed);
    }
}

impl RuntimeFilter for RuntimeFilterCollector {
    fn collect(
        &self,
        source_exprs: &BTreeMap<RuntimeFilterId, RemoteExpr>,
        data: &DataBlock,
    ) -> Result<()> {
        for (id, remote_expr) in source_exprs.iter() {
            let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
            // expr represents equi condition in join build side
            // Such as: `select * from t1 inner join t2 on t1.a = t2.a + 1`
            // expr is `t2.a + 1`
            // First we need get expected values from data by expr
            let evaluator = Evaluator::new(data, FunctionContext::default(), &BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), data.num_rows());
            let scalars = column
                .iter()
                .map(|scalar| scalar.to_owned())
                .collect::<Vec<Scalar>>();
            let mut value_sets = self.value_sets.write();
            if let Some(value_set) = value_sets.get_mut(id) {
                if value_set.len() + column.len() < VALUE_SET_THRESH_HOLD {
                    value_set.extend(scalars);
                }
            } else if column.len() < VALUE_SET_THRESH_HOLD {
                value_sets.insert(id.clone(), scalars);
            }
            let sorted_column = sorted(column.iter());
            if column.len() == 0 {
                return Ok(());
            }
            {
                let min = sorted_column.clone().min().unwrap();
                let mut min_value = self.min_value.write();
                if let Some(val) = min_value.get_mut(id) {
                    *val = min.min(val.as_ref()).to_owned().clone();
                } else {
                    min_value.insert(id.clone(), min.to_owned());
                }
            }
            {
                let max = sorted_column.max().unwrap();
                let mut max_value = self.max_value.write();
                if let Some(val) = max_value.get_mut(id) {
                    *val = max.max(val.as_ref()).to_owned().clone();
                } else {
                    max_value.insert(id.clone(), max.to_owned());
                }
            }
        }
        self.total_count
            .fetch_add(data.num_rows(), Ordering::Relaxed);
        Ok(())
    }

    fn consume(&self) -> Result<()> {
        self.construct_filters()?;
        self.clear();
        Ok(())
    }
}

impl Default for RuntimeFilterCollector {
    fn default() -> Self {
        Self::new()
    }
}
