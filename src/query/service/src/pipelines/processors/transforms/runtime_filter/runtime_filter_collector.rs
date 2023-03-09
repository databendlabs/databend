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
use common_expression::type_check::check;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::Literal;
use common_expression::RawExpr;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_sql::plans::ConstantExpr;
use common_sql::plans::FunctionCall;
use common_sql::ScalarExpr;
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
    fn construct_filters(
        &self,
        target_exprs: &BTreeMap<RuntimeFilterId, RawExpr<String>>,
    ) -> Result<()> {
        if self.total_count.load(Ordering::Relaxed) < VALUE_SET_THRESH_HOLD {
            return self.in_expr(target_exprs);
        }
        self.min_max(target_exprs)
    }

    // Construct InExpr if total_count <= VALUE_SET_THRESH_HOLD
    fn in_expr(&self, target_exprs: &BTreeMap<RuntimeFilterId, RawExpr<String>>) -> Result<()> {
        let mut filter_exprs = self.filter_exprs.write();
        let value_sets = self.value_sets.read();
        for (id, value_set) in value_sets.iter() {
            if let Some(target_expr) = target_exprs.get(id) {
                let mut args = vec![];
                if !value_set.is_empty() {
                    let mut array_args = vec![];
                    for val in value_set {
                        array_args.push(ScalarExpr::ConstantExpr (ConstantExpr{
                            value: Literal::try_from(val.clone())?,
                            data_type: Box::new(val.as_ref().infer_data_type()),
                        }))
                    }
                    // Construct `array` function
                    let array_func = ScalarExpr::FunctionCall(FunctionCall {
                        params: vec![],
                        arguments: array_args,
                        func_name: "array".to_string(),
                        return_type: Box::new(DataType::Array(Box::new(value_set[0].as_ref().infer_data_type()))),
                    });
                    // Let `array_func` as arg of `contain` function
                    args.push(array_func.as_raw_expr_with_col_name())
                }
                args.push(target_expr.clone());
                // Construct `contain` function
                let contains_function = RawExpr::FunctionCall {
                    span: None,
                    name: "contains".to_string(),
                    params: vec![],
                    args,
                };
                let expr = check(&contains_function, &BUILTIN_FUNCTIONS)?;
                filter_exprs.insert(id.clone(), expr.as_remote_expr());
            }
        }
        Ok(())
    }

    // Construct min-max Expr
    fn min_max(&self, target_exprs: &BTreeMap<RuntimeFilterId, RawExpr<String>>) -> Result<()> {
        Ok(())
    }

    fn clear(&self) {
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

    fn consume(&self, target_exprs: &BTreeMap<RuntimeFilterId, RawExpr<String>>) -> Result<()> {
        self.construct_filters(target_exprs)?;
        self.clear();
        Ok(())
    }

    fn get_filters(&self) -> Result<HashMap<RuntimeFilterId, RemoteExpr<String>>> {
        let filters = self.filter_exprs.read();
        return  Ok((*filters).clone())
    }
}

impl Default for RuntimeFilterCollector {
    fn default() -> Self {
        Self::new()
    }
}
