// Copyright 2023 Datafuse Labs.
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

use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::EvalScalar;
use crate::plans::ScalarItem;
use crate::plans::Window;
use crate::plans::WindowFuncFrame;
use crate::Binder;

impl Binder {
    pub(super) async fn bind_window_function(
        &mut self,
        window_info: &WindowFunctionInto,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut scalar_items: Vec<ScalarItem> = Vec::with_capacity(
            window_info.aggregate_arguments.len()
                + window_info.partition_by_items.len()
                + window_info.order_by_items.len(),
        );
        for arg in window_info.aggregate_arguments.iter() {
            scalar_items.push(arg.clone());
        }
        for part in window_info.partition_by_items.iter() {
            scalar_items.push(part.clone());
        }
        for order in window_info.order_by_items.iter() {
            scalar_items.push(order.order_by_item.clone())
        }

        let mut new_expr = child;
        if !scalar_items.is_empty() {
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(eval_scalar.into(), new_expr);
        }

        let window_plan = Window {
            aggregate_function: window_info.aggregate_function.clone(),
            partition_by: window_info.partition_by_items.clone(),
            order_by: window_info.order_by_items.clone(),
            frame: window_info.frame.clone(),
        };
        new_expr = SExpr::create_unary(window_plan.into(), new_expr);

        Ok(new_expr)
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct WindowInfo {
    pub window_functions: Vec<WindowFunctionInto>,
    pub window_functions_map: HashMap<String, usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WindowFunctionInto {
    pub aggregate_function: ScalarItem,
    pub aggregate_arguments: Vec<ScalarItem>,
    pub partition_by_items: Vec<ScalarItem>,
    pub order_by_items: Vec<WindowOrderByInfo>,
    pub frame: WindowFuncFrame,
}

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct WindowOrderByInfo {
    pub order_by_item: ScalarItem,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
}
