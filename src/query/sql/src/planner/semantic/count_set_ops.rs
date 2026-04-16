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

use databend_common_ast::ast::SetExpr;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::VisitorMut;

#[derive(Debug, Clone, Default)]
pub struct CountSetOps {
    pub count: usize,
}

impl VisitorMut for CountSetOps {
    fn visit_set_expr(&mut self, set_expr: &mut SetExpr) -> Result<VisitControl, !> {
        if matches!(set_expr, SetExpr::SetOperation(_)) {
            self.count += 1;
        }
        Ok(VisitControl::Continue)
    }
}
