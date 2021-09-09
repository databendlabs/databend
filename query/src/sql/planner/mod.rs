// Copyright 2020 Datafuse Labs.
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

mod binder;
mod expression_binder;
mod logical_plan;
pub mod physical_plan;
mod bind_context;

pub use binder::Binder;
pub use binder::ColumnBinding;
pub use binder::TableBinding;
pub use logical_plan::LogicalEquiJoin;
pub use logical_plan::LogicalPlan;
pub use physical_plan::PhysicalPlan;
pub use binder::BindContext;
use common_exception::Result;
use common_exception::ErrorCode;
use crate::sql::{TableScan, PhysicalProjection};

pub type IndexType = usize;

pub fn optimize(logical: LogicalPlan) -> Result<PhysicalPlan> {
    match logical {
        LogicalPlan::Get(get) => {
            Ok(PhysicalPlan::TableScan(TableScan {
                get_plan: get,
            }))
        }
        LogicalPlan::Projection(projection) => {
            Ok((PhysicalPlan::Projection(PhysicalProjection {
                projection_plan: projection.clone(),
                child: Box::new(optimize(*projection.child)?),
            })))
        }
        _ => Err(ErrorCode::LogicalError("Unsupported"))
    }
}

#[cfg(test)]
mod test {
    use crate::sql::parser::Parser;
    use crate::sql::planner::{Binder, optimize};
    use crate::datasources::DatabaseCatalog;
    use crate::sessions::DatafuseQueryContext;
    use crate::tests::try_create_context;

    #[test]
    fn run() {
        let parser = Parser {};

        let stmts = parser.parse_with_sqlparser("select * from system.tables").unwrap();
        let catalog = DatabaseCatalog::try_create().unwrap();
        for stmt in stmts {
            let binder = Binder::new(&stmt, &catalog, try_create_context().unwrap());
            let res = binder.bind().unwrap();
            let physical = optimize(res.plan).unwrap();
            println!("{:?}", physical);
        }
    }
}