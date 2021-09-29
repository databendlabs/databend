//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_metatypes::MetaId;
use common_planners::Expression;

use crate::catalogs::TableFunction;

pub type TableArgs = Option<Vec<Expression>>;

pub trait TableFuncEngine: Send + Sync {
    fn try_create(
        &self,
        db_name: &str,
        tbl_func_name: &str,
        tbl_id: MetaId,
        arg: TableArgs,
    ) -> common_exception::Result<Arc<dyn TableFunction>>;
}

impl<T> TableFuncEngine for T
where
    T: Fn(&str, &str, MetaId, TableArgs) -> common_exception::Result<Arc<dyn TableFunction>>,
    T: Send + Sync,
{
    fn try_create(
        &self,
        db_name: &str,
        tbl_func_name: &str,
        tbl_id: MetaId,
        arg: TableArgs,
    ) -> common_exception::Result<Arc<dyn TableFunction>> {
        self(db_name, tbl_func_name, tbl_id, arg)
    }
}
