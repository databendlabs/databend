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

use common_exception::Result;

use crate::catalogs::Database;
use crate::datasources::context::DataSourceContext;

pub trait DatabaseEngine: Send + Sync {
    fn try_create(&self, db_name: &str, ctx: DataSourceContext) -> Result<Box<dyn Database>>;
}

impl<T> DatabaseEngine for T
where
    T: Fn(&str, DataSourceContext) -> Result<Box<dyn Database>>,
    T: Send + Sync,
{
    fn try_create(&self, db_name: &str, ctx: DataSourceContext) -> Result<Box<dyn Database>> {
        self(db_name, ctx)
    }
}
