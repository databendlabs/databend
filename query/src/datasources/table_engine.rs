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

use crate::catalogs::Table;
use crate::catalogs::TableInfo;
use crate::common::StoreApiProvider;

// TODO maybe we should introduce a
// `Session::store_provider(...) -> Result<StoreApiProvider>`
// such that, we no longer need to pass store_provider to Table's constructor
// instead, table could access apis on demand in method read_plan  and read
pub trait TableEngine: Send + Sync {
    fn try_create(
        &self,
        tbl_info: TableInfo,
        store_provider: StoreApiProvider,
    ) -> common_exception::Result<Box<dyn Table>>;
}

impl<T> TableEngine for T
where
    T: Fn(TableInfo) -> common_exception::Result<Box<dyn Table>>,
    T: Send + Sync,
{
    fn try_create(
        &self,
        tbl_info: TableInfo,
        _store_provider: StoreApiProvider,
    ) -> common_exception::Result<Box<dyn Table>> {
        self(tbl_info)
    }
}
