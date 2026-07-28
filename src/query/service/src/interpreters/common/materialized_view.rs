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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::is_materialized_view_engine;

pub fn check_not_materialized_view(table: &dyn Table, db_name: &str) -> Result<()> {
    if is_materialized_view_engine(table.engine()) {
        return Err(ErrorCode::TableEngineNotSupported(format!(
            "Cannot modify materialized view `{}`.`{}`",
            db_name,
            table.name()
        )));
    }
    Ok(())
}
