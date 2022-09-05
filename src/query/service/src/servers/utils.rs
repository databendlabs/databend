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

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

use common_exception::Result;
use common_settings::Settings;

use crate::interpreters::InterpreterFactoryV2;
use crate::sql::DfHint;
use crate::sql::DfStatement;

pub fn use_planner_v2(
    settings: &Settings,
    stmts: &Result<(Vec<DfStatement>, Vec<DfHint>)>,
) -> Result<bool> {
    match stmts {
        Ok((stmts, _)) => Ok(settings.get_enable_planner_v2()? != 0
            || stmts.get(0).map_or(false, InterpreterFactoryV2::check)),
        Err(e) => {
            if settings.get_enable_planner_v2()? != 0 {
                Ok(true)
            } else {
                Err(e.clone())
            }
        }
    }
}
