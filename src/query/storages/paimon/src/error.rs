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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub(crate) fn read_only(operation: &str) -> ErrorCode {
    ErrorCode::Unimplemented(format!("Paimon catalog is read-only: {operation}"))
}

pub(crate) fn map_paimon_error(err: paimon::Error) -> ErrorCode {
    match err {
        paimon::Error::DatabaseNotExist { database } => {
            ErrorCode::UnknownDatabase(format!("Paimon database '{database}' does not exist"))
        }
        paimon::Error::TableNotExist { full_name } => {
            ErrorCode::UnknownTable(format!("Paimon table '{full_name}' does not exist"))
        }
        paimon::Error::ConfigInvalid { message } => ErrorCode::BadArguments(message),
        other => ErrorCode::ReadTableDataError(format!("{other:?}")),
    }
}

pub(crate) fn map_paimon_result<T>(result: paimon::Result<T>) -> Result<T> {
    result.map_err(map_paimon_error)
}
