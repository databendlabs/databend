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

use std::fmt::Display;

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::DatabaseAlreadyExists;
use databend_common_meta_app::app_error::TableAlreadyExists;
use databend_common_meta_app::app_error::UnknownDatabase;
use databend_common_meta_app::app_error::UnknownDatabaseId;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;

use super::app_error::KVAppError;

pub fn unknown_database_error(db_name_ident: &DatabaseNameIdent, msg: impl Display) -> AppError {
    let e = UnknownDatabase::new(
        db_name_ident.database_name(),
        format!("{}: {}", msg, db_name_ident.display()),
    );
    AppError::UnknownDatabase(e)
}

pub fn db_has_to_exist(
    seq: u64,
    db_name_ident: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Err(KVAppError::AppError(unknown_database_error(
            db_name_ident,
            msg,
        )))
    } else {
        Ok(())
    }
}

pub fn db_id_has_to_exist(seq: u64, db_id: u64, msg: impl Display) -> Result<(), KVAppError> {
    if seq == 0 {
        let app_err = AppError::UnknownDatabaseId(UnknownDatabaseId::new(
            db_id,
            format!("{}: {}", msg, db_id),
        ));
        Err(KVAppError::AppError(app_err))
    } else {
        Ok(())
    }
}

pub fn assert_table_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), AppError> {
    if seq > 0 {
        return Ok(());
    }
    Err(AppError::UnknownTable(UnknownTable::new(
        &name_ident.table_name,
        format!("{}: {}", ctx, name_ident),
    )))
}

pub fn db_has_to_not_exist(
    seq: u64,
    name_ident: &DatabaseNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Ok(())
    } else {
        Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
            DatabaseAlreadyExists::new(
                name_ident.database_name(),
                format!("{}: {}", ctx, name_ident.display()),
            ),
        )))
    }
}

pub fn table_has_to_not_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Ok(())
    } else {
        Err(KVAppError::AppError(AppError::TableAlreadyExists(
            TableAlreadyExists::new(&name_ident.table_name, format!("{}: {}", ctx, name_ident)),
        )))
    }
}
