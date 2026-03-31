// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use iceberg::{Error, ErrorKind, NamespaceIdent, Result, TableIdent};

/// Format an sqlx error into iceberg error.
pub fn from_sqlx_error(error: sqlx::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "operation failed for hitting sqlx error".to_string(),
    )
    .with_source(error)
}

pub fn no_such_namespace_err<T>(namespace: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("No such namespace: {namespace:?}"),
    ))
}

pub fn no_such_table_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("No such table: {table_ident:?}"),
    ))
}

pub fn table_already_exists_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("Table {table_ident:?} already exists."),
    ))
}
