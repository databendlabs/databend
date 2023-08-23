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

mod conn;
#[cfg(feature = "flight-sql")]
mod flight_sql;
mod rest_api;

pub use conn::{Client, Connection, ConnectionInfo};

// pub use for convenience
pub use databend_sql::error::{Error, Result};
pub use databend_sql::rows::{
    QueryProgress, Row, RowIterator, RowProgressIterator, RowWithProgress,
};
pub use databend_sql::schema::{DataType, DecimalSize, Field, Schema, SchemaRef};
pub use databend_sql::value::{NumberValue, Value};

pub use databend_driver_macros::TryFromRow;

#[doc(hidden)]
pub mod _macro_internal {
    pub use databend_sql::_macro_internal::*;
}
