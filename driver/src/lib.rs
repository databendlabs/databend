// Copyright 2023 Datafuse Labs.
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
mod error;
#[cfg(feature = "flight-sql")]
mod flight_sql;
mod rest_api;
mod rows;
mod schema;
mod value;

pub use conn::{new_connection, Connection};
pub use rows::{QueryProgress, Row, RowIterator, RowProgressIterator, RowWithProgress};
pub use schema::{DataType, DecimalSize, Schema, SchemaRef};
pub use value::{NumberValue, Value};
