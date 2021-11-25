// Copyright 2021 Datafuse Labs.
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

mod block_to_json;

#[cfg(test)]
mod block_to_json_test;
pub mod http_query_handlers;
#[cfg(test)]
mod http_query_handlers_test;
pub(crate) mod query;
pub mod statement;
#[cfg(test)]
mod statement_test;

pub(super) use http_query_handlers::query_route;
pub(super) use statement::statement_router;
