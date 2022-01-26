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

pub mod block_to_json;
mod http_query_handlers;
mod load;
pub mod middleware;
mod query;
mod statement;

pub(crate) use block_to_json::block_to_json;
pub(crate) use block_to_json::JsonBlock;
pub(crate) use block_to_json::JsonBlockRef;
pub use http_query_handlers::make_final_uri;
pub use http_query_handlers::make_page_uri;
pub use http_query_handlers::make_state_uri;
pub use http_query_handlers::query_route;
pub use http_query_handlers::QueryResponse;
pub use http_query_handlers::QueryStats;
pub use load::streaming_load;
pub use load::LoadResponse;
pub use query::ExecuteStateName;
pub use query::HttpQueryHandle;
pub use query::HttpQueryManager;
pub use statement::statement_handler;
pub use statement::statement_router;
