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

use poem::get;
use poem::Route;

pub fn catalog_route() -> Route {
    let mut route = Route::new()
        .at(
            "/databases",
            get(super::list_databases::list_databases_handler),
        )
        .at(
            "/databases/:database/tables",
            get(super::list_database_tables::list_database_tables_handler),
        )
        .at(
            "/databases/:database/tables/:table",
            get(super::get_database_table::get_database_table_handler),
        )
        .at(
            "/databases/:database/tables/:table/fields",
            get(super::list_database_table_fields::list_database_table_fields_handler),
        )
        .at(
            "/tables/search",
            get(super::search_tables::search_tables_handler),
        );

    route
}
