//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#![allow(clippy::too_many_arguments)]
mod alter_table;
mod analyze;
mod clustering;
mod commit;
mod gc;
mod internal_column;
mod mutation;
mod navigate;
mod optimize;
mod purge_drop;
mod read_plan;
mod replace_into;
mod table_analyze;
mod truncate;
