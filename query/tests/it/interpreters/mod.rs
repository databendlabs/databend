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

mod interpreter_database_create;
mod interpreter_database_drop;
mod interpreter_describe_stage;
mod interpreter_describe_table;
mod interpreter_explain;
mod interpreter_grant_privilege;
mod interpreter_insert;
mod interpreter_interceptor;
mod interpreter_revoke_previlege;
mod interpreter_select;
mod interpreter_setting;
mod interpreter_show_create_database;
mod interpreter_show_create_table;
mod interpreter_stage_create;
mod interpreter_stage_drop;
mod interpreter_table_create;
mod interpreter_table_drop;
mod interpreter_table_optimize;
mod interpreter_table_truncate;
mod interpreter_udf_alter;
mod interpreter_udf_create;
mod interpreter_udf_drop;
mod interpreter_udf_show;
mod interpreter_use_database;
mod interpreter_user_alter;
mod interpreter_user_create;
mod interpreter_user_drop;
mod plan_schedulers;
