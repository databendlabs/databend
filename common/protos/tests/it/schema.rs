// Copyright 2022 Datafuse Labs.
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

use common_protos::protobuf;

#[test]
fn test_schema() {
    let _ = protobuf::Database {
        tenant: "".to_string(),
        id: 0,
        name: "".to_string(),
        options: Default::default(),
        engine: "".to_string(),
        engine_options: Default::default(),
        created_on: "".to_string(),
        updated_on: "".to_string(),
        comment: "".to_string(),
    };

    let col = protobuf::Column {
        name: "".to_string(),
        data_type: 0,
        default_expr: "".to_string(),
        nullable: false,
    };

    let _ = protobuf::Table {
        tenant: "".to_string(),
        id: 0,
        name: "".to_string(),
        database_id: 0,
        options: Default::default(),
        engine: "".to_string(),
        engine_options: Default::default(),
        columns: vec![col],
        cluster_by_expr: "".to_string(),
        created_on: "".to_string(),
        updated_on: "".to_string(),
        comment: "".to_string(),
    };
}
