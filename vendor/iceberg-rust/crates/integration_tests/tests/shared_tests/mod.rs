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

use std::collections::HashMap;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent};
use iceberg_catalog_rest::RestCatalogBuilder;

use crate::get_shared_containers;

mod append_data_file_test;
mod append_partition_data_file_test;
mod conflict_commit_test;
mod datafusion;
mod read_evolved_schema;
mod read_positional_deletes;
mod scan_all_type;

pub async fn random_ns() -> Namespace {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs([uuid::Uuid::new_v4().to_string()]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    rest_catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    ns
}

fn test_schema() -> Schema {
    Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap()
}
