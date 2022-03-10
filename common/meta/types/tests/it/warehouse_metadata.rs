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

use common_exception::exception::Result;
use common_meta_types::WarehouseInfo;
use common_meta_types::WarehouseMeta;
use regex::Regex;

#[test]
fn test_warehouse_meta() -> Result<()> {
    let mut actual = WarehouseMeta::default();
    let name = "databend!@EG!EG!U🐸🐸🐸".to_string();
    let size = "large".to_string();
    actual.warehouse_name = name.clone();
    actual.size = size.clone();
    let ser = serde_json::to_string(&actual)?;

    let expect = WarehouseMeta::try_from(ser.into_bytes())?;
    assert_eq!(actual, expect);
    assert_eq!(actual.warehouse_name, name);
    assert_eq!(actual.size, size);
    Ok(())
}

#[test]
fn test_warehouse_info() -> Result<()> {
    let mut actual = WarehouseMeta::default();
    let name = "databend!@EG!EG!U🐸🐸🐸".to_string();
    let size = "large".to_string();
    actual.warehouse_name = name;
    actual.size = size;
    let info = WarehouseInfo {
        meta: actual,
        tenant: "ts1211".to_string(),
        warehouse_id: "w1211".to_string(),
    };
    let ser = serde_json::to_string(&info)?;

    let expect = WarehouseInfo::try_from(ser.into_bytes())?;
    assert_eq!(info, expect);
    Ok(())
}

#[test]
fn test_generate_id() -> Result<()> {
    let id = WarehouseInfo::generate_id();
    assert!(id.len() < 63);
    let regex = Regex::new(r"^(([a-z][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$").unwrap();
    assert!(regex.is_match(&id));
    Ok(())
}
