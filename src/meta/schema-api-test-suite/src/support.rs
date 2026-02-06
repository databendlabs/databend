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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::table_niv::TableNIV;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_types::MetaError;
use databend_meta_types::UpsertKV;

macro_rules! assert_meta_eq_without_updated {
    ($a: expr, $b: expr, $msg: expr) => {
        let mut aa = $a.clone();
        aa.meta.updated_on = $b.meta.updated_on;
        assert_eq!(aa, $b, $msg);
    };
}

pub(crate) use assert_meta_eq_without_updated;

#[derive(PartialEq, Default, Debug)]
pub(crate) struct DroponInfo {
    pub name: String,
    pub drop_on_cnt: i32,
    pub non_drop_on_cnt: i32,
}

pub(crate) fn calc_and_compare_drop_on_db_result(
    result: Vec<Arc<DatabaseInfo>>,
    expected: Vec<DroponInfo>,
) {
    let mut expected_map = BTreeMap::new();
    for expected_item in expected {
        expected_map.insert(expected_item.name.clone(), expected_item);
    }

    let mut get = BTreeMap::new();
    for item in result.iter() {
        let name = item.name_ident.to_string_key();
        if !get.contains_key(&name) {
            let info = DroponInfo {
                name: name.clone(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 0,
            };
            get.insert(name.clone(), info);
        };

        let drop_on_info = get.get_mut(&name).unwrap();
        if item.meta.drop_on.is_some() {
            drop_on_info.drop_on_cnt += 1;
        } else {
            drop_on_info.non_drop_on_cnt += 1;
        }
    }

    assert_eq!(get, expected_map);
}

pub(crate) fn calc_and_compare_drop_on_table_result(
    result: Vec<TableNIV>,
    expected: Vec<DroponInfo>,
) {
    let mut expected_map = BTreeMap::new();
    for expected_item in expected {
        expected_map.insert(expected_item.name.clone(), expected_item);
    }

    let mut got = BTreeMap::new();
    for item in result.iter() {
        let table_name = item.name().to_string_key();
        if !got.contains_key(&table_name) {
            let info = DroponInfo {
                name: table_name.clone(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 0,
            };
            got.insert(table_name.clone(), info);
        };

        let drop_on_info = got.get_mut(&table_name).expect("");
        if item.value().drop_on.is_some() {
            drop_on_info.drop_on_cnt += 1;
        } else {
            drop_on_info.non_drop_on_cnt += 1;
        }
    }

    assert_eq!(got, expected_map);
}

pub(crate) async fn upsert_test_data(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &impl kvapi::Key,
    value: Vec<u8>,
) -> Result<u64, KVAppError> {
    let res = kv_api
        .upsert_kv(UpsertKV::update(key.to_string_key(), &value))
        .await?;

    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

pub(crate) async fn delete_test_data(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &impl kvapi::Key,
) -> Result<(), KVAppError> {
    kv_api
        .upsert_kv(UpsertKV::delete(key.to_string_key()))
        .await?;

    Ok(())
}
