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

use databend_common_meta_app::schema::TableDeduplicationLabelIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::testing::assert_round_trip;

#[test]
fn test_table_deduplication_label_key() {
    let label = "daily/import%";
    let tenant_a = Tenant::new_literal("tenant_a");
    let tenant_b = Tenant::new_literal("tenant/b");

    assert_round_trip(
        TableDeduplicationLabelIdent::new(tenant_a, label),
        "__fd_table_deduplication_label/tenant_a/daily%2fimport%25",
    );
    assert_round_trip(
        TableDeduplicationLabelIdent::new(tenant_b, label),
        "__fd_table_deduplication_label/tenant%2fb/daily%2fimport%25",
    );
}
