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

mod row_access_policy_id_ident;
pub mod row_access_policy_id_to_name_ident;
pub mod row_access_policy_name_ident;
pub mod row_access_policy_table_id_ident;

use chrono::DateTime;
use chrono::Utc;
pub use row_access_policy_id_ident::RowAccessPolicyId;
pub use row_access_policy_id_ident::RowAccessPolicyIdIdent;
pub use row_access_policy_id_to_name_ident::RowAccessPolicyIdToNameIdent;
pub use row_access_policy_id_to_name_ident::RowAccessPolicyIdToNameIdentRaw;
pub use row_access_policy_name_ident::RowAccessPolicyNameIdent;
pub use row_access_policy_name_ident::RowAccessPolicyNameIdentRaw;
pub use row_access_policy_table_id_ident::RowAccessPolicyTableIdIdent;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowAccessPolicyMeta {
    // Vec<(arg_name, arg_type)>
    pub args: Vec<(String, String)>,
    pub body: String,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
    pub update_on: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRowAccessPolicyReq {
    pub name: RowAccessPolicyNameIdent,
    pub row_access_policy_meta: RowAccessPolicyMeta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRowAccessPolicyReply {
    pub id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropRowAccessPolicyReq {
    pub name: RowAccessPolicyNameIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetRowAccessPolicyReq {
    pub name: RowAccessPolicyNameIdent,
}

#[derive(Clone, Debug, Eq, Default, PartialEq)]
pub struct RowAccessPolicyTableId;
