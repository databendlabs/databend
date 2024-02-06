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

use databend_common_exception::Result;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

#[async_trait::async_trait]
pub trait UdfApi: Sync + Send {
    // Add a UDF to /tenant/udf-name.
    async fn add_udf(&self, udf: UserDefinedFunction, create_option: &CreateOption) -> Result<()>;

    // Update a UDF to /tenant/udf-name.
    async fn update_udf(&self, udf: UserDefinedFunction, seq: MatchSeq) -> Result<u64>;

    // Get UDF by name.
    async fn get_udf(&self, udf_name: &str) -> Result<SeqV<UserDefinedFunction>>;

    // Get all the UDFs for a tenant.
    async fn get_udfs(&self) -> Result<Vec<UserDefinedFunction>>;

    // Drop the tenant's UDF by name.
    async fn drop_udf(&self, udf_name: &str, seq: MatchSeq) -> Result<()>;
}
