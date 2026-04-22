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

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_base::base::BuildInfoRef;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_io::prelude::InputFormatSettings;
use databend_common_io::prelude::OutputFormatSettings;
use databend_common_pipeline::core::PlanProfile;

use crate::merge_into_join::MergeIntoJoin;
use crate::query_kind::QueryKind;

pub trait TableContextLicense: Send + Sync {
    fn get_license_key(&self) -> String;
}

pub trait TableContextMergeInto: Send + Sync {
    fn set_merge_into_join(&self, join: MergeIntoJoin);

    fn get_merge_into_join(&self) -> MergeIntoJoin;
}

pub trait TableContextQueryIdentity: Send + Sync {
    fn get_id(&self) -> String;

    fn attach_query_str(&self, kind: QueryKind, query: String);

    fn attach_query_hash(&self, text_hash: String, parameterized_hash: String);

    fn get_query_str(&self) -> String;

    fn get_query_parameterized_hash(&self) -> String;

    fn get_query_text_hash(&self) -> String;

    fn get_last_query_id(&self, index: i32) -> Option<String>;

    fn get_query_id_history(&self) -> HashSet<String>;
}

pub trait TableContextQueryInfo: Send + Sync {
    fn get_fuse_version(&self) -> String;

    fn get_version(&self) -> BuildInfoRef;

    fn get_input_format_settings(&self) -> Result<InputFormatSettings>;

    fn get_output_format_settings(&self) -> Result<OutputFormatSettings>;

    fn get_query_kind(&self) -> QueryKind;
}

pub trait TableContextQueryProfile: Send + Sync {
    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>>;

    fn add_query_profiles(&self, profiles: &HashMap<u32, PlanProfile>);

    fn get_query_profiles(&self) -> Vec<PlanProfile>;
}

pub trait TableContextResultCache: Send + Sync {
    fn get_result_cache_key(&self, query_id: &str) -> Option<String>;

    fn set_query_id_result_cache(&self, query_id: String, result_cache_key: String);
}

pub trait TableContextVariables: Send + Sync {
    fn set_variable(&self, key: String, value: Scalar);

    fn unset_variable(&self, key: &str);

    fn get_variable(&self, key: &str) -> Option<Scalar>;

    fn get_all_variables(&self) -> HashMap<String, Scalar>;
}
