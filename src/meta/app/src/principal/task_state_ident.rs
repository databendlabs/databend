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

use crate::tenant_key::ident::TIdent;

pub type TaskSucceededStateIdent = TIdent<Resource, TaskSucceededStateKey>;

impl TaskSucceededStateIdent {
    pub fn new(
        tenant: impl ToTenant,
        before_task: impl ToString,
        after_task: impl ToString,
    ) -> Self {
        TaskSucceededStateIdent::new_generic(
            tenant,
            TaskSucceededStateKey::new(before_task.to_string(), after_task.to_string()),
        )
    }
}

pub use kvapi_impl::Resource;

use crate::principal::task::TaskSucceededStateKey;
use crate::tenant::ToTenant;

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::principal::task_state_ident::TaskSucceededStateIdent;
    use crate::principal::TaskSucceededStateValue;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_task_states";
        const TYPE: &'static str = "TaskSucceededStateIdent";
        const HAS_TENANT: bool = true;
        type ValueType = TaskSucceededStateValue;
    }

    impl kvapi::Value for TaskSucceededStateValue {
        type KeyType = TaskSucceededStateIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
