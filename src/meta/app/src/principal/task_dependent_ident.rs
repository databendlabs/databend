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

pub type TaskDependentIdent = TIdent<TaskDependentResource, TaskDependent>;

pub use kvapi_impl::TaskDependentResource;

use crate::principal::TaskDependent;

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::principal::task::TaskDependent;
    use crate::principal::task_dependent_ident::TaskDependentIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct TaskDependentResource;
    impl TenantResource for TaskDependentResource {
        const PREFIX: &'static str = "__fd_task_dependents";
        const TYPE: &'static str = "TaskDependentIdent";
        const HAS_TENANT: bool = true;
        type ValueType = TaskDependent;
    }

    impl kvapi::Value for TaskDependent {
        type KeyType = TaskDependentIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
