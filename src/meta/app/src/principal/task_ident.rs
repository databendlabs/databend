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

pub type TaskIdent = TIdent<Resource>;

pub type TaskIdentRaw = TIdent<Resource>;

pub use kvapi_impl::Resource;

mod kvapi_impl {
    use databend_meta_kvapi::kvapi;

    use crate::principal::task::Task;
    use crate::principal::task_ident::TaskIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_tasks";
        const TYPE: &'static str = "TaskIdent";
        const HAS_TENANT: bool = true;
        type ValueType = Task;
    }

    impl kvapi::Value for Task {
        type KeyType = TaskIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
