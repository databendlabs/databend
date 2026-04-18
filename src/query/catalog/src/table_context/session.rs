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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_settings::Settings;
use databend_storages_common_session::SessionState;
use databend_storages_common_session::TxnManagerRef;

use crate::session_type::SessionType;

pub trait TableContextSession: Send + Sync {
    fn get_connection_id(&self) -> String;

    fn get_current_session_id(&self) -> String {
        unimplemented!()
    }

    fn get_current_client_session_id(&self) -> Option<String> {
        unimplemented!()
    }

    fn txn_mgr(&self) -> TxnManagerRef;

    fn session_state(&self) -> Result<SessionState>;

    fn get_session_type(&self) -> SessionType {
        unimplemented!()
    }

    fn get_function_context(&self) -> Result<FunctionContext>;

    fn get_settings(&self) -> Arc<Settings>;

    fn get_session_settings(&self) -> Arc<Settings>;

    fn get_shared_settings(&self) -> Arc<Settings>;

    fn get_license_key(&self) -> String;
}
