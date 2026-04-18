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

use databend_common_base::base::WatchNotify;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;

use crate::table_context::AbortChecker;
use crate::table_context::CheckAbort;
use crate::table_context::ContextError;

pub trait TableContextQueryState: Send + Sync {
    fn get_abort_checker(self: Arc<Self>) -> AbortChecker
    where Self: 'static {
        struct Checker<S> {
            this: S,
        }

        impl<S: TableContextQueryState + ?Sized> CheckAbort for Checker<Arc<S>> {
            fn try_check_aborting(&self) -> Result<()> {
                self.this.check_aborting().with_context(|| "query aborted")
            }
        }

        Arc::new(Checker { this: self })
    }

    fn check_aborting(&self) -> Result<(), ContextError>;

    fn get_abort_notify(&self) -> Arc<WatchNotify>;

    fn get_error(&self) -> Option<ErrorCode<ContextError>>;

    fn push_warning(&self, warning: String);
}
