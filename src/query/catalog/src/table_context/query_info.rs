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

use databend_common_base::base::BuildInfoRef;
use databend_common_exception::Result;
use databend_common_io::prelude::InputFormatSettings;
use databend_common_io::prelude::OutputFormatSettings;

use crate::query_kind::QueryKind;

pub trait TableContextQueryInfo: Send + Sync {
    fn get_fuse_version(&self) -> String;

    fn get_version(&self) -> BuildInfoRef;

    fn get_input_format_settings(&self) -> Result<InputFormatSettings>;

    fn get_output_format_settings(&self) -> Result<OutputFormatSettings>;

    /// Get the kind of session running query.
    fn get_query_kind(&self) -> QueryKind;
}
