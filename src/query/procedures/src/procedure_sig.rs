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

use common_expression::DataSchema;
use common_meta_app::principal::UserOptionFlag;

pub trait ProcedureSignature: Send + Sync {
    fn name(&self) -> &str;

    fn features(&self) -> ProcedureFeatures;

    fn schema(&self) -> Arc<DataSchema>;
}

#[derive(Clone, Default)]
pub struct ProcedureFeatures {
    // The number of arguments the function accepts.
    pub num_arguments: usize,
    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function.
    pub variadic_arguments: Option<(usize, usize)>,

    // Management mode only.
    pub management_mode_required: bool,

    // User option flag required.
    pub user_option_flag: Option<UserOptionFlag>,
}

impl ProcedureFeatures {
    pub fn num_arguments(mut self, num_arguments: usize) -> ProcedureFeatures {
        self.num_arguments = num_arguments;
        self
    }

    pub fn variadic_arguments(mut self, min: usize, max: usize) -> ProcedureFeatures {
        self.variadic_arguments = Some((min, max));
        self
    }

    pub fn management_mode_required(mut self, required: bool) -> ProcedureFeatures {
        self.management_mode_required = required;
        self
    }

    pub fn user_option_flag(mut self, flag: UserOptionFlag) -> ProcedureFeatures {
        self.user_option_flag = Some(flag);
        self
    }
}
