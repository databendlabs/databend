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

use databend_common_ast::ast::SetType;
use databend_common_expression::Scalar;

use super::Plan;

#[derive(Clone, Debug, PartialEq)]
pub struct SettingValue {
    pub is_global: bool,
    pub variable: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VarValue {
    pub variable: String,
    pub value: Scalar,
}

#[derive(Clone, Debug)]
pub enum SetScalarsOrQuery {
    VarValue(Vec<Scalar>),
    Query(Box<Plan>),
}

#[derive(Clone, Debug)]
pub struct SetPlan {
    pub set_type: SetType,
    pub idents: Vec<String>,
    pub values: SetScalarsOrQuery,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnsetPlan {
    pub unset_type: SetType,
    pub vars: Vec<String>,
}
