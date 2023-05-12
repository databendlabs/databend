use std::collections::HashMap;
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
use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::Expr;
use crate::ast::TypeName;

pub struct DataMaskArg {
    pub arg_name: String,
    pub arg_type: TypeName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataMaskPolicy {
    pub args: HashMap<String, TypeName>,
    pub return_type: TypeName,
    pub body: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatamaskPolicyStmt {
    pub create: bool,
    pub name: String,
    pub policy: DataMaskPolicy,
}

impl Display for CreateDatamaskPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        // write!(f, "DROP SHARE ENDPOINT {}", self.endpoint)?;

        Ok(())
    }
}
