// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;

use crate::sql::optimizer::SExpr;

mod factory;
mod rule_implement_get;
mod rule_implement_project;
mod rule_set;
mod transform_state;

pub use factory::RuleFactory;
pub use rule_set::RuleSet;
pub use transform_state::TransformState;

pub type RulePtr = Box<dyn Rule>;

pub trait Rule {
    fn id(&self) -> RuleID;

    fn apply(&self, expression: &SExpr, state: &mut TransformState) -> Result<()>;

    fn pattern(&self) -> &SExpr;
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum RuleID {
    ImplementGet,
    ImplementProject,
}

impl RuleID {
    pub fn name(&self) -> &'static str {
        match self {
            RuleID::ImplementGet => "ImplementGet",
            RuleID::ImplementProject => "ImplementProject",
        }
    }

    /// Unique integral id
    /// TODO: maybe use a macro like https://docs.rs/iota/0.2.2/iota/ to implement this?
    #[allow(dead_code)]
    pub fn uid(&self) -> u32 {
        match self {
            RuleID::ImplementGet => 0,
            RuleID::ImplementProject => 1,
        }
    }
}
