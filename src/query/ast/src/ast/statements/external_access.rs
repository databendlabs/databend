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

use std::fmt;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;
use crate::visit::VisitControl;
use crate::visit::Visitor;
use crate::visit::VisitorMut;
use crate::visit::Walk;
use crate::visit::WalkMut;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum ExternalAccessStmt {
    CreateNetworkRule {
        name: Identifier,
        create_option: CreateOption,
        rule_type: Identifier,
        mode: Identifier,
        values: Vec<String>,
    },
    AlterNetworkRule {
        name: Identifier,
        if_exists: bool,
        values: Vec<String>,
    },
    DropNetworkRule {
        name: Identifier,
        if_exists: bool,
    },
    CreateIntegration {
        name: Identifier,
        create_option: CreateOption,
        rules: Vec<Identifier>,
        enabled: bool,
    },
    AlterIntegration {
        name: Identifier,
        if_exists: bool,
        rules: Option<Vec<Identifier>>,
        enabled: Option<bool>,
    },
    DropIntegration {
        name: Identifier,
        if_exists: bool,
    },
}

impl Walk for ExternalAccessStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        use ExternalAccessStmt::*;
        let name = match self {
            CreateNetworkRule { name, .. }
            | AlterNetworkRule { name, .. }
            | DropNetworkRule { name, .. }
            | CreateIntegration { name, .. }
            | AlterIntegration { name, .. }
            | DropIntegration { name, .. } => name,
        };
        if let VisitControl::Break(value) = name.walk(visitor)? {
            return Ok(VisitControl::Break(value));
        }
        match self {
            CreateIntegration { rules, .. } => rules.walk(visitor),
            AlterIntegration { rules, .. } => rules.walk(visitor),
            _ => Ok(VisitControl::Continue),
        }
    }
}

impl WalkMut for ExternalAccessStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        use ExternalAccessStmt::*;
        let name = match self {
            CreateNetworkRule { name, .. }
            | AlterNetworkRule { name, .. }
            | DropNetworkRule { name, .. }
            | CreateIntegration { name, .. }
            | AlterIntegration { name, .. }
            | DropIntegration { name, .. } => name,
        };
        if let VisitControl::Break(value) = name.walk_mut(visitor)? {
            return Ok(VisitControl::Break(value));
        }
        match self {
            CreateIntegration { rules, .. } => rules.walk_mut(visitor),
            AlterIntegration { rules, .. } => rules.walk_mut(visitor),
            _ => Ok(VisitControl::Continue),
        }
    }
}

impl fmt::Display for ExternalAccessStmt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::CreateNetworkRule {
                name,
                create_option,
                rule_type,
                mode,
                values,
            } => {
                write!(
                    f,
                    "CREATE {}NETWORK RULE {}{name} TYPE = {rule_type} MODE = {mode} VALUE_LIST = (",
                    if matches!(create_option, CreateOption::CreateOrReplace) {
                        "OR REPLACE "
                    } else {
                        ""
                    },
                    if matches!(create_option, CreateOption::CreateIfNotExists) {
                        "IF NOT EXISTS "
                    } else {
                        ""
                    }
                )?;
                write_comma_separated_list(f, values.iter().map(|v| QuotedString(v, '\'')))?;
                write!(f, ")")
            }
            Self::AlterNetworkRule {
                name,
                if_exists,
                values,
            } => {
                write!(
                    f,
                    "ALTER NETWORK RULE {}{name} SET VALUE_LIST = (",
                    if *if_exists { "IF EXISTS " } else { "" }
                )?;
                write_comma_separated_list(f, values.iter().map(|v| QuotedString(v, '\'')))?;
                write!(f, ")")
            }
            Self::DropNetworkRule { name, if_exists } => write!(
                f,
                "DROP NETWORK RULE {}{name}",
                if *if_exists { "IF EXISTS " } else { "" }
            ),
            Self::CreateIntegration {
                name,
                create_option,
                rules,
                enabled,
            } => {
                write!(
                    f,
                    "CREATE {}EXTERNAL ACCESS INTEGRATION {}{name} ALLOWED_NETWORK_RULES = (",
                    if matches!(create_option, CreateOption::CreateOrReplace) {
                        "OR REPLACE "
                    } else {
                        ""
                    },
                    if matches!(create_option, CreateOption::CreateIfNotExists) {
                        "IF NOT EXISTS "
                    } else {
                        ""
                    }
                )?;
                write_comma_separated_list(f, rules)?;
                write!(f, ") ENABLED = {enabled}")
            }
            Self::AlterIntegration {
                name,
                if_exists,
                rules,
                enabled,
            } => {
                write!(
                    f,
                    "ALTER EXTERNAL ACCESS INTEGRATION {}{name} SET",
                    if *if_exists { "IF EXISTS " } else { "" }
                )?;
                if let Some(rules) = rules {
                    write!(f, " ALLOWED_NETWORK_RULES = (")?;
                    write_comma_separated_list(f, rules)?;
                    write!(f, ")")?;
                }
                if let Some(enabled) = enabled {
                    write!(f, " ENABLED = {enabled}")?;
                }
                Ok(())
            }
            Self::DropIntegration { name, if_exists } => write!(
                f,
                "DROP EXTERNAL ACCESS INTEGRATION {}{name}",
                if *if_exists { "IF EXISTS " } else { "" }
            ),
        }
    }
}
