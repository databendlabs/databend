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

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::quote::QuotedString;
use crate::ast::CreateOption;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateNetworkPolicyStmt {
    pub create_option: CreateOption,
    pub name: String,
    pub allowed_ip_list: Vec<String>,
    pub blocked_ip_list: Option<Vec<String>>,
    pub comment: Option<String>,
}

impl Display for CreateNetworkPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "NETWORK POLICY ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        write!(f, " ALLOWED_IP_LIST = (")?;
        for (i, ip) in self.allowed_ip_list.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "'{}'", ip)?;
        }
        write!(f, ")")?;
        if let Some(blocked_ip_list) = &self.blocked_ip_list {
            write!(f, " BLOCKED_IP_LIST = (")?;
            for (i, ip) in blocked_ip_list.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                write!(f, "'{}'", ip)?;
            }
            write!(f, ")")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterNetworkPolicyStmt {
    pub if_exists: bool,
    pub name: String,
    pub allowed_ip_list: Option<Vec<String>>,
    pub blocked_ip_list: Option<Vec<String>>,
    pub comment: Option<String>,
}

impl Display for AlterNetworkPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER NETWORK POLICY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{} SET ", self.name)?;

        if let Some(allowed_ip_list) = &self.allowed_ip_list {
            write!(f, " ALLOWED_IP_LIST = (")?;
            for (i, ip) in allowed_ip_list.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                write!(f, "'{}'", ip)?;
            }
            write!(f, ")")?;
        }
        if let Some(blocked_ip_list) = &self.blocked_ip_list {
            write!(f, " BLOCKED_IP_LIST = (")?;
            for (i, ip) in blocked_ip_list.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                write!(f, "'{}'", ip)?;
            }
            write!(f, ")")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropNetworkPolicyStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropNetworkPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP NETWORK POLICY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescNetworkPolicyStmt {
    pub name: String,
}

impl Display for DescNetworkPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE NETWORK POLICY {}", self.name)?;

        Ok(())
    }
}
