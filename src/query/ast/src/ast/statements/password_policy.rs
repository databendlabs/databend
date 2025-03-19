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

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreatePasswordPolicyStmt {
    pub create_option: CreateOption,
    pub name: String,
    pub set_options: PasswordSetOptions,
}

impl Display for CreatePasswordPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "PASSWORD POLICY ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        write!(f, "{}", self.set_options)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterPasswordPolicyStmt {
    pub if_exists: bool,
    pub name: String,
    pub action: AlterPasswordAction,
}

impl Display for AlterPasswordPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER PASSWORD POLICY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{} ", self.name)?;
        write!(f, "{}", self.action)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterPasswordAction {
    SetOptions(PasswordSetOptions),
    UnSetOptions(PasswordUnSetOptions),
}

impl Display for AlterPasswordAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterPasswordAction::SetOptions(set_options) => {
                write!(f, "SET {}", set_options)?;
            }
            AlterPasswordAction::UnSetOptions(unset_options) => {
                write!(f, "UNSET {}", unset_options)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct PasswordSetOptions {
    pub min_length: Option<u64>,
    pub max_length: Option<u64>,
    pub min_upper_case_chars: Option<u64>,
    pub min_lower_case_chars: Option<u64>,
    pub min_numeric_chars: Option<u64>,
    pub min_special_chars: Option<u64>,
    pub min_age_days: Option<u64>,
    pub max_age_days: Option<u64>,
    pub max_retries: Option<u64>,
    pub lockout_time_mins: Option<u64>,
    pub history: Option<u64>,
    pub comment: Option<String>,
}

impl Display for PasswordSetOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(min_length) = self.min_length {
            write!(f, " PASSWORD_MIN_LENGTH = {}", min_length)?;
        }
        if let Some(max_length) = self.max_length {
            write!(f, " PASSWORD_MAX_LENGTH = {}", max_length)?;
        }
        if let Some(min_upper_case_chars) = self.min_upper_case_chars {
            write!(
                f,
                " PASSWORD_MIN_UPPER_CASE_CHARS = {}",
                min_upper_case_chars
            )?;
        }
        if let Some(min_lower_case_chars) = self.min_lower_case_chars {
            write!(
                f,
                " PASSWORD_MIN_LOWER_CASE_CHARS = {}",
                min_lower_case_chars
            )?;
        }
        if let Some(min_numeric_chars) = self.min_numeric_chars {
            write!(f, " PASSWORD_MIN_NUMERIC_CHARS = {}", min_numeric_chars)?;
        }
        if let Some(min_special_chars) = self.min_special_chars {
            write!(f, " PASSWORD_MIN_SPECIAL_CHARS = {}", min_special_chars)?;
        }
        if let Some(min_age_days) = self.min_age_days {
            write!(f, " PASSWORD_MIN_AGE_DAYS = {}", min_age_days)?;
        }
        if let Some(max_age_days) = self.max_age_days {
            write!(f, " PASSWORD_MAX_AGE_DAYS = {}", max_age_days)?;
        }
        if let Some(max_retries) = self.max_retries {
            write!(f, " PASSWORD_MAX_RETRIES = {}", max_retries)?;
        }
        if let Some(lockout_time_mins) = self.lockout_time_mins {
            write!(f, " PASSWORD_LOCKOUT_TIME_MINS = {}", lockout_time_mins)?;
        }
        if let Some(history) = self.history {
            write!(f, " PASSWORD_HISTORY = {}", history)?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct PasswordUnSetOptions {
    pub min_length: bool,
    pub max_length: bool,
    pub min_upper_case_chars: bool,
    pub min_lower_case_chars: bool,
    pub min_numeric_chars: bool,
    pub min_special_chars: bool,
    pub min_age_days: bool,
    pub max_age_days: bool,
    pub max_retries: bool,
    pub lockout_time_mins: bool,
    pub history: bool,
    pub comment: bool,
}

impl Display for PasswordUnSetOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if self.min_length {
            write!(f, " PASSWORD_MIN_LENGTH")?;
        }
        if self.max_length {
            write!(f, " PASSWORD_MAX_LENGTH")?;
        }
        if self.min_upper_case_chars {
            write!(f, " PASSWORD_MIN_UPPER_CASE_CHARS")?;
        }
        if self.min_lower_case_chars {
            write!(f, " PASSWORD_MIN_LOWER_CASE_CHARS")?;
        }
        if self.min_numeric_chars {
            write!(f, " PASSWORD_MIN_NUMERIC_CHARS")?;
        }
        if self.min_special_chars {
            write!(f, " PASSWORD_MIN_SPECIAL_CHARS")?;
        }
        if self.min_age_days {
            write!(f, " PASSWORD_MIN_AGE_DAYS")?;
        }
        if self.max_age_days {
            write!(f, " PASSWORD_MAX_AGE_DAYS")?;
        }
        if self.max_retries {
            write!(f, " PASSWORD_MAX_RETRIES")?;
        }
        if self.lockout_time_mins {
            write!(f, " PASSWORD_LOCKOUT_TIME_MINS")?;
        }
        if self.history {
            write!(f, " PASSWORD_HISTORY")?;
        }
        if self.comment {
            write!(f, " COMMENT")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropPasswordPolicyStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropPasswordPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP PASSWORD POLICY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DescPasswordPolicyStmt {
    pub name: String,
}

impl Display for DescPasswordPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE PASSWORD POLICY {}", self.name)?;

        Ok(())
    }
}
