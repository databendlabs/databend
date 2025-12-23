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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowWorkloadGroupsStmt {}

impl Display for ShowWorkloadGroupsStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW WORKLOAD GROUPS")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub enum QuotaValueStmt {
    Duration(Duration),
    Percentage(usize),
    Bytes(usize),
    Number(usize),
}

impl QuotaValueStmt {
    fn parse_percentage(v: &str) -> Option<QuotaValueStmt> {
        let v = v.trim();
        if v.is_empty() {
            return None;
        }

        if v.ends_with('%') {
            let num = v.trim_end_matches('%').trim();
            if let Ok(value) = num.parse::<usize>()
                && value <= 100
            {
                return Some(QuotaValueStmt::Percentage(value));
            }
        }
        None
    }

    fn parse_human_size(v: &str) -> Option<QuotaValueStmt> {
        let v = v.trim();
        if v.is_empty() {
            return None;
        }

        if v == "0" {
            return Some(QuotaValueStmt::Bytes(0));
        }

        let (num_str, unit) = v.split_at(
            v.find(|c: char| !c.is_ascii_digit() && c != '.')
                .unwrap_or(v.len()),
        );

        let num = num_str.parse::<f64>().ok()?;
        if num <= 0.0 {
            return None;
        }

        let multiplier = match unit.trim().to_lowercase().as_str() {
            "" | "b" => 1,
            "k" | "kb" => 1024,
            "m" | "mb" => 1024 * 1024,
            "g" | "gb" => 1024 * 1024 * 1024,
            _ => return None,
        };

        let bytes = (num * multiplier as f64).round() as usize;
        Some(QuotaValueStmt::Bytes(bytes))
    }

    fn parse_human_timeout(v: &str) -> Option<QuotaValueStmt> {
        let v = v.trim();
        if v.is_empty() {
            return None;
        }

        if v == "0" {
            return Some(QuotaValueStmt::Duration(Duration::from_secs(0)));
        }

        let (num_str, unit) = v.split_at(
            v.find(|c: char| !c.is_ascii_digit() && c != '.')
                .unwrap_or(v.len()),
        );

        let num = num_str.parse::<f64>().ok()?;
        if num <= 0.0 {
            return None;
        }

        let duration = match unit.trim().to_lowercase().as_str() {
            "s" | "sec" | "secs" => Duration::from_secs_f64(num),
            "m" | "min" | "mins" => Duration::from_secs_f64(num * 60.0),
            "h" | "hour" | "hours" => Duration::from_secs_f64(num * 3600.0),
            "d" | "day" | "days" => Duration::from_secs_f64(num * 86400.0),
            "ms" | "milli" | "millis" => Duration::from_millis(num as u64),
            "" => Duration::from_secs_f64(num), // default to seconds if no unit
            _ => return None,
        };

        Some(QuotaValueStmt::Duration(duration))
    }

    fn parse_number(v: &str) -> Option<QuotaValueStmt> {
        v.trim().parse::<usize>().ok().map(QuotaValueStmt::Number)
    }

    pub fn new(key: &str, v: String) -> Result<QuotaValueStmt, &'static str> {
        match key {
            "cpu_quota" => Self::parse_percentage(&v).ok_or("Invalid CPU quota value, expected percentage (e.g. '50%') between 0-100"),
            "memory_quota" => Self::parse_percentage(&v).or_else(|| Self::parse_human_size(&v)).ok_or("Invalid memory quota value, expected percentage (e.g. '50%') or size (e.g. '1GB', '512MB')"),
            "query_timeout" => Self::parse_human_timeout(&v).ok_or("Invalid query timeout value, expected duration (e.g. '30s', '5min', '1h')"),
            "max_concurrency" => Self::parse_number(&v).filter(|x| !matches!(x, QuotaValueStmt::Number(0))).ok_or("Invalid max concurrency value, expected positive integer"),
            "query_queued_timeout" => Self::parse_human_timeout(&v).ok_or("Invalid queued query timeout value, expected duration (e.g. '30s', '5min', '1h')"),
            "max_memory_usage_ratio" => Self::parse_percentage(&v).ok_or("Invalid max_memory_usage_ratio value, expected percentage (e.g. '50%') between 0-100"),
            _ => Err("Unknown quota key"),
        }
    }
}

impl Display for QuotaValueStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaValueStmt::Percentage(v) => write!(f, "{}%", v),
            QuotaValueStmt::Duration(v) => write!(f, "{:?}", v),
            QuotaValueStmt::Bytes(v) => write!(f, "{}", v),
            QuotaValueStmt::Number(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateWorkloadGroupStmt {
    pub name: Identifier,
    pub if_not_exists: bool,
    #[drive(skip)]
    pub quotas: BTreeMap<String, QuotaValueStmt>,
}

impl Display for CreateWorkloadGroupStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE WORKLOAD GROUP")?;

        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }

        write!(f, " {}", self.name)?;

        if !self.quotas.is_empty() {
            write!(f, " WITH ")?;

            for (idx, (key, value)) in self.quotas.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }

                write!(f, " {} = '{}'", key, value)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropWorkloadGroupStmt {
    pub name: Identifier,
    pub if_exists: bool,
}

impl Display for DropWorkloadGroupStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP WORKLOAD GROUP ")?;

        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }

        write!(f, " {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RenameWorkloadGroupStmt {
    pub name: Identifier,
    pub new_name: Identifier,
}

impl Display for RenameWorkloadGroupStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RENAME WORKLOAD GROUP {} TO {}",
            self.name, self.new_name
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct SetWorkloadGroupQuotasStmt {
    pub name: Identifier,
    #[drive(skip)]
    pub quotas: BTreeMap<String, QuotaValueStmt>,
}

impl Display for SetWorkloadGroupQuotasStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER WORKLOAD GROUP {}", self.name)?;

        if !self.quotas.is_empty() {
            write!(f, " SET")?;

            for (idx, (key, value)) in self.quotas.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }

                write!(f, " {} = '{}'", key, value)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UnsetWorkloadGroupQuotasStmt {
    pub name: Identifier,
    #[drive(skip)]
    pub quotas: Vec<Identifier>,
}

impl Display for UnsetWorkloadGroupQuotasStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER WORKLOAD GROUP {}", self.name)?;

        if !self.quotas.is_empty() {
            write!(f, " UNSET")?;

            for (idx, name) in self.quotas.iter().enumerate() {
                if idx != 0 {
                    write!(f, ",")?;
                }

                write!(f, " {}", name)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::ast::QuotaValueStmt;

    #[test]
    fn test_parse_percentage() {
        // Valid cases
        // Valid cases
        assert_eq!(
            QuotaValueStmt::parse_percentage("50%"),
            Some(QuotaValueStmt::Percentage(50))
        );
        assert_eq!(
            QuotaValueStmt::parse_percentage("100%"),
            Some(QuotaValueStmt::Percentage(100))
        );
        assert_eq!(
            QuotaValueStmt::parse_percentage(" 0% "),
            Some(QuotaValueStmt::Percentage(0))
        );

        // Invalid cases
        assert_eq!(QuotaValueStmt::parse_percentage("101%"), None);
        assert_eq!(QuotaValueStmt::parse_percentage("50"), None);
        assert_eq!(QuotaValueStmt::parse_percentage("1/2"), None);
        assert_eq!(QuotaValueStmt::parse_percentage(""), None);
        assert_eq!(QuotaValueStmt::parse_percentage(" % "), None);
        assert_eq!(QuotaValueStmt::parse_percentage("-10%"), None);
        assert_eq!(QuotaValueStmt::parse_percentage("abc%"), None);
    }

    #[test]
    fn test_parse_human_size() {
        // Valid cases
        assert_eq!(
            QuotaValueStmt::parse_human_size("0"),
            Some(QuotaValueStmt::Bytes(0))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_size("1024"),
            Some(QuotaValueStmt::Bytes(1024))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_size("1.5KB"),
            Some(QuotaValueStmt::Bytes(1536))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_size(" 2 MB "),
            Some(QuotaValueStmt::Bytes(2 * 1024 * 1024))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_size("1G"),
            Some(QuotaValueStmt::Bytes(1024 * 1024 * 1024))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_size("0.5gb"),
            Some(QuotaValueStmt::Bytes(512 * 1024 * 1024))
        );

        // Invalid cases
        assert_eq!(QuotaValueStmt::parse_human_size(""), None);
        assert_eq!(QuotaValueStmt::parse_human_size("-1"), None);
        assert_eq!(QuotaValueStmt::parse_human_size("1.2.3"), None);
        assert_eq!(QuotaValueStmt::parse_human_size("1TB"), None);
        assert_eq!(QuotaValueStmt::parse_human_size("abc"), None);
    }

    #[test]
    fn test_parse_human_timeout() {
        // Valid cases
        assert_eq!(
            QuotaValueStmt::parse_human_timeout("0"),
            Some(QuotaValueStmt::Duration(Duration::from_secs(0)))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_timeout("30"),
            Some(QuotaValueStmt::Duration(Duration::from_secs(30)))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_timeout("1.5s"),
            Some(QuotaValueStmt::Duration(Duration::from_secs_f64(1.5)))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_timeout(" 30 MIN "),
            Some(QuotaValueStmt::Duration(Duration::from_secs(30 * 60)))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_timeout("2h"),
            Some(QuotaValueStmt::Duration(Duration::from_secs(2 * 3600)))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_timeout("100ms"),
            Some(QuotaValueStmt::Duration(Duration::from_millis(100)))
        );
        assert_eq!(
            QuotaValueStmt::parse_human_timeout("0.5d"),
            Some(QuotaValueStmt::Duration(Duration::from_secs(12 * 3600)))
        );

        // Invalid cases
        assert_eq!(QuotaValueStmt::parse_human_timeout(""), None);
        assert_eq!(QuotaValueStmt::parse_human_timeout("-1"), None);
        assert_eq!(QuotaValueStmt::parse_human_timeout("1.2.3"), None);
        assert_eq!(QuotaValueStmt::parse_human_timeout("1y"), None);
        assert_eq!(QuotaValueStmt::parse_human_timeout("abc"), None);
    }

    #[test]
    fn test_parse_number() {
        // Valid cases
        assert_eq!(
            QuotaValueStmt::parse_number("0"),
            Some(QuotaValueStmt::Number(0))
        );
        assert_eq!(
            QuotaValueStmt::parse_number("123"),
            Some(QuotaValueStmt::Number(123))
        );
        assert_eq!(
            QuotaValueStmt::parse_number(" 456 "),
            Some(QuotaValueStmt::Number(456))
        );

        // Invalid cases
        assert_eq!(QuotaValueStmt::parse_number(""), None);
        assert_eq!(QuotaValueStmt::parse_number("-1"), None);
        assert_eq!(QuotaValueStmt::parse_number("1.2"), None);
        assert_eq!(QuotaValueStmt::parse_number("abc"), None);
    }
}
