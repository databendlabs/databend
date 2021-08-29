// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::compute;
use common_exception::Result;

use crate::prelude::*;

impl DFBooleanArray {
    pub fn and_kleene(&self, rhs: &DFBooleanArray) -> Result<Self> {
        match (self.len(), rhs.len()) {
            // We use Kleene logic because MySQL uses Kleene logic.
            (left, right) if left == right => {
                let result = compute::boolean_kleene::and(self.inner(), rhs.inner())?;
                Ok(DFBooleanArray::new(result))
            }
            (_, 1) => {
                let opt_rhs = rhs.get(0);
                match opt_rhs {
                    None => Ok(self.apply_with_idx_on_opt(|(_, lhs)| match lhs {
                        Some(false) => Some(false),
                        _ => None,
                    })),
                    Some(rhs) => Ok(self.apply_with_idx_on_opt(|(_, lhs)| match lhs {
                        None if rhs => None,
                        None => Some(false),
                        Some(lhs) => Some(lhs && rhs),
                    })),
                }
            }
            (1, _) => {
                let opt_lhs = self.get(0);
                match opt_lhs {
                    None => Ok(rhs.apply_with_idx_on_opt(|(_, rhs)| match rhs {
                        Some(false) => Some(false),
                        _ => None,
                    })),
                    Some(lhs) => Ok(rhs.apply_with_idx_on_opt(|(_, rhs)| match rhs {
                        None if lhs => None,
                        None => Some(false),
                        Some(rhs) => Some(lhs && rhs),
                    })),
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn or_kleene(&self, rhs: &DFBooleanArray) -> Result<Self> {
        match (self.len(), rhs.len()) {
            // We use Kleene logic because MySQL uses Kleene logic.
            (left, right) if left == right => {
                let result = compute::boolean_kleene::or(self.inner(), rhs.inner())?;
                Ok(DFBooleanArray::new(result))
            }
            (_, 1) => {
                let opt_rhs = rhs.get(0);
                match opt_rhs {
                    None => Ok(self.apply_with_idx_on_opt(|(_, lhs)| match lhs {
                        Some(true) => Some(true),
                        _ => None,
                    })),
                    Some(rhs) => Ok(self.apply_with_idx_on_opt(|(_, lhs)| match lhs {
                        None if rhs => Some(true),
                        None => None,
                        Some(lhs) => Some(lhs || rhs),
                    })),
                }
            }
            (1, _) => {
                let opt_lhs = self.get(0);
                match opt_lhs {
                    None => Ok(rhs.apply_with_idx_on_opt(|(_, rhs)| match rhs {
                        Some(true) => Some(true),
                        _ => None,
                    })),
                    Some(lhs) => Ok(rhs.apply_with_idx_on_opt(|(_, rhs)| match rhs {
                        None if lhs => Some(true),
                        None => None,
                        Some(rhs) => Some(lhs || rhs),
                    })),
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn not(&self) -> Result<Self> {
        let result = compute::boolean::not(self.inner());
        Ok(DFBooleanArray::new(result))
    }

    /// Check if all values are true
    pub fn all_true(&self) -> bool {
        let mut values = self.into_iter();
        values.all(|f| f.unwrap_or(false))
    }

    /// Check if all values are false
    pub fn all_false(&self) -> bool {
        let mut values = self.into_iter();
        values.all(|f| !f.unwrap_or(true))
    }
}
