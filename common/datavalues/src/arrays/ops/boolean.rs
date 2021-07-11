// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::compute;
use common_exception::Result;

use crate::arrays::ArrayApply;
use crate::arrays::ArrayFullNull;
use crate::arrays::DataArray;
use crate::arrays::TakeRandom;
use crate::DFBooleanArray;

impl DFBooleanArray {
    pub fn and(&self, rhs: &DFBooleanArray) -> Result<Self> {
        match (self.len(), rhs.len()) {
            // We use Kleene logic because MySQL uses Kleene logic.
            (left, right) if left == right => {
                let result = compute::and_kleene(self.downcast_ref(), rhs.downcast_ref())?;
                Ok(DFBooleanArray::from_arrow_array(result))
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

    pub fn or(&self, rhs: &DFBooleanArray) -> Result<Self> {
        match (self.len(), rhs.len()) {
            // We use Kleene logic because MySQL uses Kleene logic.
            (left, right) if left == right => {
                let result = compute::or_kleene(self.downcast_ref(), rhs.downcast_ref())?;
                Ok(DFBooleanArray::from_arrow_array(result))
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
        let result = compute::not(self.downcast_ref())?;
        Ok(DFBooleanArray::from_arrow_array(result))
    }

    /// Check if all values are true
    pub fn all_true(&self) -> bool {
        let mut values = self.downcast_iter();
        values.all(|f| f.unwrap_or(false))
    }

    /// Check if all values are false
    pub fn all_false(&self) -> bool {
        let mut values = self.downcast_iter();
        values.all(|f| !f.unwrap_or(true))
    }
}
