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

use std::collections::HashMap;

use common_exception::Result;
use common_expression::Expr;

use crate::processors::Processor;
use crate::processors::ProcessorPtr;

pub trait RuntimeFilter {
    // Get runtime filter from hash join, only hash join probe processor may return non-empty filters.
    fn get_runtime_filters(&mut self) -> Result<HashMap<String, Expr<String>>> {
        Ok(HashMap::new())
    }

    // If the processor can add runtime filter, only source related processors may return true.
    fn can_add_runtime_filter(&self) -> bool {
        false
    }

    // Add runtime filter to the processor, only source related processors may implement this function.
    fn add_runtime_filters(&mut self, _filters: &HashMap<String, Expr<String>>) -> Result<()> {
        Ok(())
    }
}

impl<T: Processor + ?Sized> RuntimeFilter for Box<T> {
    fn get_runtime_filters(&mut self) -> Result<HashMap<String, Expr<String>>> {
        (**self).get_runtime_filters()
    }

    fn can_add_runtime_filter(&self) -> bool {
        (**self).can_add_runtime_filter()
    }

    fn add_runtime_filters(&mut self, filters: &HashMap<String, Expr<String>>) -> Result<()> {
        (**self).add_runtime_filters(filters)
    }
}

impl ProcessorPtr {
    /// # Safety
    pub unsafe fn get_runtime_filters(&self) -> Result<HashMap<String, Expr<String>>> {
        (*self.inner().get()).get_runtime_filters()
    }

    /// # Safety
    pub unsafe fn can_add_runtime_filter(&self) -> bool {
        (*self.inner().get()).can_add_runtime_filter()
    }

    /// # Safety
    pub unsafe fn add_runtime_filters(
        &self,
        filters: &HashMap<String, Expr<String>>,
    ) -> Result<()> {
        (*self.inner().get()).add_runtime_filters(filters)
    }
}
