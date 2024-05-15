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

use databend_common_sql::MetadataRef;

use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::servers::flight::v1::exchange::DataExchange;

impl QueryFragmentsActions {
    pub fn display_indent<'a>(&'a self, metadata: &'a MetadataRef) -> impl Display + '_ {
        QueryFragmentsActionsWrap {
            inner: self,
            metadata,
        }
    }
}

struct QueryFragmentsActionsWrap<'a> {
    inner: &'a QueryFragmentsActions,
    metadata: &'a MetadataRef,
}

impl<'a> Display for QueryFragmentsActionsWrap<'a> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        for (index, fragment_actions) in self.inner.fragments_actions.iter().enumerate() {
            if index != 0 {
                writeln!(f)?;
            }

            writeln!(f, "{}", fragment_actions.display_indent(self.metadata))?;
        }

        Ok(())
    }
}

impl QueryFragmentActions {
    pub fn display_indent<'a>(&'a self, metadata: &'a MetadataRef) -> impl Display + '_ {
        QueryFragmentActionsWrap {
            inner: self,
            metadata,
        }
    }
}

struct QueryFragmentActionsWrap<'a> {
    inner: &'a QueryFragmentActions,
    metadata: &'a MetadataRef,
}

impl<'a> Display for QueryFragmentActionsWrap<'a> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        writeln!(f, "Fragment {}:", self.inner.fragment_id)?;

        if let Some(data_exchange) = &self.inner.data_exchange {
            match data_exchange {
                DataExchange::Merge(_) => writeln!(f, "  DataExchange: Merge")?,
                DataExchange::Broadcast(_) => writeln!(f, "  DataExchange: Broadcast")?,
                DataExchange::ShuffleDataExchange(_) => writeln!(f, "  DataExchange: Shuffle")?,
            }
        }

        if !self.inner.fragment_actions.is_empty() {
            let fragment_action = &self.inner.fragment_actions[0];
            let plan_display_string = fragment_action
                .physical_plan
                .format(self.metadata.clone(), Default::default())
                .and_then(|node| node.format_pretty_with_prefix("    "))
                .unwrap();
            write!(f, "{}", plan_display_string)?;
        }

        Ok(())
    }
}
