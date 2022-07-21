use std::fmt::{Display, Formatter};
use crate::api::DataExchange;
use crate::interpreters::QueryFragmentActions;
use crate::interpreters::QueryFragmentsActions;

impl QueryFragmentsActions {
    pub fn display_indent(&self) -> impl std::fmt::Display + '_ {
        QueryFragmentsActionsWrap { inner: self }
    }
}

struct QueryFragmentsActionsWrap<'a> {
    inner: &'a QueryFragmentsActions,
}

impl<'a> Display for QueryFragmentsActionsWrap<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (index, fragment_actions) in self.inner.fragments_actions.iter().enumerate() {
            if index != 0 {
                writeln!(f, "");
            }

            writeln!(f, "{}", fragment_actions.display_indent())?;
        }

        Ok(())
    }
}

impl QueryFragmentActions {
    pub fn display_indent(&self) -> impl std::fmt::Display + '_ {
        QueryFragmentActionsWrap { inner: self }
    }
}

struct QueryFragmentActionsWrap<'a> {
    inner: &'a QueryFragmentActions,
}

impl<'a> Display for QueryFragmentActionsWrap<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
            write!(f, "{}", fragment_action.node.display_indent_format(1))?;
        }

        Ok(())
    }
}

