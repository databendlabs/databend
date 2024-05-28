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
use std::fmt;

use itertools::Itertools;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Status {}

#[derive(Debug, Clone)]
pub struct Endpoints {
    /// The current leader.
    ///
    /// All requests should be sent to the leader in order to minimize latency by avoiding forwarding between meta-service nodes.
    current: Option<String>,

    /// Nodes of the meta-service cluster.
    nodes: BTreeMap<String, Status>,
}

#[allow(clippy::len_without_is_empty)]
impl Endpoints {
    pub fn new<S>(nodes: impl IntoIterator<Item = S>) -> Self
    where S: ToString {
        Self {
            current: None,
            nodes: nodes
                .into_iter()
                .map(|x| (x.to_string(), Status::default()))
                .collect(),
        }
    }

    /// Update `current` to choose the next node to send a request to.
    ///
    /// This is used when the `current` node fails.
    pub fn choose_next(&mut self) -> &str {
        if let Some(ref c) = self.current {
            let index = self.nodes.keys().position(|x| x == c).unwrap();

            self.current = self
                .nodes
                .keys()
                .nth((index + 1) % self.nodes.len())
                .map(|x| x.to_string());
        } else {
            self.current = self.nodes.keys().next().map(|x| x.to_string());
        }

        self.current.as_deref().unwrap()
    }

    /// Return the endpoint of the node to connect.
    ///
    /// It always return the currently known leader address.
    /// If it is not known, it will choose a next node.
    pub fn current_or_next(&mut self) -> &str {
        if self.current.is_none() {
            self.choose_next();
        }

        self.current.as_deref().unwrap()
    }

    pub fn current(&self) -> Option<&str> {
        self.current.as_deref()
    }

    pub fn nodes(&self) -> impl Iterator<Item = &'_ String> + '_ {
        self.nodes.keys()
    }

    pub fn set_current(
        &mut self,
        current: Option<impl ToString>,
    ) -> Result<Option<String>, String> {
        let current = current.map(|x| x.to_string());

        if let Some(c) = current.as_deref() {
            if !self.nodes.contains_key(c) {
                return Err(format!("node({}) to set is not in the nodes list", c));
            }
        }
        let prev = std::mem::replace(&mut self.current, current);
        Ok(prev)
    }

    /// Replace the nodes of the meta-service cluster.
    ///
    /// If the `current` node is not in the new nodes, it will be set to `None`.
    pub fn replace_nodes(&mut self, nodes: impl IntoIterator<Item = impl ToString>) {
        self.nodes = nodes
            .into_iter()
            .map(|x| (x.to_string(), Status::default()))
            .collect();

        if let Some(c) = self.current.as_deref() {
            if !self.nodes.contains_key(c) {
                self.current = None;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl fmt::Display for Endpoints {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let keys = self.nodes.keys().map(|x| x.as_str()).join(", ");
        write!(f, "current:{:?}, all:[{}]", self.current, keys)
    }
}

#[cfg(test)]
mod tests {
    use crate::endpoints::Endpoints;

    #[test]
    fn test_endpoints_new() -> anyhow::Result<()> {
        let es = Endpoints::new(["a", "b"]);
        assert_eq!("current:None, all:[a, b]", es.to_string());
        Ok(())
    }

    #[test]
    fn test_endpoints_choose_next() -> anyhow::Result<()> {
        let mut es = Endpoints::new(["a", "b"]);
        assert_eq!("a", es.choose_next());
        assert_eq!("b", es.choose_next());
        assert_eq!("a", es.choose_next());
        assert_eq!("b", es.choose_next());

        // Update nodes does not reset the index.
        es.replace_nodes(["c", "d"]);
        assert_eq!("c", es.choose_next());

        // Reduce nodes list size is ok.
        let mut es = Endpoints::new(["a", "b", "c"]);
        assert_eq!("a", es.choose_next());
        assert_eq!("b", es.choose_next());
        es.replace_nodes(["d"]);
        assert_eq!("d", es.choose_next());
        Ok(())
    }

    #[test]
    fn test_endpoints_current_or_next() -> anyhow::Result<()> {
        let mut es = Endpoints::new(["a", "b"]);
        assert_eq!("a", es.current_or_next());
        assert_eq!("a", es.current_or_next());
        assert_eq!("b", es.choose_next());
        assert_eq!("b", es.current_or_next());

        // Update nodes does not reset the index, but it will reset the current node.
        es.replace_nodes(["c", "d"]);
        assert_eq!("c", es.choose_next());

        // Reduce nodes list size is ok.
        let mut es = Endpoints::new(["a", "b", "c"]);
        assert_eq!("a", es.choose_next());
        assert_eq!("b", es.choose_next());
        es.replace_nodes(["d"]);
        assert_eq!("d", es.current_or_next());
        Ok(())
    }

    #[test]
    fn test_endpoints_current() -> anyhow::Result<()> {
        let mut es = Endpoints::new(["a", "b"]);
        assert_eq!(None, es.current());
        assert_eq!("a", es.choose_next());
        assert_eq!(Some("a"), es.current());

        // Updating nodes will reset the current node.
        es.replace_nodes(["c", "d"]);
        assert_eq!(None, es.current());

        Ok(())
    }

    #[test]
    fn test_endpoints_set_current() -> anyhow::Result<()> {
        let mut es = Endpoints::new(["a", "b"]);
        assert_eq!(Ok(None), es.set_current(Some("a")));
        assert_eq!(Some("a"), es.current());

        assert_eq!(Ok(Some("a".to_string())), es.set_current(Some("a")));

        assert_eq!(Ok(Some("a".to_string())), es.set_current(Some("b")));
        assert_eq!(Some("b"), es.current());

        assert!(es.set_current(Some("c")).is_err());
        assert_eq!(Some("b"), es.current());

        Ok(())
    }
}
