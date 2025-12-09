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

use std::sync::OnceLock;

pub use inventory::submit;

/// A mapping between a module path prefix and a short log label.
#[derive(Debug, Clone, Copy)]
pub struct ModuleTag {
    pub prefix: &'static str,
    pub label: &'static str,
}

inventory::collect!(ModuleTag);

static MODULE_TAG_TRIE: OnceLock<Trie<'static>> = OnceLock::new();

/// Resolve the best matching label for the provided module path.
pub fn label_for_module(full_name: &str) -> &str {
    if let Some(label) = MODULE_TAG_TRIE
        .get_or_init(|| Trie::build(inventory::iter::<ModuleTag>.into_iter().copied()))
        .search(full_name)
    {
        label
    } else {
        full_name
    }
}

#[macro_export]
macro_rules! register_module_tag {
    ($label:expr $(,)?) => {
        $crate::module_tag::submit! {
            $crate::module_tag::ModuleTag {
                prefix: module_path!(),
                label: $label,
            }
        }
    };
    ($label:expr, $prefix:expr $(,)?) => {
        $crate::module_tag::submit! {
            $crate::module_tag::ModuleTag {
                prefix: $prefix,
                label: $label,
            }
        }
    };
}

struct Trie<'a> {
    root: TrieNode<'a>,
}

impl<'a> Trie<'a> {
    fn build<I>(iter: I) -> Self
    where I: Iterator<Item = ModuleTag> {
        let mut root = TrieNode::default();
        for entry in iter {
            root.insert(entry.prefix, entry.label);
        }
        Trie { root }
    }

    fn search(&self, query: &str) -> Option<&'a str> {
        self.root.search(query.as_bytes())
    }
}

#[derive(Default)]
struct TrieNode<'a> {
    label: Option<&'a str>,
    segment: &'a [u8],
    children: Vec<TrieNode<'a>>,
}

impl<'a> TrieNode<'a> {
    fn insert(&mut self, prefix: &'a str, label: &'a str) -> bool {
        self.insert_inner(prefix.as_bytes(), label)
    }

    fn insert_inner(&mut self, bytes: &'a [u8], label: &'a str) -> bool {
        if bytes.is_empty() {
            return match &self.label {
                Some(_) => false,
                None => {
                    self.label = Some(label);
                    true
                }
            };
        }

        for child in &mut self.children {
            let lcp = child.segment_match_len(bytes);
            if lcp == 0 {
                continue;
            }

            if lcp < child.segment.len() {
                child.split_at(lcp);
            }

            return child.insert_inner(&bytes[lcp..], label);
        }

        self.children.push(TrieNode {
            label: Some(label),
            segment: bytes,
            children: Vec::new(),
        });
        true
    }

    fn search(&self, query: &[u8]) -> Option<&'a str> {
        let mut best = self.label;
        let mut current = self;
        let mut offset = 0;

        loop {
            let mut matched = false;
            for child in &current.children {
                let matched_len = child.segment_match_len(&query[offset..]);
                if matched_len == child.segment.len() {
                    offset += matched_len;
                    current = child;
                    if current.label.is_some() {
                        best = current.label;
                    }
                    matched = true;
                    break;
                }
            }

            if !matched {
                break;
            }
        }

        best
    }

    fn split_at(&mut self, mid: usize) {
        let prefix = &self.segment[..mid];
        let suffix = &self.segment[mid..];

        let suffix_node = TrieNode {
            segment: suffix,
            ..std::mem::take(self)
        };

        self.segment = prefix;
        self.children = vec![suffix_node];
    }

    fn segment_match_len(&self, bytes: &[u8]) -> usize {
        let mut idx = 0;
        while idx < bytes.len() && idx < self.segment.len() && bytes[idx] == self.segment[idx] {
            idx += 1;
        }
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    inventory::submit! {
        ModuleTag {
            prefix: "module::submodule",
            label: "[SUB]",
        }
    }

    inventory::submit! {
        ModuleTag {
            prefix: "module",
            label: "[MODULE]",
        }
    }

    #[test]
    fn chooses_longest_prefix() {
        assert_eq!(label_for_module("module::submodule::leaf"), "[SUB]");
    }

    #[test]
    fn falls_back_to_full_module_name() {
        assert_eq!(label_for_module("unknown::module"), "unknown::module");
    }

    #[test]
    fn trie_search_returns_none_without_prefix() {
        let trie = Trie::build(
            [
                ModuleTag {
                    prefix: "foo",
                    label: "foo",
                },
                ModuleTag {
                    prefix: "baraa",
                    label: "baraa",
                },
                ModuleTag {
                    prefix: "bar",
                    label: "bar",
                },
                ModuleTag {
                    prefix: "ba",
                    label: "ba",
                },
                ModuleTag {
                    prefix: "b",
                    label: "b",
                },
            ]
            .into_iter(),
        );

        assert_eq!(trie.search("baz"), Some("ba"));
        assert_eq!(trie.search("bar"), Some("bar"));
        assert_eq!(trie.search("baa"), Some("ba"));
        assert_eq!(trie.search("barista"), Some("bar"));
        assert_eq!(trie.search("food"), Some("foo"));
        assert_eq!(trie.search("qux"), None);
    }
}
