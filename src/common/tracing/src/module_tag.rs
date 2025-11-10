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

/// A mapping between a module path prefix and a short log label.
#[derive(Debug)]
pub struct ModuleTag {
    pub prefix: &'static str,
    pub label: &'static str,
}

inventory::collect!(ModuleTag);

/// Resolve the best matching label for the provided module path.
pub fn label_for_module<'a>(full_name: &'a str) -> &'a str {
    inventory::iter::<ModuleTag>
        .into_iter()
        .filter_map(|entry| {
            full_name
                .starts_with(entry.prefix)
                .then_some((entry.prefix.len(), entry.label))
        })
        .max_by_key(|(len, _)| *len)
        .map(|(_, label)| label)
        .unwrap_or(full_name)
}

#[macro_export]
macro_rules! register_module_tag {
    ($label:expr $(,)?) => {
        inventory::submit! {
            #![crate = databend_common_tracing]

            databend_common_tracing::module_tag::ModuleTag {
                prefix: module_path!(),
                label: $label,
            }
        }
    };
    ($label:expr, $prefix:expr $(,)?) => {
        inventory::submit! {
            #![crate = databend_common_tracing]

            databend_common_tracing::module_tag::ModuleTag {
                prefix: $prefix,
                label: $label,
            }
        }
    };
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
}
