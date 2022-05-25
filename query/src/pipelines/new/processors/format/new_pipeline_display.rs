// Copyright 2022 Datafuse Labs.
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

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::pipelines::new::NewPipeline;

impl NewPipeline {
    pub fn display_indent(&self) -> impl Display + '_ {
        struct Wrapper<'a>(&'a NewPipeline);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                let mut indent = 0;
                let mut write_indent = |f: &mut fmt::Formatter| -> fmt::Result {
                    if indent > 0 {
                        writeln!(f)?;
                    }
                    for _ in 0..indent {
                        write!(f, "  ")?;
                    }
                    indent += 1;
                    Ok(())
                };
                self.0.walk_preorder(|pipe| unsafe {
                    write_indent(f)?;
                    let ways = pipe.size();
                    let processor = pipe.processor_by_index(0);
                    write!(
                        f,
                        "{} × {} {}",
                        processor.name(),
                        ways,
                        if ways == 1 { "processor" } else { "processors" },
                    )?;
                    Result::<bool, fmt::Error>::Ok(true)
                })?;
                Ok(())
            }
        }
        Wrapper(self)
    }
}

impl Debug for NewPipeline {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.display_indent().fmt(f)
    }
}
