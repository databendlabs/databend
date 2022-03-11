// Copyright 2021 Datafuse Labs.
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
use std::fmt::Display;

use crate::pipelines::processors::Pipeline;

impl Pipeline {
    pub fn display_indent(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a Pipeline);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

                let mut index = 0;

                self.0.walk_preorder(|pipe| {
                    write_indent(f)?;

                    let ways = pipe.nums();
                    let processor = pipe.processor_by_index(0);

                    match processor.name() {
                        "EmptyProcessor" => write!(f, "")?,
                        "MergeProcessor" => {
                            let mut pipes = self.0.pipes();
                            pipes.reverse();

                            let prev_pipe = pipes[index - 1].clone();
                            let prev_name = prev_pipe.name().to_string();
                            let prev_ways = prev_pipe.nums();

                            let post_pipe = pipes[index + 1].clone();
                            let post_name = post_pipe.name().to_string();
                            let post_ways = post_pipe.nums();

                            write!(
                                f,
                                "Merge ({} × {} {}) to ({} × {})",
                                post_name,
                                post_ways,
                                if post_ways == 1 {
                                    "processor"
                                } else {
                                    "processors"
                                },
                                prev_name,
                                prev_ways,
                            )?;
                        }
                        "MixedProcessor" => {
                            let mut pipes = self.0.pipes();
                            pipes.reverse();

                            let prev_pipe = pipes[index - 1].clone();
                            let prev_name = prev_pipe.name().to_string();
                            let prev_ways = prev_pipe.nums();

                            let post_pipe = pipes[index + 1].clone();
                            let post_name = post_pipe.name().to_string();
                            let post_ways = post_pipe.nums();

                            write!(
                                f,
                                "Mixed ({} × {} {}) to ({} × {} {})",
                                post_name,
                                post_ways,
                                if post_ways == 1 {
                                    "processor"
                                } else {
                                    "processors"
                                },
                                prev_name,
                                prev_ways,
                                if prev_ways == 1 {
                                    "processor"
                                } else {
                                    "processors"
                                },
                            )?;
                        }
                        "RemoteTransform" => {
                            let name = processor.name();

                            // TODO: We should output for every remote
                            write!(
                                f,
                                "{} × {} processor(s)",
                                name, ways /*, pipeline_display*/
                            )?
                        }
                        _ => {
                            write!(
                                f,
                                "{} × {} {}",
                                processor.name(),
                                ways,
                                if ways == 1 { "processor" } else { "processors" },
                            )?;
                        }
                    }

                    index += 1;
                    Result::<bool, fmt::Error>::Ok(true)
                })?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    pub fn display_graphviz(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a Pipeline);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin Databend GraphViz Pipeline (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;
                // TODO()
                writeln!(f, "}}")?;
                writeln!(f, "// End Databend GraphViz Pipeline")?;
                Ok(())
            }
        }
        Wrapper(self)
    }
}

impl fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}
