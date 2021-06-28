// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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
                    "// Begin DataFuse GraphViz Pipeline (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;
                // TODO()
                writeln!(f, "}}")?;
                writeln!(f, "// End DataFuse GraphViz Pipeline")?;
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
