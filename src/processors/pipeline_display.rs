// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;
use std::fmt::Display;

use crate::processors::{EmptyProcessor, MergeProcessor, Pipeline};

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
                self.0
                    .walk_preorder(|pipe| {
                        write_indent(f)?;

                        let ways = pipe.len();
                        let processor = pipe[0].clone();
                        if processor
                            .as_any()
                            .downcast_ref::<EmptyProcessor>()
                            .is_some()
                        {
                            write!(f, "")?;
                        } else if processor
                            .as_any()
                            .downcast_ref::<MergeProcessor>()
                            .is_some()
                        {
                            let prev_pipe = self.0.processors[index].clone();
                            let prev_name = prev_pipe[0].name().to_string();
                            let prev_ways = prev_pipe.len();

                            let post_pipe = self.0.processors[index + 2].clone();
                            let post_name = post_pipe[0].name().to_string();
                            let post_ways = post_pipe.len();

                            write!(
                                f,
                                "Merge ({} × {} {}) to ({} × {})",
                                prev_name,
                                prev_ways,
                                if prev_ways == 1 {
                                    "processor"
                                } else {
                                    "processors"
                                },
                                post_name,
                                post_ways,
                            )?;
                        } else {
                            write!(
                                f,
                                "{} × {} {}",
                                processor.name(),
                                ways,
                                if ways == 1 { "processor" } else { "processors" },
                            )?;
                        }
                        index += 1;
                        Ok(true)
                    })
                    .map_err(|_| fmt::Error)?;
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
