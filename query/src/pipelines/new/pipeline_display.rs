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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;

impl NewPipeline {
    pub fn display_indent(&self) -> impl std::fmt::Display + '_ {
        NewPipelineIndentDisplayWrapper { pipeline: self }
    }
}

struct NewPipelineIndentDisplayWrapper<'a> {
    pipeline: &'a NewPipeline,
}

impl<'a> NewPipelineIndentDisplayWrapper<'a> {
    fn pipe_name(pipe: &NewPipe) -> &'static str {
        unsafe {
            match pipe {
                NewPipe::SimplePipe { processors, .. } => processors[0].name(),
                NewPipe::ResizePipe { processor, .. } => processor.name(),
            }
        }
    }
}

impl<'a> Display for NewPipelineIndentDisplayWrapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let pipes = &self.pipeline.pipes;
        for (index, pipe) in pipes.iter().rev().enumerate() {
            if index > 0 {
                writeln!(f)?;
            }

            for _ in 0..index {
                write!(f, "  ")?;
            }

            match pipe {
                NewPipe::SimplePipe { processors, .. } => {
                    write!(
                        f,
                        "{} × {} {}",
                        Self::pipe_name(pipe),
                        processors.len(),
                        if processors.len() == 1 {
                            "processor"
                        } else {
                            "processors"
                        },
                    )?;
                }
                NewPipe::ResizePipe {
                    inputs_port,
                    outputs_port,
                    ..
                } => {
                    let prev_name = Self::pipe_name(&pipes[index - 1]);
                    let post_name = Self::pipe_name(&pipes[index + 1]);

                    write!(
                        f,
                        "Merge ({} × {} {}) to ({} × {})",
                        prev_name,
                        inputs_port.len(),
                        if inputs_port.len() == 1 {
                            "processor"
                        } else {
                            "processors"
                        },
                        post_name,
                        outputs_port.len(),
                    )?;
                }
            }
        }

        Ok(())
    }
}
