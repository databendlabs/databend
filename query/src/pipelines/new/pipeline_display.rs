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
