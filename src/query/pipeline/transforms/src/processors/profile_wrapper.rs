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

use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_profile::ProcessorProfile;
use databend_common_profile::SharedProcessorProfiles;

use crate::processors::transforms::Transform;
use crate::processors::transforms::Transformer;

/// A profile wrapper for `Processor` trait.
/// This wrapper will record the time cost of each processor.
/// But because of the limitation of `Processor` trait,
/// we can't get the number of rows processed by the processor.
pub struct ProcessorProfileWrapper<T> {
    inner: T,
    prof_id: u32,
    proc_profs: SharedProcessorProfiles,

    prof: ProcessorProfile,
}

impl<T> ProcessorProfileWrapper<T>
where T: Processor + 'static
{
    pub fn create(
        inner: T,
        prof_id: u32,
        proc_profs: SharedProcessorProfiles,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            prof_id,
            proc_profs,
            prof: ProcessorProfile::default(),
        })
    }
}

#[async_trait::async_trait]
impl<T> Processor for ProcessorProfileWrapper<T>
where T: Processor + 'static
{
    fn name(&self) -> String {
        self.inner.name()
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.inner.event()? {
            Event::Finished => {
                self.proc_profs
                    .lock()
                    .unwrap()
                    .update(self.prof_id, self.prof);
                Ok(Event::Finished)
            }
            v => Ok(v),
        }
    }

    fn process(&mut self) -> Result<()> {
        let instant = Instant::now();
        self.inner.process()?;
        let elapsed = instant.elapsed();
        self.prof = self.prof
            + ProcessorProfile {
                cpu_time: elapsed,
                ..Default::default()
            };
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let instant = Instant::now();
        self.inner.async_process().await?;
        let elapsed = instant.elapsed();
        self.prof = self.prof
            + ProcessorProfile {
                wait_time: elapsed,
                ..Default::default()
            };
        Ok(())
    }
}

/// A profile wrapper for `Transform` trait.
/// This wrapper will record the time cost and the information
/// about the number of rows processed by the processor.
pub struct TransformProfileWrapper<T> {
    inner: T,
    prof_id: u32,
    proc_profs: SharedProcessorProfiles,

    prof: ProcessorProfile,
}

impl<T> TransformProfileWrapper<T>
where T: Transform + 'static
{
    pub fn create(
        inner: T,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        prof_id: u32,
        proc_profs: SharedProcessorProfiles,
    ) -> Box<dyn Processor> {
        Box::new(Transformer::create(input_port, output_port, Self {
            inner,
            prof_id,
            proc_profs,
            prof: ProcessorProfile::default(),
        }))
    }
}

impl<T> Transform for TransformProfileWrapper<T>
where T: Transform + 'static
{
    const NAME: &'static str = "TransformProfileWrapper";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let input_rows = data.num_rows();
        let input_bytes = data.memory_size();

        let instant = Instant::now();
        let res = self.inner.transform(data)?;
        let elapsed = instant.elapsed();
        self.prof = self.prof
            + ProcessorProfile {
                cpu_time: elapsed,
                wait_time: Default::default(),
                input_rows,
                input_bytes,
                output_rows: res.num_rows(),
                output_bytes: res.memory_size(),
            };
        Ok(res)
    }

    fn on_finish(&mut self) -> Result<()> {
        self.proc_profs
            .lock()
            .unwrap()
            .update(self.prof_id, self.prof);
        Ok(())
    }
}

/// A stub transform for collecting profile information
/// at some point of the pipeline.
/// For example, we can profiling the output data of a
/// processor by adding a [`ProfileStub`] after it.
/// [`ProfileStub`] will pass through all the data without
/// any modification.
#[allow(clippy::type_complexity)]
pub struct ProfileStub {
    prof_id: u32,
    proc_profs: SharedProcessorProfiles,
    prof: ProcessorProfile,

    /// Callback function for processing the start event.
    on_start: Box<dyn Fn(&ProcessorProfile) -> ProcessorProfile + Send + Sync + 'static>,
    /// Callback function for processing the input data.
    on_process:
        Box<dyn Fn(&DataBlock, &ProcessorProfile) -> ProcessorProfile + Send + Sync + 'static>,
    /// Callback function for processing the finish event.
    on_finish: Box<dyn Fn(&ProcessorProfile) -> ProcessorProfile + Send + Sync + 'static>,
}

impl ProfileStub {
    pub fn new(prof_id: u32, proc_profs: SharedProcessorProfiles) -> Self {
        Self {
            prof_id,
            proc_profs,
            prof: Default::default(),
            on_start: Box::new(|_| Default::default()),
            on_process: Box::new(|_, _| Default::default()),
            on_finish: Box::new(|_| Default::default()),
        }
    }

    /// Create a new [`ProfileStub`] with `on_start` callback.
    /// The previous callback will be called before the new one.
    pub fn on_start(
        self,
        f: impl Fn(&ProcessorProfile) -> ProcessorProfile + Sync + Send + 'static,
    ) -> Self {
        Self {
            on_start: Box::new(move |prof| f(&(self.on_start)(prof))),
            ..self
        }
    }

    /// Create a new [`ProfileStub`] with `on_process` callback.
    /// The previous callback will be called before the new one.
    pub fn on_process(
        self,
        f: impl Fn(&DataBlock, &ProcessorProfile) -> ProcessorProfile + Sync + Send + 'static,
    ) -> Self {
        Self {
            on_process: Box::new(move |data, prof| f(data, &(self.on_process)(data, prof))),
            ..self
        }
    }

    /// Create a new [`ProfileStub`] with `on_finish` callback.
    /// The previous callback will be called before the new one.
    pub fn on_finish(
        self,
        f: impl Fn(&ProcessorProfile) -> ProcessorProfile + Sync + Send + 'static,
    ) -> Self {
        Self {
            on_finish: Box::new(move |prof| f(&(self.on_finish)(prof))),
            ..self
        }
    }

    /// Accumulate the number of output rows.
    pub fn accumulate_output_rows(self) -> Self {
        self.on_process(|data, prof| ProcessorProfile {
            output_rows: prof.output_rows + data.num_rows(),
            ..*prof
        })
    }

    /// Accumulate the number of output bytes.
    pub fn accumulate_output_bytes(self) -> Self {
        self.on_process(|data, prof| ProcessorProfile {
            output_bytes: prof.output_bytes + data.memory_size(),
            ..*prof
        })
    }
}

impl Transform for ProfileStub {
    const NAME: &'static str = "ProfileStub";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.prof = self.prof + (self.on_process)(&data, &self.prof);
        Ok(data)
    }

    fn on_start(&mut self) -> Result<()> {
        self.prof = self.prof + (self.on_start)(&self.prof);
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        self.prof = self.prof + (self.on_finish)(&self.prof);
        self.proc_profs
            .lock()
            .unwrap()
            .update(self.prof_id, self.prof);
        Ok(())
    }
}
