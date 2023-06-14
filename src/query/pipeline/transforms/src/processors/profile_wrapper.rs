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

use std::time::Instant;

use common_exception::Result;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_profile::ProcessorProfile;
use common_profile::SharedProcessorProfiles;

pub struct ProfileWrapper<T> {
    inner: T,
    prof_span_id: u32,
    prof_span_set: SharedProcessorProfiles,

    prof: ProcessorProfile,
}

impl<T> ProfileWrapper<T>
where T: Processor + 'static
{
    pub fn create(
        inner: T,
        prof_span_id: u32,
        prof_span_set: SharedProcessorProfiles,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            prof_span_id,
            prof_span_set,
            prof: ProcessorProfile::default(),
        })
    }
}

#[async_trait::async_trait]
impl<T> Processor for ProfileWrapper<T>
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
                self.prof_span_set
                    .lock()
                    .unwrap()
                    .update(self.prof_span_id, self.prof);
                Ok(Event::Finished)
            }
            v => Ok(v),
        }
    }

    fn process(&mut self) -> Result<()> {
        let instant = Instant::now();
        self.inner.process()?;
        let elapsed = instant.elapsed();
        self.prof = self.prof + ProcessorProfile { cpu_time: elapsed };
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        // TODO: record profile information for async process
        self.inner.async_process().await
    }
}
