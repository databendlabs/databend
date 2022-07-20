use itertools::max;
use common_pipeline::Pipeline;

pub struct PipelineBuildResult {
    pub main_pipeline: Pipeline,
    pub sources_pipelines: Vec<Pipeline>,
}

impl PipelineBuildResult {
    pub fn set_max_threads(&mut self, max_threads: usize) {
        self.main_pipeline.set_max_threads(max_threads);

        for source_pipeline in &mut self.sources_pipelines {
            source_pipeline.set_max_threads(max_threads);
        }
    }
}
