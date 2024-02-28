use std::sync::Arc;
use databend_common_base::runtime::profile::Profile;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::pipelines::executor::QueriesPipelineExecutor;
use crate::pipelines::executor::QueryPipelineExecutor;

pub enum PipelineExecutor {
    QueryPipelineExecutor(Arc<QueryPipelineExecutor>),
    QueriesPipelineExecutor(Arc<QueriesPipelineExecutor>),
}

impl PipelineExecutor {
    pub fn execute(&self) -> Result<()> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.execute(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.execute(),
        }
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.finish(cause),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.finish(cause),
        }
    }

    pub fn is_finished(&self) -> bool {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.is_finished(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.is_finished(),
        }
    }

    pub fn format_graph_nodes(&self) -> String {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.format_graph_nodes(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.format_graph_nodes(),
        }
    }

    pub fn get_profiles(&self) -> Vec<Arc<Profile>> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.get_profiles(),
            PipelineExecutor::QueriesPipelineExecutor(executor) => executor.get_profiles(),
        }
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn execute_single_thread(self, thread_num: usize) -> Result<()> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => {
                executor.execute_single_thread(thread_num)
            }
            PipelineExecutor::QueriesPipelineExecutor(executor) => {
                executor.execute_single_thread(thread_num)
            }
        }
    }
}
