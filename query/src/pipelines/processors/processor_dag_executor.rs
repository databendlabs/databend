use std::collections::{VecDeque, HashMap, HashSet};

use petgraph::Direction;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::IntoNeighbors;

use common_base::tokio::sync::mpsc::{channel, Sender, Receiver};
use common_exception::{ErrorCode, Result};
use common_streams::SendableDataBlockStream;

use crate::pipelines::processors::{ProcessorsDAG, Processor};
use crate::sessions::{DatabendQueryContext, DatabendQueryContextRef};
use common_datablocks::DataBlock;
use std::sync::Arc;
use std::any::Any;
use common_base::TrySpawn;
use tokio_stream::StreamExt;
use futures::{Future};
use common_base::tokio::macros::support::{Pin, Poll};
use std::task::Context;
use std::collections::hash_map::Entry;
use tokio_stream::wrappers::ReceiverStream;
use common_base::tokio::sync::mpsc::error::TrySendError;

pub struct ProcessorDAGExecutor {
    graph: ProcessorsDAG,
    work_queue: VecDeque<NodeIndex>,
    completed: HashSet<NodeIndex>,
    channels: HashMap<NodeIndex, Vec<Sender<Result<DataBlock>>>>,
}

impl ProcessorDAGExecutor {
    pub fn create(graph: ProcessorsDAG) -> ProcessorDAGExecutor {
        ProcessorDAGExecutor { graph, work_queue: VecDeque::new(), completed: HashSet::new(), channels: HashMap::new() }
    }

    pub fn execute(&mut self, ctx: DatabendQueryContextRef) -> Result<SendableDataBlockStream> {
        let graph = &self.graph.graph;
        let outgoing_externals = graph.externals(Direction::Outgoing);
        self.work_queue = outgoing_externals.collect();

        self.execute_graph_nodes(ctx)?;
        unimplemented!("")
    }

    fn execute_graph_nodes(&mut self, ctx: DatabendQueryContextRef) -> Result<()> {
        while let Some(node) = self.work_queue.pop_front() {
            self.execute_graph_node(node, ctx.as_ref())?;
        }

        Ok(())
    }

    fn execute_graph_node(&mut self, node: NodeIndex, ctx: &DatabendQueryContext) -> Result<()> {
        if self.completed.contains(&node) {
            return Ok(());
        }

        let graph = &mut self.graph.graph;
        let neighbors = graph.neighbors_directed(node, Direction::Incoming);
        let neighbors = neighbors.collect::<Vec<NodeIndex>>();

        match neighbors.len() {
            0 => { /* Source */ },
            1 => {
                // Transform
                self.execute_graph_node(neighbors[0], ctx)?;

                let current = &mut graph[node];
                current.connect_to(graph[neighbors[0]].clone());
            },
            multiple_inputs => {
                // When a processor has multiple inputs or outputs, it will be regarded as a pipe
                // This is because we must ensure that the inputs and outputs are parallel
                let (tx, rx) = channel(multiple_inputs);

                for neighbor in neighbors {
                    self.work_queue.push_back(neighbor);

                    match self.channels.entry(neighbor) {
                        Entry::Vacant(entry) => entry.insert(vec![tx.clone()]),
                        Entry::Occupied(mut entry) => entry.get_mut().push(tx.clone()),
                    };
                }

                let current = &mut graph[node];
                current.connect_to(Arc::new(ChannelsProcessor(rx)))?;
            }
        };

        if let Some(channels) = self.channels.remove(&node) {
            let processor = graph[node].clone();
            let shared_runtime = ctx.get_shared_runtime()?;
            shared_runtime.spawn(async move || {
                match processor.execute().await {
                    Err(cause) => {
                        let pusher = push_error(cause, channels.clone());
                        pusher.await;
                    }
                    Ok(data_stream) => {
                        let pusher = push_to_channels(data_stream, channels.clone());
                        pusher.await;
                    }
                };
            });
        }

        self.completed.insert(node);
        Ok(())
    }
}

async fn push_error(cause: ErrorCode, channels: Vec<Sender<Result<DataBlock>>>) {}

async fn push_to_channels(mut inner: SendableDataBlockStream, channels: Vec<Sender<Result<DataBlock>>>) {
    let mut channels_pos = 0;
    while let Some(data) = inner.next().await {
        if data.is_err() {
            let cause = data.expect_err("Must be return error when after is_err = true.");
            push_error(cause, channels).await;
            return;
        }

        match channels[channels_pos].try_reserve() {
            Ok(permit) => permit.send(data),
            Err(_) => {
                // We try send data to other channels when the channel is high load.
                for index in 1..channels.len() {
                    let channel = &channels[(index + channels_pos) % channels.len()];
                    if let Ok(permit) = channel.try_reserve() {
                        permit.send(data);
                        break;
                    } else if index + 1 == channels_pos {
                        if let Err(cause) = channels[channels_pos].send(data).await {
                            let message = format!("Merge processor cannot push data: {}", cause);
                            let send_error = push_error(ErrorCode::TokioError(message), channels);
                            send_error.await;
                            return;
                        }
                        break;
                    }
                }
            }
        }

        channels_pos += 1;
    }
}


struct ChannelsProcessor(Receiver<Result<DataBlock>>);

impl Processor for ChannelsProcessor {
    fn name(&self) -> &str {
        "ChannelsProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        Err(ErrorCode::IllegalTransformConnectionState(""))
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        // ReceiverStream::new(self.0)
        unimplemented!()
    }
}
