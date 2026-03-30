# RFC: Ping-Pong Exchange Based on Arrow Flight DoExchange

## 1. Background

### 1.1 Current Implementation

Databend currently uses Arrow Flight's `do_get` for data exchange between nodes:

```
Sender Node                                    Receiver Node
     │                                              │
     │  ◄──── do_get (pull model) ─────────────    │
     │                                              │
     │  ────── FlightData stream ────────────►     │
     │                                              │
```

This is a **pull-based** model where the receiver initiates the connection and the sender streams data.

### 1.2 Motivation

Inspired by Apache Doris's shuffle channel implementation, we propose implementing a **ping-pong exchange** mechanism with:

1. **Backpressure Control**: Global queue-based backpressure to prevent memory overflow
2. **Batch Coalescing**: Accumulate multiple blocks before sending to reduce RPC overhead
3. **Bidirectional Communication**: Using `do_exchange` for true bidirectional streaming
4. **Simplified Scheduling**: Shared sink buffer reduces coordination complexity

### 1.3 Goals

- Implement ping-pong mode for flow control (one RPC in-flight per channel)
- Global backpressure across all destination channels
- Batch coalescing with configurable byte limit
- Leverage Arrow Flight's `do_exchange` for bidirectional communication

---

## 2. Architecture Overview

### 2.1 High-Level Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Sender Node                                        │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ Pipeline Task 0 │  │ Pipeline Task 1 │  │ Pipeline Task 2 │             │
│  │                 │  │                 │  │                 │             │
│  │ Partitioner     │  │ Partitioner     │  │ Partitioner     │             │
│  │      │          │  │      │          │  │      │          │             │
│  │      ▼          │  │      ▼          │  │      ▼          │             │
│  │ Serializer      │  │ Serializer      │  │ Serializer      │             │
│  └───────┬─────────┘  └───────┬─────────┘  └───────┬─────────┘             │
│          │                    │                    │                        │
│          └────────────────────┼────────────────────┘                        │
│                               ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    SharedExchangeSinkBuffer                           │  │
│  │                                                                       │  │
│  │   ┌─────────────────────────────────────────────────────────────┐    │  │
│  │   │ ChannelState[dest_0]                                        │    │  │
│  │   │   - queue: VecDeque<SerializedBlock>                        │    │  │
│  │   │   - is_idle: AtomicBool (ping-pong state)                   │    │  │
│  │   │   - flight_stream: DoExchangeStream                         │    │  │
│  │   └─────────────────────────────────────────────────────────────┘    │  │
│  │   ┌─────────────────────────────────────────────────────────────┐    │  │
│  │   │ ChannelState[dest_1]                                        │    │  │
│  │   │   ...                                                       │    │  │
│  │   └─────────────────────────────────────────────────────────────┘    │  │
│  │                                                                       │  │
│  │   total_queue_size: AtomicUsize                                      │  │
│  │   queue_capacity: usize (64 * num_destinations)                      │  │
│  │   backpressure_notify: Arc<Notify>                                   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ DoExchange (bidirectional)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Receiver Node                                      │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    ExchangeSourceBuffer                               │  │
│  │                                                                       │  │
│  │   receive_queue: VecDeque<FlightData>                                │  │
│  │   memory_limit: usize (20MB default)                                 │  │
│  │   ack_sender: Sender<Ack>  (for ping-pong response)                  │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                               │                                             │
│                               ▼                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ Deserializer    │  │ Deserializer    │  │ Deserializer    │             │
│  │      │          │  │      │          │  │      │          │             │
│  │      ▼          │  │      ▼          │  │      ▼          │             │
│  │ Pipeline Task 0 │  │ Pipeline Task 1 │  │ Pipeline Task 2 │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Ping-Pong Flow

```
Sender                              Receiver
   │                                    │
   │ ─── Ping (FlightData + blocks) ──► │
   │                                    │
   │     [Cannot send next until ack]   │
   │                                    │
   │ ◄─── Pong (Ack status) ─────────── │
   │                                    │
   │ ─── Ping (FlightData + blocks) ──► │
   │                                    │
   │ ◄─── Pong (Ack status) ─────────── │
   │                                    │
  ...                                  ...
```

---

## 3. Core Components

### 3.1 SerializedBlock

```rust
/// A serialized block ready for transmission
pub struct SerializedBlock {
    /// Arrow IPC encoded data
    pub data: Vec<u8>,
    /// Number of rows in this block
    pub num_rows: usize,
    /// Serialized byte size
    pub byte_size: usize,
    /// Source partition/task ID
    pub source_task_id: usize,
    /// End of stream marker
    pub eos: bool,
}
```

### 3.2 ChannelState

```rust
/// State for a single destination channel
pub struct ChannelState {
    /// Destination node identifier
    destination_id: String,

    /// Queue of serialized blocks waiting to be sent
    queue: Mutex<VecDeque<SerializedBlock>>,

    /// Ping-pong state: true if no RPC in-flight
    is_idle: AtomicBool,

    /// Sequence number for packets
    sequence: AtomicU64,

    /// Channel for sending acks back (from receiver task)
    ack_receiver: Receiver<AckMessage>,

    /// The bidirectional exchange stream
    exchange_stream: Option<DoExchangeStream>,

    /// Statistics
    stats: ChannelStats,
}

pub struct ChannelStats {
    pub rpc_count: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub total_latency_ns: AtomicU64,
}
```

### 3.3 SharedExchangeSinkBuffer

```rust
/// Shared buffer for all exchange sink tasks
pub struct SharedExchangeSinkBuffer {
    /// Channel states indexed by destination
    channels: Vec<ChannelState>,

    /// Total queue size across all channels (for global backpressure)
    total_queue_size: AtomicUsize,

    /// Queue capacity threshold (64 * num_destinations by default)
    queue_capacity: usize,

    /// Notify for backpressure release
    backpressure_notify: Arc<Notify>,

    /// Configuration
    config: ExchangeConfig,

    /// Query context
    query_id: String,
}

pub struct ExchangeConfig {
    /// Queue capacity factor (default: 64)
    pub queue_capacity_factor: usize,

    /// Max bytes per RPC batch (default: 256KB)
    pub max_batch_bytes: usize,

    /// Enable batch coalescing
    pub enable_batch_coalescing: bool,

    /// Low memory mode thresholds
    pub low_memory_queue_capacity: usize,
}
```

### 3.4 AckMessage

```rust
/// Acknowledgment message in ping-pong protocol
#[derive(Debug, Clone)]
pub struct AckMessage {
    /// Sequence number being acknowledged
    pub sequence: u64,

    /// Status of the receive operation
    pub status: AckStatus,

    /// Receiver-side timestamp (for latency calculation)
    pub receive_time_ns: u64,
}

#[derive(Debug, Clone)]
pub enum AckStatus {
    /// Successfully received
    Ok,
    /// Receiver buffer full (backpressure signal)
    BufferFull,
    /// Receiver encountered error
    Error(String),
    /// Receiver is done (EOS)
    Finished,
}
```

---

## 4. Protocol Design

### 4.1 FlightData Extension

We extend FlightData with metadata for ping-pong protocol:

```rust
/// Metadata keys in FlightData
pub mod metadata_keys {
    pub const SEQUENCE: &str = "x-sequence";
    pub const SOURCE_TASK_ID: &str = "x-source-task-id";
    pub const EOS: &str = "x-eos";
    pub const BATCH_COUNT: &str = "x-batch-count";
    pub const QUERY_ID: &str = "x-query-id";
    pub const CHANNEL_ID: &str = "x-channel-id";
}

/// Build FlightData with ping-pong metadata
fn build_flight_data(
    blocks: &[SerializedBlock],
    sequence: u64,
    query_id: &str,
    channel_id: &str,
) -> FlightData {
    let mut builder = FlightDataBuilder::new();

    // Combine multiple blocks into single FlightData
    for block in blocks {
        builder.append_ipc_data(&block.data);
    }

    builder
        .with_metadata(metadata_keys::SEQUENCE, sequence.to_string())
        .with_metadata(metadata_keys::BATCH_COUNT, blocks.len().to_string())
        .with_metadata(metadata_keys::EOS, blocks.last().map_or(false, |b| b.eos).to_string())
        .with_metadata(metadata_keys::QUERY_ID, query_id)
        .with_metadata(metadata_keys::CHANNEL_ID, channel_id)
        .build()
}
```

### 4.2 Ack FlightData

```rust
/// Build acknowledgment FlightData
fn build_ack_flight_data(ack: &AckMessage) -> FlightData {
    let body = serde_json::to_vec(ack).unwrap();

    FlightData {
        flight_descriptor: None,
        data_header: Bytes::new(),
        app_metadata: Bytes::from(body),
        data_body: Bytes::new(),
    }
}

/// Parse ack from FlightData
fn parse_ack_flight_data(data: &FlightData) -> Result<AckMessage> {
    serde_json::from_slice(&data.app_metadata)
        .map_err(|e| ErrorCode::InvalidData(format!("Invalid ack: {}", e)))
}
```

---

## 5. Implementation Details

### 5.1 SharedExchangeSinkBuffer Implementation

```rust
impl SharedExchangeSinkBuffer {
    /// Create a new shared buffer
    pub fn create(
        destinations: Vec<String>,
        config: ExchangeConfig,
        query_id: String,
    ) -> Arc<Self> {
        let num_destinations = destinations.len();
        let queue_capacity = config.queue_capacity_factor * num_destinations;

        let channels = destinations
            .into_iter()
            .map(|dest_id| ChannelState::new(dest_id))
            .collect();

        Arc::new(Self {
            channels,
            total_queue_size: AtomicUsize::new(0),
            queue_capacity,
            backpressure_notify: Arc::new(Notify::new()),
            config,
            query_id,
        })
    }

    /// Add a block to the queue for a specific destination
    /// Called by pipeline tasks (serializer output)
    pub async fn add_block(
        &self,
        destination_idx: usize,
        block: SerializedBlock,
    ) -> Result<()> {
        // Check global backpressure
        while self.total_queue_size.load(Ordering::Relaxed) >= self.queue_capacity {
            // Wait for backpressure release
            self.backpressure_notify.notified().await;
        }

        let channel = &self.channels[destination_idx];
        let mut queue = channel.queue.lock().await;
        queue.push_back(block);

        let prev_size = self.total_queue_size.fetch_add(1, Ordering::Relaxed);

        // Try to trigger send if channel is idle
        if channel.is_idle.load(Ordering::Relaxed) {
            self.try_send(destination_idx).await?;
        }

        Ok(())
    }

    /// Try to send data on a channel (ping)
    async fn try_send(&self, destination_idx: usize) -> Result<()> {
        let channel = &self.channels[destination_idx];

        // Atomically check and set idle -> busy
        if !channel.is_idle.compare_exchange(
            true, false,
            Ordering::AcqRel, Ordering::Relaxed
        ).is_ok() {
            // Another task is already sending
            return Ok(());
        }

        // Collect blocks for this batch (up to max_batch_bytes)
        let blocks = self.collect_batch(destination_idx).await;

        if blocks.is_empty() {
            // No data to send, mark idle again
            channel.is_idle.store(true, Ordering::Release);
            return Ok(());
        }

        // Build and send FlightData
        let sequence = channel.sequence.fetch_add(1, Ordering::Relaxed);
        let flight_data = build_flight_data(
            &blocks,
            sequence,
            &self.query_id,
            &channel.destination_id,
        );

        // Send via DoExchange stream
        self.send_flight_data(destination_idx, flight_data).await?;

        // Update queue size
        let removed = blocks.len();
        let new_size = self.total_queue_size.fetch_sub(removed, Ordering::Relaxed) - removed;

        // Release backpressure if below threshold
        if new_size < self.queue_capacity {
            self.backpressure_notify.notify_waiters();
        }

        Ok(())
    }

    /// Collect blocks for a batch, respecting byte limit
    async fn collect_batch(&self, destination_idx: usize) -> Vec<SerializedBlock> {
        let channel = &self.channels[destination_idx];
        let mut queue = channel.queue.lock().await;

        let mut blocks = Vec::new();
        let mut total_bytes = 0;

        while let Some(block) = queue.front() {
            if !blocks.is_empty() && total_bytes + block.byte_size > self.config.max_batch_bytes {
                break;
            }

            let block = queue.pop_front().unwrap();
            total_bytes += block.byte_size;
            blocks.push(block);
        }

        blocks
    }

    /// Handle ack from receiver (pong)
    pub async fn handle_ack(&self, destination_idx: usize, ack: AckMessage) -> Result<()> {
        let channel = &self.channels[destination_idx];

        // Update statistics
        channel.stats.rpc_count.fetch_add(1, Ordering::Relaxed);

        match ack.status {
            AckStatus::Ok | AckStatus::BufferFull => {
                // Mark channel as idle
                channel.is_idle.store(true, Ordering::Release);

                // Try to send next batch
                self.try_send(destination_idx).await?;
            }
            AckStatus::Error(msg) => {
                return Err(ErrorCode::ExchangeError(msg));
            }
            AckStatus::Finished => {
                // Receiver is done, close channel
                channel.is_idle.store(true, Ordering::Release);
            }
        }

        Ok(())
    }

    /// Set low memory mode
    pub fn set_low_memory_mode(&self) {
        // Reduce queue capacity
        // Note: This is a simplified version; actual implementation
        // would need careful synchronization
    }
}
```

### 5.2 DoExchange Server Implementation

```rust
impl FlightService for DatabendQueryFlightService {
    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Response<Self::DoExchangeStream> {
        let query_id = request.get_metadata("x-query-id")?;
        let channel_id = request.get_metadata("x-channel-id")?;

        // Get or create exchange source buffer for this channel
        let source_buffer = DataExchangeManager::instance()
            .get_or_create_source_buffer(&query_id, &channel_id)?;

        let mut inbound = request.into_inner();

        // Create outbound channel for acks
        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel::<FlightData>(16);

        // Spawn task to handle incoming data and send acks
        let source_buffer_clone = source_buffer.clone();
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(flight_data) => {
                        let ack = source_buffer_clone.receive_data(flight_data).await;
                        let ack_data = build_ack_flight_data(&ack);
                        if ack_tx.send(ack_data).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let ack = AckMessage {
                            sequence: 0,
                            status: AckStatus::Error(e.to_string()),
                            receive_time_ns: 0,
                        };
                        let _ = ack_tx.send(build_ack_flight_data(&ack)).await;
                        break;
                    }
                }
            }
        });

        // Convert ack channel to stream
        let stream = ReceiverStream::new(ack_rx).map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }
}
```

### 5.3 DoExchange Client Implementation

```rust
pub struct DoExchangeClient {
    client: FlightServiceClient<Channel>,
    query_id: String,
    channel_id: String,
}

impl DoExchangeClient {
    /// Start bidirectional exchange
    pub async fn start_exchange(
        &mut self,
        sink_buffer: Arc<SharedExchangeSinkBuffer>,
        destination_idx: usize,
    ) -> Result<()> {
        // Create outbound channel
        let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<FlightData>(64);

        // Convert to stream for gRPC
        let outbound_stream = ReceiverStream::new(outbound_rx);

        // Build request with metadata
        let mut request = Request::new(outbound_stream);
        request.metadata_mut().insert("x-query-id", self.query_id.parse()?);
        request.metadata_mut().insert("x-channel-id", self.channel_id.parse()?);

        // Start bidirectional stream
        let response = self.client.do_exchange(request).await?;
        let mut inbound = response.into_inner();

        // Store outbound sender in channel state
        sink_buffer.channels[destination_idx]
            .set_outbound_sender(outbound_tx);

        // Spawn task to handle acks
        let sink_buffer_clone = sink_buffer.clone();
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(ack_data) => {
                        if let Ok(ack) = parse_ack_flight_data(&ack_data) {
                            if let Err(e) = sink_buffer_clone
                                .handle_ack(destination_idx, ack)
                                .await
                            {
                                log::error!("Error handling ack: {}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error receiving ack: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }
}
```

### 5.4 ExchangeSourceBuffer Implementation

```rust
/// Buffer on receiver side
pub struct ExchangeSourceBuffer {
    /// Queue of received blocks
    receive_queue: Mutex<VecDeque<ReceivedBlock>>,

    /// Memory usage tracking
    memory_usage: AtomicUsize,

    /// Memory limit (default 20MB)
    memory_limit: usize,

    /// Notify for data availability
    data_notify: Arc<Notify>,

    /// Sequence tracking for each source
    expected_sequences: DashMap<String, u64>,
}

pub struct ReceivedBlock {
    pub data: FlightData,
    pub sequence: u64,
    pub receive_time_ns: u64,
}

impl ExchangeSourceBuffer {
    /// Receive data from sender
    pub async fn receive_data(&self, flight_data: FlightData) -> AckMessage {
        let receive_time = std::time::Instant::now();
        let sequence = flight_data
            .get_metadata(metadata_keys::SEQUENCE)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Check memory limit
        let data_size = flight_data.data_body.len();
        let current_usage = self.memory_usage.load(Ordering::Relaxed);

        if current_usage + data_size > self.memory_limit {
            return AckMessage {
                sequence,
                status: AckStatus::BufferFull,
                receive_time_ns: receive_time.elapsed().as_nanos() as u64,
            };
        }

        // Add to queue
        {
            let mut queue = self.receive_queue.lock().await;
            queue.push_back(ReceivedBlock {
                data: flight_data,
                sequence,
                receive_time_ns: receive_time.elapsed().as_nanos() as u64,
            });
        }

        self.memory_usage.fetch_add(data_size, Ordering::Relaxed);
        self.data_notify.notify_one();

        AckMessage {
            sequence,
            status: AckStatus::Ok,
            receive_time_ns: receive_time.elapsed().as_nanos() as u64,
        }
    }

    /// Get next block for pipeline consumption
    pub async fn next_block(&self) -> Option<ReceivedBlock> {
        loop {
            {
                let mut queue = self.receive_queue.lock().await;
                if let Some(block) = queue.pop_front() {
                    let size = block.data.data_body.len();
                    self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                    return Some(block);
                }
            }

            // Wait for data
            self.data_notify.notified().await;
        }
    }
}
```

---

## 6. Pipeline Integration

### 6.1 Transform Exchange Sink (Ping-Pong)

```rust
pub struct TransformExchangeSinkPingPong {
    /// Shared sink buffer
    sink_buffer: Arc<SharedExchangeSinkBuffer>,

    /// Partitioner for hash shuffle
    partitioner: Arc<dyn FlightScatter>,

    /// Serializer
    serializer: ExchangeSerializer,

    /// Input port
    input: Arc<InputPort>,

    /// Output port (for pipeline completion)
    output: Arc<OutputPort>,
}

#[async_trait::async_trait]
impl Processor for TransformExchangeSinkPingPong {
    fn name(&self) -> String {
        "TransformExchangeSinkPingPong".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.input.is_finished() {
            // Send EOS to all channels
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input.pull_data().transpose()? {
            // Partition the block
            let partitioned = self.partitioner.execute(data_block)?;

            // Serialize each partition
            for (dest_idx, block) in partitioned.into_iter().enumerate() {
                if !block.is_empty() {
                    let serialized = self.serializer.serialize(&block)?;

                    // Non-blocking add to queue
                    // Actual send happens in async context
                    self.pending_blocks.push((dest_idx, serialized));
                }
            }
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        // Send all pending blocks
        for (dest_idx, block) in self.pending_blocks.drain(..) {
            self.sink_buffer.add_block(dest_idx, block).await?;
        }

        // If input finished, send EOS
        if self.input.is_finished() {
            for dest_idx in 0..self.sink_buffer.channels.len() {
                self.sink_buffer.add_block(dest_idx, SerializedBlock::eos()).await?;
            }
        }

        Ok(())
    }
}
```

### 6.2 Transform Exchange Source (Ping-Pong)

```rust
pub struct TransformExchangeSourcePingPong {
    /// Source buffer
    source_buffer: Arc<ExchangeSourceBuffer>,

    /// Deserializer
    deserializer: ExchangeDeserializer,

    /// Output port
    output: Arc<OutputPort>,

    /// Received block pending deserialization
    pending_block: Option<ReceivedBlock>,
}

#[async_trait::async_trait]
impl Processor for TransformExchangeSourcePingPong {
    fn name(&self) -> String {
        "TransformExchangeSourcePingPong".to_string()
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if self.pending_block.is_some() {
            return Ok(Event::Sync);
        }

        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.pending_block.take() {
            let data_block = self.deserializer.deserialize(block.data)?;
            self.output.push_data(Ok(data_block));
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        self.pending_block = self.source_buffer.next_block().await;
        Ok(())
    }
}
```

---

## 7. Configuration

### 7.1 Session Variables

```sql
-- Queue capacity factor (blocks per destination)
SET exchange_ping_pong_queue_capacity_factor = 64;

-- Max batch size for coalescing (bytes)
SET exchange_ping_pong_max_batch_bytes = 262144;  -- 256KB

-- Enable/disable ping-pong mode
SET exchange_use_ping_pong = true;

-- Receiver memory limit
SET exchange_receiver_memory_limit = 20971520;  -- 20MB
```

### 7.2 Config Struct

```rust
#[derive(Clone, Debug)]
pub struct PingPongExchangeConfig {
    /// Queue capacity factor
    pub queue_capacity_factor: usize,

    /// Max batch bytes
    pub max_batch_bytes: usize,

    /// Use ping-pong mode
    pub use_ping_pong: bool,

    /// Receiver memory limit
    pub receiver_memory_limit: usize,

    /// Low memory queue capacity
    pub low_memory_queue_capacity: usize,
}

impl Default for PingPongExchangeConfig {
    fn default() -> Self {
        Self {
            queue_capacity_factor: 64,
            max_batch_bytes: 256 * 1024,
            use_ping_pong: true,
            receiver_memory_limit: 20 * 1024 * 1024,
            low_memory_queue_capacity: 8,
        }
    }
}
```

---

## 8. Error Handling & Resilience

### 8.1 Error Types

```rust
pub enum ExchangeError {
    /// Backpressure timeout
    BackpressureTimeout { destination: String, timeout_ms: u64 },

    /// Channel closed unexpectedly
    ChannelClosed { destination: String, reason: String },

    /// Serialization error
    SerializationError { source: Box<dyn std::error::Error> },

    /// Network error
    NetworkError { status: tonic::Status },

    /// Receiver buffer full
    ReceiverBufferFull { destination: String },

    /// Query cancelled
    QueryCancelled { query_id: String },
}
```

### 8.2 Graceful Shutdown

```rust
impl SharedExchangeSinkBuffer {
    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<()> {
        // Send EOS to all channels
        for (idx, channel) in self.channels.iter().enumerate() {
            // Wait for pending RPCs to complete
            while !channel.is_idle.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Clear remaining queue
            let mut queue = channel.queue.lock().await;
            queue.clear();
        }

        Ok(())
    }

    /// Cancel all pending operations
    pub fn cancel(&self) {
        // Mark all channels as closed
        for channel in &self.channels {
            channel.is_closed.store(true, Ordering::Release);
        }

        // Wake up any waiting tasks
        self.backpressure_notify.notify_waiters();
    }
}
```

---

## 9. Metrics & Observability

### 9.1 Metrics

```rust
pub struct PingPongExchangeMetrics {
    /// Total RPCs sent
    pub rpcs_sent: Counter,

    /// Total bytes sent
    pub bytes_sent: Counter,

    /// Total blocks sent
    pub blocks_sent: Counter,

    /// Average batch size
    pub avg_batch_size: Gauge,

    /// Backpressure wait time
    pub backpressure_wait_time: Histogram,

    /// RPC latency
    pub rpc_latency: Histogram,

    /// Queue size
    pub queue_size: Gauge,
}
```

### 9.2 Tracing

```rust
#[tracing::instrument(skip(self, block))]
async fn add_block(&self, destination_idx: usize, block: SerializedBlock) -> Result<()> {
    let _span = tracing::info_span!(
        "ping_pong_add_block",
        destination = %self.channels[destination_idx].destination_id,
        block_size = %block.byte_size,
    );

    // ... implementation
}
```

---

## 10. Testing Strategy

### 10.1 Unit Tests

```rust
#[tokio::test]
async fn test_ping_pong_basic() {
    let config = PingPongExchangeConfig::default();
    let buffer = SharedExchangeSinkBuffer::create(
        vec!["node1".to_string(), "node2".to_string()],
        config,
        "test_query".to_string(),
    );

    // Add blocks
    for i in 0..100 {
        buffer.add_block(i % 2, create_test_block(i)).await.unwrap();
    }

    // Verify distribution
    assert!(buffer.channels[0].queue.lock().await.len() > 0);
    assert!(buffer.channels[1].queue.lock().await.len() > 0);
}

#[tokio::test]
async fn test_backpressure() {
    let mut config = PingPongExchangeConfig::default();
    config.queue_capacity_factor = 2;  // Small capacity

    let buffer = SharedExchangeSinkBuffer::create(
        vec!["node1".to_string()],
        config,
        "test_query".to_string(),
    );

    // Fill queue
    for i in 0..2 {
        buffer.add_block(0, create_test_block(i)).await.unwrap();
    }

    // This should block due to backpressure
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        buffer.add_block(0, create_test_block(99)),
    ).await;

    assert!(result.is_err()); // Timeout expected
}
```

### 10.2 Integration Tests

```rust
#[tokio::test]
async fn test_do_exchange_end_to_end() {
    // Start test server
    let server = TestFlightServer::start().await;

    // Create client
    let client = DoExchangeClient::connect(&server.addr).await.unwrap();

    // Create sink buffer
    let buffer = SharedExchangeSinkBuffer::create(...);

    // Start exchange
    client.start_exchange(buffer.clone(), 0).await.unwrap();

    // Send data
    for i in 0..100 {
        buffer.add_block(0, create_test_block(i)).await.unwrap();
    }

    // Verify received on server
    assert_eq!(server.received_blocks().await, 100);
}
```

---

## 11. Migration Plan

### Phase 1: Infrastructure (Week 1-2)
- [ ] Implement `SharedExchangeSinkBuffer`
- [ ] Implement `ExchangeSourceBuffer`
- [ ] Implement `AckMessage` protocol
- [ ] Add DoExchange server handling

### Phase 2: Pipeline Integration (Week 3-4)
- [ ] Implement `TransformExchangeSinkPingPong`
- [ ] Implement `TransformExchangeSourcePingPong`
- [ ] Integrate with `ExchangeManager`
- [ ] Add session variables

### Phase 3: Testing & Optimization (Week 5-6)
- [ ] Unit tests
- [ ] Integration tests
- [ ] Benchmark against current implementation
- [ ] Performance tuning

### Phase 4: Rollout (Week 7-8)
- [ ] Feature flag for gradual rollout
- [ ] Documentation
- [ ] Monitoring dashboards
- [ ] Production validation

---

## 12. Future Enhancements

1. **Adaptive Batch Sizing**: Dynamically adjust batch size based on network conditions
2. **Priority Queues**: Support different priority levels for different query types
3. **Compression Selection**: Auto-select compression based on data characteristics
4. **Multi-Channel Parallelism**: Multiple DoExchange streams per destination
5. **Persistent Queues**: Spill to disk when memory pressure is high

---

## 13. References

- [Apache Doris Exchange Sink Buffer](https://github.com/apache/doris/blob/master/be/src/pipeline/exec/exchange_sink_buffer.h)
- [Arrow Flight DoExchange](https://arrow.apache.org/docs/format/Flight.html#do-exchange)
- [Databend Pipeline Architecture](https://databend.rs/doc/contributing/rfcs/pipeline)

---

## Appendix A: Sequence Diagram

```
┌─────────┐          ┌──────────────┐          ┌─────────┐          ┌──────────────┐
│Pipeline │          │SharedSinkBuf │          │DoExchange│          │SourceBuffer │
│  Task   │          │              │          │ Stream   │          │              │
└────┬────┘          └──────┬───────┘          └────┬────┘          └──────┬───────┘
     │                      │                       │                      │
     │ add_block(block)     │                       │                      │
     │─────────────────────►│                       │                      │
     │                      │                       │                      │
     │                      │ is_idle? Yes          │                      │
     │                      │──────────────────────►│                      │
     │                      │ send FlightData       │                      │
     │                      │                       │ receive_data()       │
     │                      │                       │─────────────────────►│
     │                      │                       │                      │
     │                      │                       │◄─────────────────────│
     │                      │                       │ AckMessage           │
     │                      │◄──────────────────────│                      │
     │                      │ handle_ack()          │                      │
     │                      │                       │                      │
     │                      │ is_idle? Yes          │                      │
     │                      │ try_send() again      │                      │
     │◄─────────────────────│                       │                      │
     │ Ok                   │                       │                      │
     │                      │                       │                      │
```
