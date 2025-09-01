# logfowd2

![Tests and linters](https://github.com/soulgarden/logfowd2/actions/workflows/main.yml/badge.svg)
[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://github.com/soulgarden/logfowd2)
[![Tests](https://img.shields.io/badge/tests-269%20passing-success.svg)](https://github.com/soulgarden/logfowd2)
[![Code Quality](https://img.shields.io/badge/linter-zero%20warnings-success.svg)](https://github.com/soulgarden/logfowd2)

**High-performance Kubernetes log forwarding tool built with Rust**

Logfowd2 is a memory-efficient log forwarding daemon designed for Kubernetes environments. It monitors pod logs using filesystem events and streams them to Elasticsearch with advanced reliability features including circuit breakers, dead letter queues, and automatic backpressure control.

## 🚀 Key Features

### Production-Ready Reliability
- **Fault Tolerance** - Circuit breaker pattern prevents cascading failures
- **Dead Letter Queue** - Failed events persisted to disk with retry mechanism  
- **Atomic State Management** - Crash-safe state persistence with checksums
- **Graceful Shutdown** - Attempts to process remaining events before termination
- **Advanced Network Resilience** - Adaptive timeouts (30s-120s), network failure classification, automatic degradation detection

### Performance & Scalability
- **NotifyBridge Architecture** - Two-tier channel system preventing filesystem callback blocking
- **Bounded Channels** - Memory-safe queuing with automatic backpressure control
- **Pure Event-Driven Architecture** - No polling, responds only to filesystem events
- **Memory Bounded Operation** - Constant memory usage regardless of log volume
- **FIFO Event Ordering** - Preserves correct temporal sequence of log events

### Advanced System Optimization
- **MetadataCache System** - High-performance file metadata caching with TTL-based eviction (100ms TTL, LRU)
- **Intelligent Retry Management** - Universal exponential backoff retry mechanism for all async operations
- **Lock Optimization** - Drop/reacquire pattern minimizes lock contention and improves concurrency
- **Event-Driven File Monitoring** - Uses filesystem events for instant rotation detection
- **Historical Log Recovery** - Reads existing log content on startup (no data loss)
- **Symlink Support** - Full support for Kubernetes symlinked log files
- **Modern Logging** - Uses `tracing` and `tracing_subscriber` for structured, async-aware logging with JSON and human-readable formats

## 🏗️ Architecture

### Clean Architecture

logfowd2 is built with Domain-Driven Design (DDD) principles:
- **Modular design** - Clear separation between domain, infrastructure, and transport layers
- **Comprehensive testing** - 269 tests covering all critical paths
- **Type safety** - Leverages Rust's type system for compile-time guarantees
- **Extensible architecture** - Ready for parallel file processing and custom extensions

### Asynchronous Pipeline

The system implements a robust modular asynchronous pipeline:

```
Filesystem Events
        │
        ▼
┌─────────────────┐   unbounded     ┌─────────────────┐   bounded      ┌─────────────────────┐
│                 │   to bounded    │                 │   channels     │                     │
│  NotifyBridge   ├────────────────►│     Watcher     ├───────────────►│   ES Worker Pool    │
│                 │    channels     │                 │ (backpressure) │                     │
└─────────────────┘                 └─────────────────┘                └─────────────────────┘
        │                                   │                                   │
        ▼                                   ▼                                   ▼
Two-Tier Channel                      FileTracker                      Circuit Breaker
Drop-on-Overflow                      MetadataCache                    Dead Letter Queue
Buffer Management                     State Persist                    RetryManager
                                           │                           NetworkStats
                                           ▼                           Connection Pool
                                    ┌─────────────────┐
                                    │                 │
                                    │     Sender      │
                                    │                 │
                                    └─────────────────┘
                                           │
                                           ▼
                                    Batch Buffer
                                    Timer/Size
                                    Flush Logic
```

### Component Details

#### Watcher (`src/watcher.rs`)
- **Purpose**: Monitors `/var/log/pods` recursively using filesystem events
- **NotifyBridge Integration**: Uses NotifyBridge to prevent filesystem notify callback blocking
- **File Tracking**: Advanced FileTracker with symlink and rapid rotation support, leveraging MetadataCache
- **Metadata Parsing**: Extracts Kubernetes metadata (namespace, pod, container) from log paths
- **Initial Sync**: Processes existing files on startup with position restoration
- **Output**: Streams parsed events to Sender via bounded channels

#### Sender (`src/sender.rs`) 
- **Purpose**: Batches events based on size limits and time intervals
- **Event Ordering**: FIFO order using VecDeque (preserves temporal sequence)
- **Batching Logic**: Configurable `bulk_size` (1000 events) and `flush_interval` (1000ms)
- **Thread Safety**: Uses RwLock for concurrent access to event buffer
- **Backpressure**: Applies flow control when ES workers are overloaded
- **Output**: Forwards batched events to ES Worker Pool

#### ES Worker Pool (`src/infrastructure/elasticsearch/pool.rs`)
- **Purpose**: Parallel Elasticsearch workers with advanced fault tolerance
- **Network Resilience**: Adaptive timeouts (30s-120s) based on measured network latency
- **Circuit Breaker**: Fast-fail protection (10 failures → 30s timeout) with network awareness
- **Connection Pooling**: Reuses HTTP connections with adaptive timeout adjustment
- **Failure Classification**: Detailed network error types (DNS, TLS, Connection, Rate Limiting)
- **NetworkStats Monitoring**: Exponential moving average for latency tracking with degradation detection
- **Index Management**: Creates daily indices (`{index_name}-YYYY.MM.DD`)
- **Error Handling**: Failed events routed to Dead Letter Queue with RetryManager integration

#### NotifyBridge (`src/infrastructure/filesystem/notify.rs`)
- **Purpose**: Two-tier channel architecture preventing filesystem notify callback deadlocks
- **Architecture**: Filesystem events → unbounded channel → bounded channel → Watcher
- **Buffer Management**: Configurable buffer sizes with warning thresholds
- **Overflow Protection**: Configurable drop-on-overflow behavior to prevent memory exhaustion

#### MetadataCache (`src/infrastructure/filesystem/metadata_cache.rs`)
- **Purpose**: High-performance file metadata caching with configurable TTL (100ms default)
- **Performance**: Reduces filesystem syscalls for improved performance
- **Eviction Strategy**: LRU eviction with capacity management (1000 entries default)
- **Statistics**: Cache statistics tracking (hits, misses, hit rate) for optimization
- **Thread Safety**: Thread-safe metadata operations with automatic expiration

#### RetryManager (`src/retry.rs`)
- **Purpose**: Universal exponential backoff retry mechanism for resilient operations
- **Configuration**: Configurable retry parameters (max retries: 3, initial delay: 100ms, max delay: 10s)
- **Adaptive Behavior**: Exponential delay calculation with customizable backoff multiplier (2.0 default)
- **Generic Support**: Works with any async operation returning `Result<T, E>`
- **Error Preservation**: Final error from last attempt is preserved and returned

### Communication & Flow Control

- **NotifyBridge Integration**: Critical two-tier channel architecture prevents filesystem notify callback deadlocks
- **Bounded Channels**: Configurable capacity prevents memory exhaustion with intelligent buffer sizing
- **Backpressure Mechanism**: Automatic throttling when downstream is overloaded with adaptive delay scaling
- **Circuit Breaker Integration**: Network-aware protection against Elasticsearch cascade failures
- **RetryManager Coordination**: Universal retry mechanism ensures reliable event delivery across all components
- **MetadataCache Optimization**: Shared metadata caching reduces filesystem pressure across the pipeline
- **Atomic Operations**: State changes are crash-safe and consistent with optimized lock patterns

## ⚡ Performance Characteristics

### Throughput Optimization
- **Parallel ES Workers**: Concurrent bulk operations with configurable pool sizing
- **Adaptive Batching**: Size and time-based flushing with backpressure awareness
- **Memory Streaming**: Bounded buffer architecture prevents memory growth
- **Advanced Lock Optimization**: Drop/reacquire pattern minimizes lock contention during I/O operations

### Resource Efficiency  
- **Ultra-Low Memory Baseline**: 30-50Mi baseline memory usage
- **CPU Efficient**: Pure event-driven architecture eliminates polling overhead
- **Optimized Channel Buffers**: 78% memory reduction with smart sizing
- **Metadata Caching**: TTL-based filesystem metadata caching reduces syscalls by order of magnitude

### Network & System Resilience
- **Adaptive Network Behavior**: Dynamic timeout adjustment (30s-120s) based on measured latency
- **Network Failure Intelligence**: Detailed error classification (DNS, TLS, Connection, Rate Limiting)
- **NetworkStats Monitoring**: Exponential moving average latency tracking with degradation detection
- **Universal Retry Logic**: Exponential backoff (100ms → 10s) with configurable parameters for all async operations
- **NotifyBridge Protection**: Two-tier channel system prevents filesystem callback deadlocks

### Reliability Features
- **State Persistence**: Application state saved every 10 seconds with integrity checks
- **Crash Recovery**: Resumes from exact file positions after unexpected shutdowns
- **Data Integrity**: Checksums and atomic writes ensure state consistency
- **Lock Contention Minimization**: Clone-then-save pattern and scoped locking for maximum concurrency

## 🧪 Code Quality & Testing

### Test Coverage
- **263 Comprehensive Tests**: Unit, integration, and edge case coverage
- **Domain Testing**: File rotation, symlinks, corrupted files, permission issues
- **Network Testing**: Circuit breaker, retry logic, timeout behavior
- **Memory Testing**: Backpressure, channel overflow, cache eviction
- **Shutdown Testing**: Graceful shutdown with proper task coordination

### Code Quality Standards
- **Zero Linter Warnings**: Clean codebase with Clippy compliance
- **Domain-Driven Design**: Clear module boundaries and separation of concerns
- **Trait-Based Architecture**: Testable, mockable, and extensible design
- **Error Handling**: Comprehensive error types with context preservation
- **Documentation**: Inline documentation for all public APIs

## 🔧 Installation & Deployment

### Prerequisites
- **Platform**: Linux/Unix only (uses `std::os::unix` APIs and Unix signals)
- **Rust Toolchain**: 1.85+ (required for Rust 2024 edition support)
- **Kubernetes**: 1.14+ with `/var/log/pods` access
- **Elasticsearch**: 7.x+ or ZincSearch compatible target

### Quick Start with Helm

1. **Create namespace**:
   ```bash
   make create_namespace
   ```

2. **Install with Helm**:
   ```bash
   make helm_install
   ```

3. **Verify deployment**:
   ```bash
   kubectl get pods -n logging
   kubectl logs -n logging deployment/logfowd2
   ```

### Alternative Deployment Methods

#### Docker Compose (Development)
```bash
# Start local stack (Elasticsearch + logfowd2)
make docker_up

# Stop services  
make docker_down
```

#### Manual Docker Deployment
```bash
make build
docker run -d --name logfowd2 \
  -v /var/log/pods:/var/log/pods:ro \
  -v $(pwd)/config.json:/app/config.json:ro \
  soulgarden/logfowd2:0.1.0
```

#### Kubernetes DaemonSet (Manual)
```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: logfowd2
spec:
  selector:
    matchLabels:
      name: logfowd2
  template:
    spec:
      containers:
      - name: logfowd2
        image: soulgarden/logfowd2:0.1.0
        volumeMounts:
        - name: varlogpods
          mountPath: /var/log/pods
          readOnly: true
        - name: config
          mountPath: /app/config.json
          subPath: config.json
      volumes:
      - name: varlogpods
        hostPath:
          path: /var/log/pods
      - name: config
        configMap:
          name: logfowd2-config
```

## ⚙️ Configuration

### Core Configuration (`config.json`)

```json
{
  "log_path": "/var/log/pods",
  "state_file_path": "/tmp/logfowd2_state.json",
  "read_existing_on_startup": false,
  "read_chunk_size": 200,
  "max_line_size": 1048576,
  "max_concurrent_file_readers": 10,
  
  "channels": {
    "watcher_buffer_size": 300,
    "es_buffer_size": 30,
    "backpressure_threshold": 0.7,
    "backpressure_min_delay_ms": 5,
    "backpressure_max_delay_ms": 60,
    "notify_buffer_warning_threshold": 1000,
    "notify_buffer_max_size": 10000,
    "notify_drop_on_overflow": true,
    "notify_filesystem_buffer_warning_threshold": 1000,
    "notify_filesystem_buffer_size": 1024
  },
  
  "metrics": {
    "enabled": false,
    "port": 9090,
    "path": "/metrics"
  },
  
  "logging": {
    "log_level": "info",
    "log_format": "simple"
  },
  
  "es": {
    "host": "http://elasticsearch", 
    "port": 9200,
    "index_name": "logfowd",
    "flush_interval": 1000,
    "bulk_size": 300,
    "workers": 1
  }
}
```

**Configuration Field Explanations:**
- `max_concurrent_file_readers: 10` - Used by SmartTaskPool (2-10 dynamic range)
- `watcher_buffer_size: 300` - Events buffer from watcher (memory optimized)
- `es_buffer_size: 30` - Events buffer to ES workers (memory optimized)  
- `backpressure_threshold: 0.7` - Trigger backpressure at 70% buffer full
- `backpressure_min_delay_ms: 5` - Minimum adaptive delay when backpressure is active
- `backpressure_max_delay_ms: 60` - Maximum adaptive delay at full utilization
- `notify_buffer_warning_threshold: 1000` - Warning threshold for notify event buffer
- `notify_buffer_max_size: 10000` - Maximum size of notify event buffer
- `notify_drop_on_overflow: true` - Whether to drop events when notify buffer is full
- `notify_filesystem_buffer_warning_threshold: 1000` - Warning threshold for filesystem event buffer
- `notify_filesystem_buffer_size: 1024` - Size of filesystem event buffer
- `metrics.enabled: false` - Enable/disable Prometheus metrics
- `metrics.port: 9090` - Port for metrics endpoint
- `metrics.path: "/metrics"` - Path for metrics endpoint
- `logging.log_level: "info"` - Log level (trace/debug/info/warn/error)
- `logging.log_format: "simple"` - Log format (simple/json)
- `read_existing_on_startup: false|true` - If false, skip historical content and start from end (reduces startup memory spikes)
- `read_chunk_size: 200` - Max lines per read batch; smaller values reduce peak memory
- `max_line_size: 1048576` - Maximum bytes per log line (1MB default); prevents OOM from extremely long lines
- `index_name: "logfowd"` - Creates daily indices: logfowd-2024.01.15
- `flush_interval: 1000` - Batch timeout in milliseconds
- `bulk_size: 300` - Maximum events per batch
- `workers: 1` - Parallel ES worker count

### Tuning Guide

- Producers: control input rate
  - `max_concurrent_file_readers` (2–10), `read_chunk_size` (100–400)
- Consumers: increase drain rate
  - `es.flush_interval` (500–1000 ms), `es.bulk_size` (300–2000), `es.workers` (1–4)
- Buffers: absorb bursts
  - `channels.watcher_buffer_size`, `channels.es_buffer_size`
- Backpressure: latency vs. protection
  - `backpressure_threshold` (0.7–0.9), `backpressure_min_delay_ms`/`max_delay_ms`

Symptoms → Actions
- "Channel backpressure detected": lower producers (readers/chunk), raise consumers (flush/bulk/workers), or increase buffer.
- "ES worker pool backpressured": focus on ES throughput (scale ES, tweak bulk size/flush, index settings).

Helm overrides example:
```bash
helm upgrade -n logging logfowd helm/logfowd2 \
  --set app.elasticsearch.flush_interval=1000 \
  --set app.elasticsearch.bulk_size=500 \
  --set app.max_concurrent_file_readers=3 \
  --set app.read_chunk_size=100 \
  --set app.max_line_size=524288 \
  --set app.channels.es_buffer_size=60 \
  --set app.channels.backpressure_threshold=0.8
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CFG_PATH` | `./config.json` | Path to configuration file |

### Prometheus Metrics (Optional)

Logfowd2 includes comprehensive Prometheus metrics for observability. Metrics are **disabled by default** for performance:

```json
{
  "metrics": {
    "enabled": true,
    "port": 9090,
    "path": "/metrics"
  }
}
```

**Available Metrics:**
- `logfowd_events_processed_total` - Total events processed by component
- `logfowd_queue_size` - Current queue sizes and utilization
- `logfowd_circuit_breaker_state` - Circuit breaker state transitions
- `logfowd_workers_active` - Active worker count across components
- `logfowd_batch_size` - Distribution of batch sizes
- `logfowd_processing_duration_seconds` - Processing time histograms
- `logfowd_errors_total` - Error counts by type and component
- `logfowd_files_tracked` - Number of files being monitored per namespace

**Access metrics:**
```bash
curl http://localhost:9090/metrics
```

### Dead Letter Queue (Auto-Configured)
- **Location**: `/tmp/logfowd2_dead_letters.json`
- **Max Size**: 10,000 events
- **Retry Interval**: 60 seconds  
- **Max Retries**: 3 attempts per event
- **Cleanup**: Automatic removal after successful delivery

## 🛠️ Development & Maintenance

### Development Commands

```bash
make dev_check  # Format, lint, test, build
make ci         # Full CI pipeline
```

### Quality Assurance

```bash
cargo audit    # Security vulnerabilities  
cargo bench    # Performance benchmarks
```

### Building Custom Images

```bash
# Build multi-platform image
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t your-registry/logfowd2:custom \
  --push .
```

### Helm Management

```bash
make helm_upgrade  # Upgrade deployment
make helm_delete   # Uninstall
```

## 📊 Monitoring & Observability

### Key Events to Monitor

Based on the structured logging in the application, you can monitor:

- **Circuit Breaker Activity**: State transitions and error recovery
- **Dead Letter Queue Operations**: Failed event persistence and retry attempts  
- **File State Management**: Position updates, state persistence, and recovery
- **Component Status**: Watcher, sender, and ES worker pool activity

### Log Analysis

```bash
# View real-time logs
kubectl logs -f -n logging deployment/logfowd2

# Check for circuit breaker state changes
kubectl logs -n logging deployment/logfowd2 | grep "Circuit breaker.*transitioned"

# Monitor dead letter queue activity  
kubectl logs -n logging deployment/logfowd2 | grep "dead letters"

# Watch for state management issues
kubectl logs -n logging deployment/logfowd2 | grep "state"

# View error conditions
kubectl logs -n logging deployment/logfowd2 | grep "ERROR\|WARN"
```

---

**logfowd2** - High-performance Kubernetes log forwarding, reimagined in Rust 🦀
