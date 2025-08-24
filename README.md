# logfowd2

![Tests and linters](https://github.com/soulgarden/logfowd2/actions/workflows/main.yml/badge.svg)

**High-performance Kubernetes log forwarding tool built with Rust**

Logfowd2 is a memory-efficient log forwarding daemon designed for Kubernetes environments. It monitors pod logs using filesystem events and streams them to Elasticsearch with advanced reliability features including circuit breakers, dead letter queues, and automatic backpressure control.

## ⚡ Recent Architectural Improvements

### Data Integrity & Correctness
- **FIFO Event Ordering** - Fixed sender to preserve correct temporal sequence of log events
- **Historical Log Recovery** - No data loss on startup - reads all existing log content  
- **File Rotation Handling** - Robust support for Kubernetes log file rotation scenarios

### Performance Optimizations  
- **SmartTaskPool Architecture** - Dynamic worker scaling (2-10 workers) with 30s idle timeout
- **Memory Baseline Reduction** - 90% reduction: from ~128Mi to 30-50Mi baseline usage
- **CPU Efficiency** - Eliminated 100ms polling cycles for pure event-driven architecture
- **Optimized Lock Management** - Reduced critical sections for better concurrency under load

### Production Readiness
- **Kubernetes Resource Limits** - Optimized to run within 150m CPU / 128Mi memory limits
- **Channel Buffer Optimization** - 78% memory reduction with smart buffer sizing
- **Debug Mode Control** - Production deployments use non-verbose logging by default

## 🚀 Key Features

### Production-Ready Reliability
- **Fault Tolerance** - Circuit breaker pattern prevents cascading failures
- **Dead Letter Queue** - Failed events persisted to disk with retry mechanism  
- **Atomic State Management** - Crash-safe state persistence with checksums
- **Graceful Shutdown** - Attempts to process remaining events before termination
- **Error Recovery** - Comprehensive error handling with persistent event storage

### Performance & Scalability
- **Smart Dynamic TaskPool** - On-demand worker scaling with automatic idle timeout
- **Bounded Channels** - Memory-safe queuing with automatic backpressure control
- **Pure Event-Driven Architecture** - No polling, responds only to filesystem events
- **Memory Bounded Operation** - Constant memory usage regardless of log volume
- **FIFO Event Ordering** - Preserves correct temporal sequence of log events

### Advanced File Handling
- **Event-Driven File Monitoring** - Uses filesystem events for instant rotation detection
- **Historical Log Recovery** - Reads existing log content on startup (no data loss)
- **Symlink Support** - Full support for Kubernetes symlinked log files
- **Optimized Lock Management** - Minimal critical sections for better concurrency

## 🏗️ Architecture

logfowd2 implements a robust 3-component asynchronous pipeline designed for high throughput and fault tolerance:

```
┌─────────────────┐    bounded     ┌─────────────────┐    bounded     ┌─────────────────────┐
│                 │    channels    │                 │    channels    │                     │
│     Watcher     ├───────────────►│     Sender      ├───────────────►│   ES Worker Pool    │
│                 │  (backpressure)│                 │  (backpressure)│                     │
└─────────────────┘                └─────────────────┘                └─────────────────────┘
        │                                   │                                   │
        ▼                                   ▼                                   ▼
   FileTracker                         Batch Buffer                    Circuit Breaker
   State Persist                       Timer/Size                     Dead Letter Queue
   Symlink Support                     Flush Logic                    Connection Pool
```

### Component Details

#### Watcher (`src/watcher.rs`)
- **Purpose**: Monitors `/var/log/pods` recursively using filesystem events
- **File Tracking**: Advanced FileTracker with symlink and rapid rotation support
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

#### ES Worker Pool (`src/es_worker_pool.rs`)
- **Purpose**: Parallel Elasticsearch workers with fault tolerance
- **Scalability**: Configurable worker pool size for horizontal scaling
- **Circuit Breaker**: Fast-fail protection (10 failures → 30s timeout)
- **Connection Pooling**: Reuses HTTP connections with 30s timeouts
- **Index Management**: Creates daily indices (`{index_name}-YYYY.MM.DD`)
- **Error Handling**: Failed events routed to Dead Letter Queue

### Communication & Flow Control

- **Bounded Channels**: Configurable capacity prevents memory exhaustion
- **Backpressure Mechanism**: Automatic throttling when downstream is overloaded
- **Circuit Breaker Integration**: Protects against Elasticsearch cascade failures
- **Atomic Operations**: State changes are crash-safe and consistent

## ⚡ Performance Characteristics

### Throughput Optimization
- **Parallel ES Workers**: Concurrent bulk operations with configurable pool sizing
- **Adaptive Batching**: Size and time-based flushing with backpressure awareness
- **Memory Streaming**: Bounded buffer architecture prevents memory growth

### Resource Efficiency  
- **Ultra-Low Memory Baseline**: 30-50Mi baseline with SmartTaskPool (90% reduction)
- **CPU Efficient**: Pure event-driven architecture eliminates polling overhead
- **Dynamic Worker Scaling**: 2-10 workers on-demand with 30s idle timeout
- **Optimized Channel Buffers**: 78% memory reduction with smart sizing
- **Smart Retry Logic**: Exponential backoff (500ms → 30s) reduces CPU waste

### Reliability Features
- **State Persistence**: Application state saved every 10 seconds with integrity checks
- **Crash Recovery**: Resumes from exact file positions after unexpected shutdowns
- **Data Integrity**: Checksums and atomic writes ensure state consistency
- **Structured Logging**: JSON logs for observability and debugging

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
  soulgarden/logfowd2:0.0.8
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
        image: soulgarden/logfowd2:0.0.8
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
  "is_debug": false,
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
    "level": "info",
    "format": "simple"
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
- `logging.level: "info"` - Log level (debug/info/warn/error)
- `logging.format: "simple"` - Log format (simple/structured)
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

Helm overrides example
`helm upgrade -n logging logfowd helm/logfowd2 \
  --set app.elasticsearch.flush_interval=1000 \
  --set app.elasticsearch.bulk_size=500 \
  --set app.max_concurrent_file_readers=3 \
  --set app.read_chunk_size=100 \
  --set app.max_line_size=524288 \
  --set app.channels.es_buffer_size=60 \
  --set app.channels.backpressure_threshold=0.8`

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
