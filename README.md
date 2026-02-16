# logfowd2

![Tests and linters](https://github.com/soulgarden/logfowd2/actions/workflows/main.yml/badge.svg)

`logfowd2` forwards Kubernetes pod logs from node files (kubelet-style logs under paths like `/var/log/pods`) to Elasticsearch (Bulk API). It aims to be lightweight, event-driven, and resilient to temporary Elasticsearch/network issues.

Status: used by the author in production for ~2 years across several projects.

## Features

- Event-driven log file watching using `notify` (no polling)
- Resume offsets via a state file (`state_file_path`, default: `/tmp/logfowd2_state.json`)
- Batching and configurable worker pool for Elasticsearch bulk ingestion
- Bounded queues with backpressure
- Circuit breaker around Elasticsearch operations
- Dead letter queue persisted to disk (default: `/tmp/logfowd2_dead_letters.json`)
- Structured logging with `tracing`

## Installation (Helm)

Prerequisites:

- Kubernetes cluster with access to kubelet log files on nodes (typically `/var/log/pods`)
- Elasticsearch endpoint (Bulk API)

Install:

```bash
make create_namespace
make helm_install
```

Check pods and logs:

```bash
kubectl get pods -n logging -l app.kubernetes.io/instance=logfowd
kubectl logs -n logging -l app.kubernetes.io/instance=logfowd -f
```

Upgrade / uninstall:

```bash
make helm_upgrade
make helm_delete
```

## Scaling (cluster-wide)

The Helm chart deploys `replicaCount` instances and uses required pod anti-affinity to spread pods across hosts. For a "one instance per node" setup, set `replicaCount` to the number of nodes you want to cover.

```bash
helm upgrade -n logging logfowd helm/logfowd2 --wait --set replicaCount=3
```

## Configuration

The Helm chart generates a `config.json` and mounts it into the container at `/config.json`. The binary reads `./config.json` by default (override via `CFG_PATH`).

Minimal config example:

```json
{
  "log_path": "/var/log/pods",
  "state_file_path": "/tmp/logfowd2_state.json",
  "logging": { "log_level": "info", "log_format": "json" },
  "es": {
    "host": "http://elasticsearch-master",
    "port": 9200,
    "index_name": "logfowd",
    "flush_interval": 1000,
    "bulk_size": 500,
    "workers": 1
  }
}
```

See `config.json` for a complete example, and `helm/logfowd2/values.yaml` for Helm values.

Example Helm overrides:

```bash
helm upgrade -n logging logfowd helm/logfowd2 --wait \
  --set app.elasticsearch.host=http://elasticsearch-master \
  --set app.elasticsearch.bulk_size=500 \
  --set app.max_concurrent_file_readers=3
```

## Testing and development

Tests: 293 unit/integration tests (as of Feb 16, 2026), executed in CI.

```bash
make test
make dev_check
make ci
```

For local development, Rust `1.93.1` is pinned in `rust-toolchain.toml`.
