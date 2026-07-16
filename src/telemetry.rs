//! OTLP metrics push to Grafana Cloud — the Rust counterpart of the Go
//! fleet's pkg/telemetry. Gates off when OTEL_EXPORTER_OTLP_ENDPOINT is
//! unset (local runs, or the ESO secret not yet synced), like the Go fleet.
//!
//! Signals: a 5s recorder samples the playhead and pipeline running time
//! (running time ≈ wallclock is the realtime-health check the soaks used),
//! and counters track clip spawns (every boundary or redirect) and NATS
//! commands by verb. ponytail: no per-boundary drift histogram yet — the
//! spawn log lines carry what drift analysis needs; add one if dashboards
//! outgrow log-based analysis.

use std::sync::LazyLock;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use opentelemetry_otlp::{MetricExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tracing::{info, warn};

use crate::SharedPlayer;

pub static CLIP_SPAWNS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("playout")
        .u64_counter("playout_clip_spawns_total")
        .with_description("Clips spliced into concat (boundaries + redirect commands)")
        .build()
});

pub static CLIP_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("playout")
        .u64_counter("playout_clip_errors_total")
        .with_description("Clip bins torn down after a decode/negotiation error")
        .build()
});

pub static COMMANDS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("playout")
        .u64_counter("playout_commands_total")
        .with_description("NATS playback commands dispatched, by verb")
        .build()
});

fn parse_headers(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

/// Bring up the OTLP meter provider, or None when no endpoint is configured.
/// The caller holds the provider and shuts it down at exit to flush.
pub fn init(platform: &str, env: &str) -> Option<SdkMeterProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    // Endpoint + auth headers passed explicitly: the env-var wiring is what
    // the grafana-cloud-otlp secret ships, same contract as the Go fleet.
    let headers = parse_headers(&std::env::var("OTEL_EXPORTER_OTLP_HEADERS").unwrap_or_default());
    let exporter = match MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/metrics", endpoint.trim_end_matches('/')))
        .with_headers(headers.into_iter().collect())
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            warn!(err = %e, "OTLP metric exporter failed to build; telemetry disabled");
            return None;
        }
    };
    // Fleet label convention (Go pkg/telemetry via OTEL_RESOURCE_ATTRIBUTES):
    // service.namespace=tripbot, service.platform, deployment.environment. These
    // become the service_namespace / service_platform / deployment_environment
    // Prometheus labels the shared dashboards and the
    // `by (service_platform, deployment_environment)` alert rules key off, so
    // playout's series line up with the rest of the fleet.
    let resource = Resource::builder()
        .with_service_name("playout")
        .with_attributes([
            KeyValue::new("service.version", crate::VERSION),
            KeyValue::new("service.namespace", "tripbot"),
            KeyValue::new("service.platform", platform.to_string()),
            KeyValue::new("deployment.environment", env.to_string()),
        ])
        .build();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build();
    global::set_meter_provider(provider.clone());
    info!(endpoint = %endpoint, "OTLP metrics export enabled");
    Some(provider)
}

/// Sample playhead + pipeline running time every 5s (matches the lastplayed
/// ticker cadence). Running time advancing at ~wallclock rate is the
/// realtime-health signal; a frozen playhead is the wedge tell.
pub fn spawn_recorder(player: SharedPlayer) {
    tokio::spawn(async move {
        let meter = global::meter("playout");
        let playhead = meter
            .i64_gauge("playout_playhead_position_ms")
            .with_description("Position in the active clip")
            .build();
        let running = meter
            .u64_gauge("playout_pipeline_running_time_ms")
            .with_description("Pipeline running time (advances ~1s/s when realtime holds)")
            .build();
        loop {
            if let Some((_, pos)) = player.playhead() {
                playhead.record(pos, &[]);
            }
            if let Some(rt) = player.running_time() {
                running.record(rt.mseconds(), &[]);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}
