//! OTLP metrics push to Grafana Cloud — the Rust counterpart of the Go
//! fleet's pkg/telemetry. Gates off when OTEL_EXPORTER_OTLP_ENDPOINT is
//! unset (local runs, or the ESO secret not yet synced), like the Go fleet.
//!
//! Signals: a 5s recorder samples the playhead and pipeline running time
//! (running time ≈ wallclock is the realtime-health check the soaks used),
//! a tee-sink pad probe counts output frames and PTS gaps (true output fps
//! and visible-stall detection), and counters track clip spawns (every
//! boundary or redirect), clip errors, and NATS commands by verb. ponytail:
//! no per-boundary drift histogram yet — the spawn log lines carry what
//! drift analysis needs; add one if dashboards outgrow log-based analysis.

use std::sync::LazyLock;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use opentelemetry_otlp::{MetricExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tracing::{info, warn};

use crate::SharedPlayer;

/// NATS control-plane connection state, exported by the recorder as
/// `playout_nats_connected`. Flipped by the connect result and async_nats
/// connect/disconnect events (see `nats::connect`). Starts false: until the
/// first successful connect the control plane is down and playback commands
/// (find/goto/timewarp/skip) are silently dropped even though the corpus keeps
/// looping — the exact boot-race this metric exists to surface.
static NATS_CONNECTED: AtomicBool = AtomicBool::new(false);

/// Record whether the NATS control-plane connection is currently up. Called
/// from the nats module on connect (true) and disconnect (false).
pub fn set_nats_connected(connected: bool) {
    NATS_CONNECTED.store(connected, Ordering::Relaxed);
}

/// `service.platform` stamped onto every metric data point. Grafana Cloud's
/// OTLP gateway promotes a data-point attribute to a per-series Prometheus
/// label, whereas the same attribute on the *resource* only reaches
/// target_info — so the shared dashboards' `service_platform=~"$platform"`
/// filter matches series only when it's stamped here. The Go fleet does the
/// identical per-record stamp (pkg/instrumentation platformAttr). Set once by
/// `init`; empty on local/test runs so no attribute is attached.
static PLATFORM_ATTR: OnceLock<[KeyValue; 1]> = OnceLock::new();

/// Data-point attributes to stamp on every metric record — the platform, once
/// `init` has run, else nothing.
pub fn attrs() -> &'static [KeyValue] {
    PLATFORM_ATTR.get().map_or(&[], |a| a.as_slice())
}

/// `attrs()` plus one call-site attribute (e.g. a command verb).
pub fn attrs_with(extra: KeyValue) -> Vec<KeyValue> {
    let mut v = attrs().to_vec();
    v.push(extra);
    v
}

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

pub static OUTPUT_FRAMES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("playout")
        .u64_counter("playout_output_frames_total")
        .with_description("Video frames leaving the pipeline (buffers through the output tee); rate() is output fps")
        .build()
});

pub static OUTPUT_FRAME_GAPS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("playout")
        .u64_counter("playout_output_frame_gaps_total")
        .with_description("Output frames whose PTS jumped >1.5 frame intervals past the previous one (visible stalls/drops)")
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
pub fn init(platform: &str, deployment_env: &str) -> Option<SdkMeterProvider> {
    // Stamped onto every data point via attrs(); this is what becomes the
    // service_platform series label the dashboards filter on.
    let _ = PLATFORM_ATTR.set([KeyValue::new("service.platform", platform.to_string())]);
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
    // service.namespace=tripbot, deployment.environment. Grafana Cloud promotes
    // these standard resource attributes onto every metric series as the
    // service_namespace / deployment_environment Prometheus labels the shared
    // dashboards and alert rules key off. deployment.environment is the k8s
    // namespace (prod-1 / stage-1), matching the fleet's value. service.platform
    // is custom, so the gateway files it into target_info only — the series
    // label the `by (service_platform, ...)` queries need comes from the
    // per-record stamp in attrs(), not from here.
    let resource = Resource::builder()
        .with_service_name("playout")
        .with_attributes([
            KeyValue::new("service.version", crate::VERSION),
            KeyValue::new("service.namespace", "tripbot"),
            KeyValue::new("service.platform", platform.to_string()),
            KeyValue::new("deployment.environment", deployment_env.to_string()),
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
        let nats_connected = meter
            .u64_gauge("playout_nats_connected")
            .with_description(
                "1 while the NATS control-plane connection is up, 0 while down. A 0 means \
                 playback commands (find/goto/timewarp/skip) are being dropped even though the \
                 corpus still loops — catches the boot-race where playout starts before NATS.",
            )
            .build();
        loop {
            if let Some((_, pos)) = player.playhead() {
                playhead.record(pos, attrs());
            }
            if let Some(rt) = player.running_time() {
                running.record(rt.mseconds(), attrs());
            }
            nats_connected.record(NATS_CONNECTED.load(Ordering::Relaxed) as u64, attrs());
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}
