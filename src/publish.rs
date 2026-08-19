//! The tee's publish slot: exactly one branch occupies it — the RTSP publish
//! when this process holds the MediaMTX path, a clock-paced fakesink when it
//! can't (relay parked, another publisher on the path, the sink errored).
//! Either way the pipeline stays PLAYING, so the pod reports ready and the
//! playhead the console map reads keeps advancing.
//!
//! The acquirer task polls the path with an RTSP DESCRIBE and swaps the
//! publish in the moment the path is free — never before, so this process
//! never kicks a live publisher off it. Rolling deploys ride this: the new
//! pod goes ready on the fakesink while the old pod still publishes, k8s
//! then SIGTERMs the old pod, its teardown frees the path, and the next poll
//! acquires it — a handoff of about one poll interval, with OBS's RTSP
//! session to MediaMTX untouched throughout.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::{telemetry, watchdog};

/// Poll cadence while waiting for the path to free. A deploy handoff is
/// old-pod TEARDOWN → next poll → attach, so this is the gap's floor; a
/// DESCRIBE against the in-cluster relay costs well under a millisecond.
const RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The encode branch ends in an RTSP RECORD publish to MediaMTX; consumers
/// attach to MediaMTX, so this end can restart without them noticing.
///
/// ENCODER=passthrough publishes the corpus clips' compressed H.264 without
/// re-encoding — the airing corpus is transcoded to one uniform spec
/// (identical params, IDR-leading closed GOPs), which is what makes splicing
/// compressed streams safe. h264parse re-sends SPS/PPS at every IDR so each
/// splice and every late joiner resyncs.
fn make_encode_branch(encoder_name: &str, rtsp_url: &str) -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let parse = gst::ElementFactory::make("h264parse").build()?;
    // Re-send SPS/PPS with every IDR so late joiners always sync.
    parse.set_property("config-interval", -1i32);
    let sink = gst::ElementFactory::make("rtspclientsink").build()?;
    sink.set_property("location", rtsp_url);

    if encoder_name == "passthrough" {
        return Ok(vec![queue, parse, sink]);
    }

    let encoder = gst::ElementFactory::make(encoder_name)
        .build()
        .with_context(|| format!("creating encoder {encoder_name}"))?;
    if encoder_name == "x264enc" {
        encoder.set_property("bitrate", 8000u32);
        // 2s GOP at 60fps, matching the corpus spec the stream runs today.
        encoder.set_property("key-int-max", 120u32);
        encoder.set_property_from_str("speed-preset", "veryfast");
    }
    Ok(vec![queue, encoder, parse, sink])
}

/// Map-only sink: swallow the stream at real time and broadcast nothing.
/// `sync=true` paces the sink to the buffer clock, so the pipeline still
/// reaches PLAYING and advances the running-time the NATS playhead reports —
/// which is what drives the console map — while nothing leaves the pod.
/// fakesink is format-agnostic, so it takes the passthrough H.264 or the
/// decoded path equally.
fn make_fakesink_branch() -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let sink = gst::ElementFactory::make("fakesink").build()?;
    sink.set_property("sync", true);
    Ok(vec![queue, sink])
}

pub(crate) struct Output {
    pipeline: gst::Pipeline,
    tee: gst::Element,
    encoder_name: String,
    rtsp_url: String,
    /// The branch currently in the slot; `on_error`'s ancestry check reads it.
    branch: Mutex<Vec<gst::Element>>,
    /// True while the RTSP publish branch holds the slot. Shared with the
    /// DESCRIBE watchdog, which only alarms while a publish should be live.
    publishing: Arc<AtomicBool>,
    /// Wakes the acquirer after a start or drop into map-only.
    wake: Notify,
}

impl Output {
    pub(crate) fn new(
        pipeline: gst::Pipeline,
        tee: gst::Element,
        encoder_name: String,
        rtsp_url: String,
    ) -> Result<Arc<Self>> {
        // Surface a bad ENCODER (missing plugin) at boot, not at the first
        // swap hours later.
        drop(make_encode_branch(&encoder_name, &rtsp_url)?);
        Ok(Arc::new(Self {
            pipeline,
            tee,
            encoder_name,
            rtsp_url,
            branch: Mutex::new(Vec::new()),
            publishing: Arc::new(AtomicBool::new(false)),
            wake: Notify::new(),
        }))
    }

    /// The flag the DESCRIBE watchdog gates its probes on.
    pub(crate) fn publishing_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.publishing)
    }

    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Remove the slot's current branch, if any. Release the tee pad before
    /// stopping the branch (the release-pad-before-Null lesson from clip
    /// teardown); the tee's allow-not-linked keeps the momentarily branchless
    /// slot from erroring the pipeline.
    fn detach(&self) {
        let elements = std::mem::take(&mut *self.branch.lock().unwrap());
        let Some(first) = elements.first() else {
            return;
        };
        if let Some(peer) = first.static_pad("sink").and_then(|p| p.peer()) {
            self.tee.release_request_pad(&peer);
        }
        for e in elements.iter().rev() {
            e.set_state(gst::State::Null).ok();
            self.pipeline.remove(e).ok();
        }
    }

    /// Put `elements` in the slot in place of whatever holds it. Sticky
    /// events (stream-start, caps, segment) replay on the fresh tee pad, so
    /// a branch attached mid-stream negotiates like one attached at boot.
    fn swap(&self, elements: Vec<gst::Element>) -> Result<()> {
        self.detach();
        let refs: Vec<&gst::Element> = elements.iter().collect();
        self.pipeline.add_many(&refs)?;
        gst::Element::link_many(&refs)?;
        self.tee.link(&elements[0])?;
        // Publish the branch before starting it: a sink that errors during
        // its state change must already be classifiable as the slot's.
        *self.branch.lock().unwrap() = elements.clone();
        for e in elements.iter().rev() {
            e.sync_state_with_parent().context("starting slot branch")?;
        }
        Ok(())
    }

    pub(crate) fn attach_publish(&self) -> Result<()> {
        self.swap(make_encode_branch(&self.encoder_name, &self.rtsp_url)?)?;
        self.publishing.store(true, Ordering::SeqCst);
        info!(url = %self.rtsp_url, "publish attached");
        Ok(())
    }

    pub(crate) fn attach_fakesink(&self) -> Result<()> {
        self.publishing.store(false, Ordering::SeqCst);
        self.swap(make_fakesink_branch()?)
    }

    /// A bus error sourced under the slot's branch (a rejected or kicked RTSP
    /// publish): absorb it by dropping to the fakesink and waking the
    /// acquirer. Returns false when the error is not the slot's to absorb —
    /// or when even the fakesink can't attach, which leaves nothing to hold
    /// the pipeline up.
    pub(crate) fn on_error(&self, src: &gst::Object) -> bool {
        let of_slot = self
            .branch
            .lock()
            .unwrap()
            .iter()
            .any(|e| src == e.upcast_ref::<gst::Object>() || src.has_as_ancestor(e));
        if !of_slot {
            return false;
        }
        telemetry::PUBLISH_ERRORS.add(1, telemetry::attrs());
        if self.attach_fakesink().is_err() {
            return false;
        }
        self.wake();
        true
    }

    /// Long-lived: parked until woken, then polls until the publish is back
    /// in the slot. Woken by a map-only boot and by `on_error`.
    pub(crate) async fn run_acquirer(self: Arc<Self>) {
        loop {
            self.wake.notified().await;
            info!(
                url = %self.rtsp_url,
                poll_ms = RETRY_INTERVAL.as_millis() as u64,
                "waiting to acquire the publish path"
            );
            while !self.publishing.load(Ordering::SeqCst) {
                tokio::time::sleep(RETRY_INTERVAL).await;
                if !watchdog::path_free(&self.rtsp_url).await {
                    continue;
                }
                if let Err(e) = self.attach_publish() {
                    error!(err = %e, "attaching the publish failed; staying map-only");
                    if self.attach_fakesink().is_err() {
                        return;
                    }
                }
            }
        }
    }
}
