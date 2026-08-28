//! The RTSP publish as a detachable branch. The tee always carries a
//! clock-paced fakesink — it paces the pipeline whether or not anything is
//! broadcast, so the pod reports ready and the playhead the console map
//! reads keeps advancing — and the publish branch attaches beside it only
//! while this process holds the MediaMTX path. Nothing load-bearing is ever
//! detached: a publish that can't exist (relay parked, another publisher on
//! the path, a rejected or kicked session) is simply absent.
//!
//! The acquirer task polls the path with an RTSP DESCRIBE and attaches the
//! publish the moment the path is free — never before, so this process never
//! kicks a live publisher off it. Rolling deploys ride this: the new pod
//! goes ready with no publish branch while the old pod still publishes, k8s
//! then SIGTERMs the old pod, its teardown frees the path, and the next poll
//! acquires it. OBS is never repointed — it stays pointed at the same relay
//! across the swap — but its session does not survive it: MediaMTX destroys
//! every reader when the publisher tears down, and OBS reconnects on its own
//! a few hundred milliseconds later.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

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
fn make_encode_branch(
    encoder_name: &str,
    rtsp_url: &str,
    gap_start: Option<Instant>,
) -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    // Never backpressure the tee: rtspclientsink consumes nothing until its
    // RTSP session establishes (and never again after the server closes it),
    // and a full non-leaky queue here would block the tee and freeze the
    // whole pipeline — map, playhead, and all. Dropped buffers only ever
    // precede a working session; readers resync at the next IDR regardless.
    queue.set_property_from_str("leaky", "downstream");
    // The leak bound must clear rtspclientsink's steady-state occupancy: its
    // internal rtpbin holds `latency` (2s default) of stream even on a healthy
    // session, so a cap below that sheds frames continuously — and every shed
    // delta frame breaks readers' reference chains until the next IDR, which
    // viewers see as constant artifacts. (The queue's default 1s time cap did
    // exactly that.) 5s = that occupancy + headroom; time-bound only, since
    // the default buffer/byte caps (200 buffers ≈ 3.3s at 60fps) would
    // otherwise bind first and restate the same limit less directly.
    queue.set_property("max-size-time", 5_000_000_000u64);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-bytes", 0u32);
    let parse = gst::ElementFactory::make("h264parse").build()?;
    // Re-send SPS/PPS with every IDR so late joiners always sync.
    parse.set_property("config-interval", -1i32);
    // Hold the branch dark until a keyframe: attached mid-GOP, the sink
    // would otherwise preroll on a headerless delta AU and ANNOUNCE an SDP
    // without sprop-parameter-sets, which MediaMTX rejects (400). The first
    // keyframe through h264parse carries SPS/PPS in-band (config-interval
    // above), the probe removes itself, and the corpus GOP bounds the wait
    // to ~1-2s. Boot attaches start at a keyframe and pass straight through.
    // That first keyframe is the moment the path carries video again, so it
    // is where `playout_publish_gap_seconds` stops its clock: the IDR wait
    // is the bulk of a handoff's on-air gap and would otherwise go uncounted.
    parse
        .static_pad("src")
        .expect("h264parse always has a static src pad")
        .add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            if let Some(gst::PadProbeData::Buffer(ref buf)) = info.data
                && buf.flags().contains(gst::BufferFlags::DELTA_UNIT)
            {
                return gst::PadProbeReturn::Drop;
            }
            if let Some(start) = gap_start {
                let gap_s = start.elapsed().as_secs_f64();
                telemetry::PUBLISH_GAP.record(gap_s, telemetry::attrs());
                info!(gap_s, "publish live: first keyframe on the wire");
            }
            gst::PadProbeReturn::Remove
        });
    let sink = gst::ElementFactory::make("rtspclientsink").build()?;
    sink.set_property("location", rtsp_url);
    // Interleave RTP over the RTSP connection instead of a side UDP flow.
    // rtspclientsink offers UDP first and MediaMTX accepts it, and that hop
    // drops datagrams whenever the node is busy: an IDR is a burst of ~140
    // back-to-back packets, so a briefly-descheduled reader overflows the
    // receive buffer. MediaMTX discards any frame with a packet missing, which
    // readers decode as broken references — visible artifacts until the next
    // keyframe — while every other signal stays clean, including this
    // pipeline's own frame-gap counter. TCP is flow-controlled, so the frames
    // either arrive or block; the hop is pod-to-pod on one node, so the
    // latency this trades for is nil.
    sink.set_property_from_str("protocols", "tcp");

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

/// The permanent pacing sink: swallow the stream at real time. `sync=true`
/// paces the sink to the buffer clock, so the pipeline reaches PLAYING and
/// advances the running-time the NATS playhead reports — which is what
/// drives the console map — even with nothing broadcast. fakesink is
/// format-agnostic, so it takes the passthrough H.264 or the decoded path
/// equally.
fn make_fakesink_branch() -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let sink = gst::ElementFactory::make("fakesink").build()?;
    sink.set_property("sync", true);
    // async=false: this sink must not gate preroll. rtspclientsink completes
    // its PAUSED transition without data (it only announces after PLAYING),
    // so before this branch existed the pipeline hit PLAYING immediately and
    // every clip mechanism — EOS boundaries, corrupt-clip recovery — ran
    // while streaming. A data-gated sink here holds the pipeline in PAUSED
    // until frames flow, and concat pad churn during that window (a corrupt
    // first clip, a short clip draining to EOS) wedges preroll for good.
    sink.set_property("async", false);
    Ok(vec![queue, sink])
}

pub(crate) struct Output {
    pipeline: gst::Pipeline,
    tee: gst::Element,
    encoder_name: String,
    rtsp_url: String,
    /// The attached publish branch; empty while not publishing. `on_error`'s
    /// ancestry check reads it, so it is assigned before the branch starts.
    branch: Mutex<Vec<gst::Element>>,
    /// True while the publish branch is attached. Shared with the DESCRIBE
    /// watchdog, which only alarms while a publish should be live.
    publishing: Arc<AtomicBool>,
    /// Wakes the acquirer after a start or drop into map-only.
    wake: Notify,
    /// When the current publish gap opened — set on detach (error recovery)
    /// or on the acquirer's first free-path sighting (deploy handoffs), and
    /// taken on attach; the branch's keyframe probe records
    /// `playout_publish_gap_seconds` when the first keyframe passes. None
    /// while publishing and across a cold boot (no prior publisher, no gap
    /// to measure).
    gap_start: Mutex<Option<Instant>>,
}

impl Output {
    /// Wires the permanent fakesink into the tee and validates the encode
    /// branch is constructible — a bad ENCODER (missing plugin) should fail
    /// boot, not the first attach hours later.
    pub(crate) fn new(
        pipeline: gst::Pipeline,
        tee: gst::Element,
        encoder_name: String,
        rtsp_url: String,
    ) -> Result<Arc<Self>> {
        drop(make_encode_branch(&encoder_name, &rtsp_url, None)?);
        let fakesink = make_fakesink_branch()?;
        let refs: Vec<&gst::Element> = fakesink.iter().collect();
        pipeline.add_many(&refs)?;
        gst::Element::link_many(&refs)?;
        tee.link(&fakesink[0])?;
        Ok(Arc::new(Self {
            pipeline,
            tee,
            encoder_name,
            rtsp_url,
            branch: Mutex::new(Vec::new()),
            publishing: Arc::new(AtomicBool::new(false)),
            wake: Notify::new(),
            gap_start: Mutex::new(None),
        }))
    }

    /// The flag the DESCRIBE watchdog gates its probes on.
    pub(crate) fn publishing_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.publishing)
    }

    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Attach the publish branch beside the fakesink. Sticky events
    /// (stream-start, caps, segment) replay on the fresh tee pad, so a
    /// branch attached mid-stream negotiates like one attached at boot.
    pub(crate) fn attach_publish(&self) -> Result<()> {
        let mut branch = self.branch.lock().unwrap();
        if !branch.is_empty() {
            return Ok(());
        }
        // The gap clock rides into the branch's keyframe probe; a failed
        // attach hands it back so the retry keeps the original start.
        let gap_start = self.gap_start.lock().unwrap().take();
        let mut attach = || -> Result<()> {
            let elements = make_encode_branch(&self.encoder_name, &self.rtsp_url, gap_start)?;
            let refs: Vec<&gst::Element> = elements.iter().collect();
            self.pipeline.add_many(&refs)?;
            gst::Element::link_many(&refs)?;
            self.tee.link(&elements[0])?;
            // Publish the branch before starting it: a sink that errors during
            // its state change must already be classifiable as the branch's.
            *branch = elements.clone();
            for e in elements.iter().rev() {
                if let Err(e) = e.sync_state_with_parent() {
                    Self::detach_locked(&self.pipeline, &self.tee, &mut branch);
                    return Err(e).context("starting publish branch");
                }
            }
            Ok(())
        };
        if let Err(e) = attach() {
            *self.gap_start.lock().unwrap() = gap_start;
            return Err(e);
        }
        self.publishing.store(true, Ordering::SeqCst);
        info!(url = %self.rtsp_url, "publish attached; waiting for a keyframe");
        Ok(())
    }

    /// Remove the publish branch, if attached. Release the tee pad before
    /// stopping the branch (the release-pad-before-Null lesson from clip
    /// teardown); the fakesink keeps the tee fed and the pipeline paced
    /// throughout.
    fn detach_publish(&self) {
        let mut branch = self.branch.lock().unwrap();
        Self::detach_locked(&self.pipeline, &self.tee, &mut branch);
        self.publishing.store(false, Ordering::SeqCst);
        *self.gap_start.lock().unwrap() = Some(Instant::now());
    }

    fn detach_locked(
        pipeline: &gst::Pipeline,
        tee: &gst::Element,
        branch: &mut MutexGuard<Vec<gst::Element>>,
    ) {
        let elements = std::mem::take(&mut **branch);
        let Some(first) = elements.first() else {
            return;
        };
        if let Some(peer) = first.static_pad("sink").and_then(|p| p.peer()) {
            tee.release_request_pad(&peer);
        }
        for e in elements.iter().rev() {
            e.set_state(gst::State::Null).ok();
            pipeline.remove(e).ok();
        }
    }

    /// A bus error sourced under the publish branch (a rejected or kicked
    /// RTSP publish): absorb it by dropping the branch and waking the
    /// acquirer. Returns false when the error is not the branch's to absorb.
    pub(crate) fn on_error(&self, src: &gst::Object) -> bool {
        let of_branch = self
            .branch
            .lock()
            .unwrap()
            .iter()
            .any(|e| src == e.upcast_ref::<gst::Object>() || src.has_as_ancestor(e));
        if !of_branch {
            return false;
        }
        telemetry::PUBLISH_ERRORS.add(1, telemetry::attrs());
        self.detach_publish();
        self.wake();
        true
    }

    /// Long-lived: parked until woken, then polls until the publish branch
    /// is attached. Woken by a map-only boot and by `on_error`.
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
                // The gap clock for a handoff: the old publisher's exit is
                // invisible from here, so the first free-path sighting is the
                // earliest measurable start. get_or_insert keeps the earlier
                // detach timestamp on the error-recovery path.
                self.gap_start
                    .lock()
                    .unwrap()
                    .get_or_insert_with(Instant::now);
                if let Err(e) = self.attach_publish() {
                    error!(err = %e, "attaching the publish failed; retrying");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The publish must not negotiate UDP. rtspclientsink offers UDP ahead of
    /// TCP by default and MediaMTX accepts it, so dropping the property is
    /// silent everywhere else: the pipeline builds, the session establishes,
    /// and the handoff tests pass while the stream quietly loses frames to a
    /// busy node.
    #[test]
    fn publish_branch_forces_tcp_transport() {
        gst::init().unwrap();
        let branch =
            make_encode_branch("passthrough", "rtsp://localhost:8554/dashcam", None).unwrap();
        let sink = branch.last().expect("the branch ends in the sink");
        // GStreamer serializes a flags value to its nick; TCP alone means the
        // UDP flags the default carries are gone.
        let protocols = sink.property_value("protocols").serialize().unwrap();
        assert_eq!(protocols.as_str(), "GST_RTSP_LOWER_TRANS_TCP");
    }
}
