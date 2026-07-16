use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use gst::glib;
use gst::prelude::*;
use gstreamer as gst;
use gstreamer_pbutils as gst_pbutils;
use tracing::{error, info, warn};

use crate::telemetry;

/// A live decode bin plus the bookkeeping a playback command needs: which
/// playlist entry it is, the concat pad it feeds, the offset it started at,
/// and the output running time it went active (for the playhead position).
pub(crate) struct Clip {
    bin: gst::Element,
    /// concat sink pad, set once the decode bin exposes its src pad.
    pad: Option<gst::Pad>,
    index: usize,
    /// Seek offset this clip started at, ms (0 = top of clip).
    offset_ms: i64,
    /// Pipeline running time when this clip became active; None until then.
    start_rt: Option<gst::ClockTime>,
}

/// Everything the clip-spawning path needs to hang on to. Clip bins come and
/// go at every boundary; the pipeline, concat, and playlist live forever.
pub(crate) struct Player {
    pub(crate) pipeline: gst::Pipeline,
    pub(crate) concat: gst::Element,
    /// Immutable playlist, sorted. Commands index into this.
    pub(crate) files: Vec<PathBuf>,
    /// Live clip bins in play order: `[active, prerolled-next]`.
    pub(crate) clips: Mutex<Vec<Clip>>,
    /// Clip bins stop at parsed H.264 instead of decoding, and seeks snap to
    /// keyframes (a compressed stream can't start mid-GOP).
    pub(crate) passthrough: bool,
    /// Consecutive clip-bin failures absorbed without a clip reaching EOS.
    /// Once it exceeds the playlist length the whole corpus is bad and the
    /// error goes fatal instead of spinning through recovery forever.
    pub(crate) recoveries: AtomicUsize,
    /// Clip durations (ms) by playlist index, discovered on first use by a
    /// seek walk and cached for the process lifetime (the corpus is immutable
    /// while running, like `files`).
    pub(crate) durations: Mutex<HashMap<usize, i64>>,
}

pub(crate) type SharedPlayer = Arc<Player>;

/// Playlist index `n` clips forward of `active`, wrapping. n<1 is treated as 1.
fn skip_index(active: usize, n: i32, len: usize) -> usize {
    (active + (n.max(1) as usize)) % len
}

/// Playlist index `n` clips back of `active`, wrapping. n<1 is treated as 1.
fn back_index(active: usize, n: i32, len: usize) -> usize {
    let n = (n.max(1) as usize) % len;
    (active + len - n) % len
}

/// Where a signed playhead move lands: walk the playlist from `pos_ms` into
/// the `active` clip by `delta_ms`, wrapping in both directions, and return
/// the landing (index, offset_ms). Any timescale works — after one full lap
/// the walk has seen every clip's duration, so it reduces what's left modulo
/// the corpus length and finishes within one more (now fully cached) lap.
/// `duration_ms` answers per-clip durations; a clip whose duration it can't
/// give can't be positioned within, so a forward walk lands at the top of
/// the clip after it and a backward walk at its own top.
fn seek_walk(
    active: usize,
    pos_ms: i64,
    delta_ms: i64,
    len: usize,
    mut duration_ms: impl FnMut(usize) -> Option<i64>,
) -> (usize, i64) {
    let mut index = active;
    let mut offset = pos_ms.max(0).saturating_add(delta_ms);
    let mut steps = 0;
    let mut lap_total: i64 = 0;
    let mut modded = false;
    while offset < 0 {
        if steps >= len {
            if modded {
                return (index, 0);
            }
            offset = offset.rem_euclid(lap_total);
            steps = 0;
            modded = true;
            break;
        }
        steps += 1;
        index = (index + len - 1) % len;
        match duration_ms(index) {
            Some(d) if d > 0 => {
                offset += d;
                lap_total += d;
            }
            _ => return (index, 0),
        }
    }
    loop {
        match duration_ms(index) {
            Some(d) if d > 0 && offset < d => return (index, offset),
            Some(d) if d > 0 => {
                if steps >= len {
                    if modded {
                        return (index, 0);
                    }
                    offset %= lap_total;
                    steps = 0;
                    modded = true;
                    continue;
                }
                steps += 1;
                offset -= d;
                index = (index + 1) % len;
                lap_total += d;
            }
            _ => {
                if offset == 0 {
                    return (index, 0);
                }
                return ((index + 1) % len, 0);
            }
        }
    }
}

/// Keeps a resume/play.at seek from landing in a clip's last moments — the
/// clip would EOS almost immediately after the splice. An unknown duration
/// errs toward seeking (matches vlc-server's tail guard).
const SEEK_TAIL_GUARD_MS: i64 = 2000;

fn should_seek_to(offset_ms: i64, duration_ms: Option<i64>) -> bool {
    if offset_ms <= 0 {
        return false;
    }
    match duration_ms {
        Some(d) if d > 0 => offset_ms < d - SEEK_TAIL_GUARD_MS,
        _ => true,
    }
}

impl Player {
    fn uri_at(&self, index: usize) -> String {
        format!("file://{}", self.files[index].display())
    }

    fn basename_at(&self, index: usize) -> String {
        self.files[index]
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_owned()
    }

    /// Playlist index of the clip whose basename matches `name`, if any.
    pub(crate) fn find(&self, name: &str) -> Option<usize> {
        self.files
            .iter()
            .position(|p| p.file_name().and_then(|n| n.to_str()) == Some(name))
    }

    // ponytail: stdlib RNG via RandomState's seeded hasher — good enough to
    // pick a clip, no `rand` crate. Upgrade to `rand` only if distribution
    // quality ever matters here (it won't for "play a random dashcam clip").
    pub(crate) fn random_index(&self) -> usize {
        let r = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        (r % self.files.len() as u64) as usize
    }

    fn active_index(&self) -> usize {
        self.clips
            .lock()
            .unwrap()
            .first()
            .map(|c| c.index)
            .unwrap_or(0)
    }

    /// Add a decode bin for playlist `index` and link it into concat. The new
    /// clip prerolls immediately but its buffers block in concat until every
    /// earlier pad has finished — that blocking is what makes the splice
    /// gapless. On EOS the bin tears itself down and spawns the successor, so
    /// the pipeline always holds the active clip plus the prerolled next.
    /// `offset_ms` seeks the clip before concat reaches it (play.at / resume).
    pub(crate) fn spawn(self: &Arc<Self>, index: usize, offset_ms: i64) {
        let uri = self.uri_at(index);
        info!(index, uri = %uri, offset_ms, "spawning clip");
        telemetry::CLIP_SPAWNS.add(1, &[]);
        let decode = gst::ElementFactory::make("uridecodebin3")
            .property("uri", &uri)
            .build()
            .expect("creating uridecodebin3");

        // Passthrough: stop at parsed H.264 — decodebin3 emits the demuxed,
        // parsed stream and never builds a decoder. Relies on the corpus
        // contract (every clip the same uniform spec); a non-H.264 clip fails
        // to negotiate and errors loudly rather than silently re-encoding.
        if self.passthrough {
            decode.set_property("caps", gst::Caps::builder("video/x-h264").build());
        }

        // Video only: audio is composited downstream in OBS, and deselecting
        // here keeps clips with/without audio tracks topology-identical.
        decode.connect("select-stream", false, |args| {
            let stream = args[2].get::<gst::Stream>().unwrap();
            let selected = stream.stream_type().contains(gst::StreamType::VIDEO);
            Some((selected as i32).to_value())
        });

        // Request the concat pad here, not in pad-added: concat plays its
        // sink pads in request order, while pad-added fires in
        // preroll-completion order. Letting preroll order pick the pad order
        // plays clips out of sequence — and on resume the seeked clip
        // prerolls last, so its pad lands behind the "next" clip and the
        // pipeline EOSes at the first boundary.
        let sinkpad = self
            .concat
            .request_pad_simple("sink_%u")
            .expect("requesting concat pad");

        // EOS on this pad = clip finished and concat has moved on: tear
        // down the finished bin off the streaming thread and top up.
        let player = Arc::clone(self);
        let decode_for_eos = decode.clone();
        sinkpad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |pad, info| {
            let Some(ev) = info.event() else {
                return gst::PadProbeReturn::Ok;
            };
            if ev.type_() != gst::EventType::Eos {
                return gst::PadProbeReturn::Ok;
            }
            let player = Arc::clone(&player);
            let decode = decode_for_eos.clone();
            let pad = pad.clone();
            glib::idle_add_once(move || player.on_clip_eos(&decode, &pad));
            // Drop the EOS: concat handles pad switching itself, and the
            // pipeline-level EOS must never fire (24/7 stream).
            gst::PadProbeReturn::Drop
        });

        let player = Arc::clone(self);
        let concat_pad = sinkpad.clone();
        decode.connect_pad_added(move |decode, pad| {
            // No seek requested: link straight into concat. The clip's buffers
            // block there until every earlier pad finishes.
            if offset_ms <= 0 {
                pad.link(&concat_pad).expect("linking clip into concat");
                return;
            }

            // Seek path (play.at / resume). Constraints, each one learned the
            // hard way: a flush seek issued from a streaming thread (this
            // callback, or a pad probe) deadlocks waiting for the very thread
            // issuing it; the bin must negotiate while LINKED into concat or
            // the decoder picks caps against an unlinked pad and errors
            // not-negotiated; the seek only takes once the bin is streaming
            // end-to-end (first decoded buffer) — issued earlier it is
            // accepted and silently swallowed; and nothing pre-seek — the
            // flush, buffers, or the pre-seek segment, which would rewind
            // downstream running time and fast-forward the clip — may reach
            // concat's shared chain. So: link immediately, and hold a probe
            // that DROPS events (they stay sticky on the pad, and the
            // post-seek ones re-deliver once the probe is gone — concat
            // never sees the pre-seek segment) and BLOCKS the first buffer,
            // which proves the bin fully up; then seek from the main loop
            // and unblock. Concat's first sight of this pad is the post-seek
            // segment and buffers to match — indistinguishable from a clip
            // that begins at the offset.
            let player = Arc::clone(&player);
            let decode = decode.clone();
            let pad_for_seek = pad.clone();
            let probes = Arc::new(Mutex::new(Vec::new()));
            let probes_for_seek = Arc::clone(&probes);
            let scheduled = std::sync::atomic::AtomicBool::new(false);
            // Flush containment must be its own NON-blocking probe: flush
            // events bypass blocking probes entirely (callback included), so
            // the data probe below never even sees them.
            let flush_probe = pad
                .add_probe(gst::PadProbeType::EVENT_FLUSH, |_, _| {
                    gst::PadProbeReturn::Drop
                })
                .expect("adding flush-drop probe");
            let data_probe = pad
                .add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, move |_, info| {
                    if info.event().is_some() {
                        return gst::PadProbeReturn::Drop;
                    }
                    if scheduled.swap(true, Ordering::SeqCst) {
                        return gst::PadProbeReturn::Ok;
                    }
                    let player = Arc::clone(&player);
                    let decode = decode.clone();
                    let pad = pad_for_seek.clone();
                    let probes = Arc::clone(&probes_for_seek);
                    glib::idle_add_once(move || {
                        player.finish_seek(decode, pad, probes, offset_ms);
                    });
                    gst::PadProbeReturn::Ok
                })
                .expect("blocking clip pad");
            *probes.lock().unwrap() = vec![data_probe, flush_probe];
            pad.link(&concat_pad).expect("linking clip into concat");
        });

        // Push the bookkeeping entry before the bin can start prerolling, so
        // pad-added always finds it.
        self.clips.lock().unwrap().push(Clip {
            bin: decode.clone(),
            pad: Some(sinkpad),
            index,
            offset_ms,
            start_rt: None,
        });
        self.pipeline.add(&decode).expect("adding clip bin");
        decode.sync_state_with_parent().expect("starting clip bin");
    }

    /// Complete a pending clip seek from the main loop, then unblock the
    /// pad's probe so data flows into concat. Runs once the first decoded
    /// buffer reaches the pad — the only point where a flush seek on the bin
    /// reliably takes (any earlier and uridecodebin3 accepts but swallows it).
    fn finish_seek(
        self: &Arc<Self>,
        decode: gst::Element,
        pad: gst::Pad,
        probes: Arc<Mutex<Vec<gst::PadProbeId>>>,
        offset_ms: i64,
    ) {
        let mut offset_ms = offset_ms;
        let duration_ms = decode
            .query_duration::<gst::ClockTime>()
            .map(|d| d.mseconds() as i64);
        // Tail guard and refusal both fall back to top-of-clip, like
        // vlc-server.
        if !should_seek_to(offset_ms, duration_ms) {
            info!(offset_ms, "seek lands at the clip tail; starting at top");
            offset_ms = 0;
        }
        if offset_ms > 0 {
            let pos = gst::ClockTime::from_mseconds(offset_ms as u64);
            // Passthrough can't decode-trim to an exact frame: snap to the
            // keyframe at/before the target instead (≤ one GOP early, so the
            // playhead the offset seeds runs up to 2s ahead of true position
            // at the corpus's 2s GOP — fine for resume).
            let flags = if self.passthrough {
                gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT | gst::SeekFlags::SNAP_BEFORE
            } else {
                gst::SeekFlags::FLUSH | gst::SeekFlags::ACCURATE
            };
            if decode.seek_simple(flags, pos).is_err() {
                warn!(offset_ms, "seek refused; starting clip at top");
                offset_ms = 0;
            }
        }
        for id in probes.lock().unwrap().drain(..) {
            pad.remove_probe(id);
        }
        info!(offset_ms, "clip seeked and unblocked");

        // Record the offset the clip actually starts at (the guards above may
        // have demoted a requested offset to top-of-clip).
        if let Some(clip) = self
            .clips
            .lock()
            .unwrap()
            .iter_mut()
            .find(|c| c.bin == decode)
        {
            clip.offset_ms = offset_ms;
        }
    }

    /// Natural boundary: the finished bin's EOS reached concat, which has
    /// already switched to the prerolled clip. Tear the finished bin down,
    /// stamp the promoted clip active, and preroll its sequential successor.
    fn on_clip_eos(self: &Arc<Self>, decode: &gst::Element, pad: &gst::Pad) {
        self.recoveries.store(0, Ordering::SeqCst);
        self.clips.lock().unwrap().retain(|c| &c.bin != decode);
        decode.set_state(gst::State::Null).ok();
        self.pipeline.remove(decode).ok();
        self.concat.release_request_pad(pad);
        self.mark_active();
        let next = self
            .clips
            .lock()
            .unwrap()
            .first()
            .map(|c| (c.index + 1) % self.files.len());
        if let Some(next) = next {
            self.spawn(next, 0);
        }
    }

    /// A clip bin posted an error (corrupt file, or caps that won't negotiate
    /// under passthrough): the per-clip analogue of vlc-server rolling past a
    /// bad file. Tear the failed bin down and splice in the clip after it, so
    /// a garbage `.mp4` mid-corpus — or a resume that lands on one — costs one
    /// clip, not the pipeline. Returns false when the error is not a clip's
    /// to absorb: encoder, sink, and pipeline errors stay fatal.
    pub(crate) fn on_clip_error(self: &Arc<Self>, src: &gst::Object) -> bool {
        // A torn-down bin's already-queued messages can still arrive; anything
        // no longer under the pipeline is an echo of a handled failure.
        if !src.has_as_ancestor(&self.pipeline) {
            return true;
        }
        let (failed, was_active) = {
            let mut clips = self.clips.lock().unwrap();
            let Some(pos) = clips.iter().position(|c| src.has_as_ancestor(&c.bin)) else {
                return false;
            };
            (clips.remove(pos), pos == 0)
        };
        telemetry::CLIP_ERRORS.add(1, &[]);
        if self.recoveries.fetch_add(1, Ordering::SeqCst) >= self.files.len() {
            error!("every clip in the playlist failed consecutively; giving up");
            return false;
        }
        warn!(
            index = failed.index,
            file = %self.basename_at(failed.index),
            was_active,
            "clip failed; skipping past it"
        );
        // Same teardown order as teardown_preroll: release the concat pad
        // before Null, or the bin's streaming thread parked in concat holds
        // the stream lock set_state needs. Releasing the *active* pad is also
        // what makes concat cut to the prerolled clip.
        if let Some(pad) = failed.pad {
            self.concat.release_request_pad(&pad);
        }
        failed.bin.set_state(gst::State::Null).ok();
        self.pipeline.remove(&failed.bin).ok();
        let next = if was_active {
            self.mark_active();
            // Preroll the promoted clip's successor, like a natural boundary.
            self.clips
                .lock()
                .unwrap()
                .first()
                .map(|c| c.index)
                .unwrap_or(failed.index)
        } else {
            // The preroll failed: replace it with the clip after it, not the
            // active clip's successor — that would respawn the bad clip.
            failed.index
        };
        self.spawn((next + 1) % self.files.len(), 0);
        true
    }

    /// Pipeline running time: how long the pipeline has been playing, by the
    /// clock. Unlike a position query (answered from stream time, which jumps
    /// with every clip's segment) this is monotonic and wall-paced.
    pub(crate) fn running_time(&self) -> Option<gst::ClockTime> {
        let now = self.pipeline.clock()?.time();
        Some(now.saturating_sub(self.pipeline.base_time()?))
    }

    /// Stamp the current active clip (clips[0]) with the running time it went
    /// live, so the playhead can report position within the clip.
    pub(crate) fn mark_active(&self) {
        let rt = self.running_time();
        if let Some(active) = self.clips.lock().unwrap().first_mut() {
            active.start_rt = rt.or(Some(gst::ClockTime::ZERO));
        }
    }

    /// Tear down the prerolled clip(s) behind the active one, releasing their
    /// concat pads — so a following `spawn` becomes concat's next pad.
    fn teardown_preroll(&self) {
        let extra: Vec<Clip> = self.clips.lock().unwrap().drain(1..).collect();
        for c in extra {
            // Release the concat pad BEFORE stopping the bin: the prerolled
            // bin's streaming thread is parked inside concat waiting for its
            // turn, holding its pad's stream lock — set_state(Null) needs
            // that lock to deactivate the pad and deadlocks unless the
            // release wakes the waiter first.
            if let Some(pad) = c.pad {
                self.concat.release_request_pad(&pad);
            }
            c.bin.set_state(gst::State::Null).ok();
            self.pipeline.remove(&c.bin).ok();
        }
    }

    /// Redirect playback to `index` (optionally seeked): swap it in as the
    /// prerolled clip, then finish the active clip so concat cuts straight to
    /// it through the same long-lived encoder.
    pub(crate) fn play_index(self: &Arc<Self>, index: usize, offset_ms: i64) {
        self.teardown_preroll();
        self.spawn(index, offset_ms);
        self.jump();
    }

    pub(crate) fn play_random(self: &Arc<Self>) {
        self.play_index(self.random_index(), 0);
    }

    pub(crate) fn play_file(self: &Arc<Self>, name: &str) {
        match self.find(name) {
            Some(i) => self.play_index(i, 0),
            None => warn!(file = name, "play.file: not in playlist"),
        }
    }

    pub(crate) fn play_at(self: &Arc<Self>, name: &str, position_ms: i64) {
        match self.find(name) {
            Some(i) => self.play_index(i, position_ms),
            None => warn!(file = name, "play.at: not in playlist"),
        }
    }

    pub(crate) fn skip(self: &Arc<Self>, n: i32) {
        let i = skip_index(self.active_index(), n, self.files.len());
        self.play_index(i, 0);
    }

    pub(crate) fn back(self: &Arc<Self>, n: i32) {
        let i = back_index(self.active_index(), n, self.files.len());
        self.play_index(i, 0);
    }

    /// The stdin `j` analogue: finish the active clip *now*. Its EOS probe
    /// promotes the already-prerolled next clip through the same long-lived
    /// encoder — same mechanism as a natural boundary.
    pub(crate) fn jump(&self) {
        let active = self.clips.lock().unwrap().first().map(|c| c.bin.clone());
        if let Some(active) = active {
            active.send_event(gst::event::Eos::new());
        }
    }

    /// Basename of the active clip (`2018_0704_120000.MP4`), matching what
    /// vlc-server reports over `/vlc/current`. None when nothing is playing.
    pub(crate) fn current_basename(&self) -> Option<String> {
        let index = self.clips.lock().unwrap().first()?.index;
        Some(self.basename_at(index))
    }

    /// Current clip basename + playback position (ms) for the lastplayed
    /// last-value cache. Position = start offset + running time since the
    /// clip went active — clock-derived, so it can neither freeze nor race
    /// ahead the way position queries (stream time) and PTS watermarks
    /// (decode/queue horizon) both do. Falls back to the offset alone before
    /// the clip is stamped active.
    pub(crate) fn playhead(&self) -> Option<(String, i64)> {
        let clips = self.clips.lock().unwrap();
        let active = clips.first()?;
        let basename = self.basename_at(active.index);
        let position_ms = match (self.running_time(), active.start_rt) {
            (Some(now), Some(start)) => {
                active.offset_ms + (now.mseconds() as i64 - start.mseconds() as i64).max(0)
            }
            _ => active.offset_ms,
        };
        Some((basename, position_ms))
    }

    /// Clip duration in ms, from the cache or a pbutils Discoverer parse of
    /// the container (headers only, no decode). None when the file can't be
    /// parsed — seek walks treat such a clip as a boundary they land at
    /// rather than a span they can measure.
    pub(crate) fn duration_ms(&self, index: usize) -> Option<i64> {
        if let Some(d) = self.durations.lock().unwrap().get(&index) {
            return Some(*d);
        }
        let disc = gst_pbutils::Discoverer::new(gst::ClockTime::from_seconds(5)).ok()?;
        let info = disc.discover_uri(&self.uri_at(index)).ok()?;
        let d = info.duration()?.mseconds() as i64;
        self.durations.lock().unwrap().insert(index, d);
        Some(d)
    }

    /// Resolve the seek verb's signed delta against the live playhead into a
    /// landing (index, offset_ms). Walks clip durations, which discovers
    /// files on cache misses — file I/O, so callers must run this OFF the
    /// GLib main loop (which clip teardown shares) and hand the result to
    /// play_index there.
    pub(crate) fn seek_target(&self, delta_ms: i64) -> (usize, i64) {
        let active = self.active_index();
        let pos_ms = self.playhead().map(|(_, p)| p).unwrap_or(0);
        seek_walk(active, pos_ms, delta_ms, self.files.len(), |i| {
            self.duration_ms(i)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{back_index, should_seek_to, skip_index};

    #[test]
    fn skip_wraps_and_floors_to_one() {
        assert_eq!(skip_index(0, 1, 5), 1);
        assert_eq!(skip_index(3, 3, 5), 1); // 3+3=6 % 5
        assert_eq!(skip_index(4, 1, 5), 0); // wrap
        assert_eq!(skip_index(2, 0, 5), 3); // n<1 treated as 1
        assert_eq!(skip_index(2, -4, 5), 3);
    }

    #[test]
    fn seek_guards() {
        assert!(!should_seek_to(0, Some(60_000))); // no offset
        assert!(!should_seek_to(-5, Some(60_000)));
        assert!(should_seek_to(30_000, Some(60_000))); // mid-clip
        assert!(!should_seek_to(58_000, Some(60_000))); // tail guard
        assert!(!should_seek_to(59_500, Some(60_000))); // past the end
        assert!(should_seek_to(30_000, None)); // unknown duration errs toward seeking
        assert!(should_seek_to(30_000, Some(0)));
    }

    #[test]
    fn seek_walk_moves_by_signed_spans() {
        use super::seek_walk;
        // Three 180s clips; -1 marks a clip Discoverer couldn't measure.
        let durs = |list: &'static [i64]| {
            move |i: usize| {
                let d = list[i];
                (d >= 0).then_some(d)
            }
        };
        let uniform = durs(&[180_000, 180_000, 180_000]);

        // Within the active clip, both directions.
        assert_eq!(seek_walk(0, 10_000, 5_000, 3, uniform), (0, 15_000));
        assert_eq!(seek_walk(1, 60_000, -30_000, 3, uniform), (1, 30_000));
        assert_eq!(seek_walk(1, 5_000, 0, 3, uniform), (1, 5_000));

        // Across boundaries, wrapping in both directions.
        assert_eq!(seek_walk(0, 170_000, 20_000, 3, uniform), (1, 10_000));
        assert_eq!(seek_walk(0, 0, 400_000, 3, uniform), (2, 40_000));
        assert_eq!(seek_walk(2, 170_000, 20_000, 3, uniform), (0, 10_000));
        assert_eq!(seek_walk(1, 10_000, -30_000, 3, uniform), (0, 160_000));
        assert_eq!(seek_walk(0, 10_000, -30_000, 3, uniform), (2, 160_000));

        // A clip with an unknown duration is a boundary, not a span: forward
        // lands at the top of the clip after it, backward at its own top.
        let holey = durs(&[180_000, -1, 180_000]);
        assert_eq!(seek_walk(0, 0, 200_000, 3, holey), (2, 0));
        assert_eq!(seek_walk(2, 10_000, -30_000, 3, holey), (1, 0));

        // Moves longer than the corpus wrap modulo its total length (540s):
        // +10000s ≡ +280s → clip 1 @ 100s; -10000s ≡ +260s → clip 1 @ 80s.
        assert_eq!(seek_walk(0, 0, 10_000_000, 3, uniform), (1, 100_000));
        assert_eq!(seek_walk(0, 0, -10_000_000, 3, uniform), (1, 80_000));
        // Exact multiples of the corpus land back where they started.
        assert_eq!(seek_walk(1, 30_000, 5_400_000, 3, uniform), (1, 30_000));
        assert_eq!(seek_walk(1, 30_000, -5_400_000, 3, uniform), (1, 30_000));
        // Extreme deltas saturate instead of overflowing, then wrap.
        let (i, off) = seek_walk(0, 0, i64::MIN, 3, uniform);
        assert!(i < 3 && (0..180_000).contains(&off));
    }

    #[test]
    fn back_wraps_and_floors_to_one() {
        assert_eq!(back_index(1, 1, 5), 0);
        assert_eq!(back_index(0, 1, 5), 4); // wrap
        assert_eq!(back_index(2, 3, 5), 4); // 2-3 mod 5
        assert_eq!(back_index(3, 0, 5), 2); // n<1 treated as 1
    }
}
