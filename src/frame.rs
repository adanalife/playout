//! One frame of one clip as a JPEG, for the console's clip page. The console
//! has neither the corpus nor a decoder; this process has both, so it answers
//! `GET /frame/{basename}?at=` off its own playlist.
//!
//! Each grab is its own short-lived playbin, never the live pipeline: a seek
//! here can't touch what's on air, and a bad file fails this one request
//! rather than the stream.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use gst::prelude::*;
use gstreamer as gst;

/// Longest a grab may take before it is abandoned. Reading a keyframe off the
/// NFS mount is usually well under a second; the budget is for a soft mount
/// timing out, which must not pin a worker thread for good.
pub(crate) const BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// Output width in pixels. The clips are 1920 wide and the page shows the
/// picture at a card's width, so a third of that keeps the JPEG around 50 KB.
// ponytail: fixed width; a `w=` query parameter if a page ever wants more.
pub(crate) const WIDTH: u32 = 640;

/// The frame nearest to `at_ms` into `path`, JPEG-encoded. Seeks snap to the
/// keyframe before the moment: the clips are closed-GOP at a keyframe every
/// couple of seconds, which is as exact as a preview needs, and decoding from
/// the keyframe is what keeps a grab cheap on the box that is also encoding
/// the stream.
pub(crate) fn grab(path: &Path, at_ms: u64) -> Result<Vec<u8>> {
    let playbin = gst::ElementFactory::make("playbin")
        .property("uri", format!("file://{}", path.display()))
        .build()
        .context("playbin")?;
    // Video only: the audio track is never decoded, and nothing is rendered —
    // the sink bin ends in an appsink the frame is pulled from. Square pixels
    // are pinned so videoscale derives the height from the width; with width
    // alone it keeps the source height and records the squeeze in the
    // pixel-aspect-ratio, which a JPEG viewer ignores.
    playbin.set_property_from_str("flags", "video");
    // One decode thread: libav's decoder otherwise starts one per core for a
    // single keyframe, and every one of those threads leaves a malloc arena
    // behind. element-setup reaches the elements decodebin adds too.
    playbin.connect("element-setup", false, |args| {
        let element = args[1].get::<gst::Element>().ok()?;
        if element.find_property("max-threads").is_some() {
            element.set_property("max-threads", 1i32);
        }
        None
    });
    let sink = gst::parse::bin_from_description(
        &format!(
            "videoconvert ! videoscale ! video/x-raw,width={WIDTH},pixel-aspect-ratio=1/1 ! jpegenc ! appsink name=sink sync=false"
        ),
        true,
    )
    .context("sink bin")?;
    playbin.set_property("video-sink", &sink);

    let result = grab_from(&playbin, &sink, at_ms);
    let _ = playbin.set_state(gst::State::Null);
    result
}

fn grab_from(playbin: &gst::Element, sink: &gst::Bin, at_ms: u64) -> Result<Vec<u8>> {
    let appsink = sink.by_name("sink").context("appsink")?;
    playbin.set_state(gst::State::Paused)?;
    let (res, _, _) = playbin.state(gst::ClockTime::from_seconds(BUDGET.as_secs()));
    res.context("preroll")?;
    if at_ms > 0 {
        playbin.seek_simple(
            gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT | gst::SeekFlags::SNAP_BEFORE,
            gst::ClockTime::from_mseconds(at_ms),
        )?;
        let (res, _, _) = playbin.state(gst::ClockTime::from_seconds(BUDGET.as_secs()));
        res.context("preroll after seek")?;
    }
    // appsink's action signal; the gstreamer-app crate would wrap the same
    // call and is not otherwise a dependency.
    let sample = appsink
        .emit_by_name::<Option<gst::Sample>>("pull-preroll", &[])
        .ok_or_else(|| anyhow!("no frame prerolled"))?;
    let buffer = sample.buffer().context("sample without a buffer")?;
    let map = buffer.map_readable()?;
    Ok(map.as_slice().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A two-second 1080p test-pattern clip the way the corpus is cut: closed
    /// GOPs, H.264 in MP4. None when the encoder isn't installed, which is a
    /// skip on a laptop and a failure under `PLAYOUT_TOOLS_REQUIRED`.
    fn synth_clip(dir: &Path) -> Option<std::path::PathBuf> {
        gst::init().unwrap();
        if gst::ElementFactory::find("x264enc").is_none() {
            assert!(
                std::env::var_os("PLAYOUT_TOOLS_REQUIRED").is_none(),
                "PLAYOUT_TOOLS_REQUIRED is set, but x264enc is not installed"
            );
            eprintln!("skipping: x264enc not installed");
            return None;
        }
        let path = dir.join("2018_0514_224801_013.MP4");
        let desc = format!(
            "videotestsrc num-buffers=60 ! video/x-raw,width=1920,height=1080,framerate=30/1 \
             ! x264enc key-int-max=30 ! mp4mux ! filesink location={}",
            path.display()
        );
        let pipeline = gst::parse::launch(&desc).unwrap();
        pipeline.set_state(gst::State::Playing).unwrap();
        let bus = pipeline.bus().unwrap();
        let msg = bus.timed_pop_filtered(
            gst::ClockTime::from_seconds(30),
            &[gst::MessageType::Eos, gst::MessageType::Error],
        );
        pipeline.set_state(gst::State::Null).unwrap();
        assert!(
            matches!(
                msg.as_ref().map(|m| m.view()),
                Some(gst::MessageView::Eos(_))
            ),
            "synth clip did not reach EOS: {msg:?}"
        );
        Some(path)
    }

    /// (width, height) from the JPEG's baseline start-of-frame (SOF0) header.
    fn dimensions(jpeg: &[u8]) -> (u32, u32) {
        let at = jpeg
            .windows(2)
            .position(|w| w == [0xFF, 0xC0])
            .expect("no SOF0 marker");
        let be = |i: usize| u32::from(jpeg[at + i]) << 8 | u32::from(jpeg[at + i + 1]);
        (be(7), be(5))
    }

    #[test]
    fn grabs_a_jpeg_at_the_top_and_mid_clip() {
        let dir = std::env::temp_dir().join(format!("playout-frame-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let Some(clip) = synth_clip(&dir) else {
            return;
        };
        for at_ms in [0, 1500] {
            let jpeg = grab(&clip, at_ms).unwrap();
            // JPEG SOI marker, and a real picture rather than a header.
            assert_eq!(&jpeg[..2], &[0xFF, 0xD8], "at {at_ms} ms");
            assert!(jpeg.len() > 1000, "at {at_ms} ms: {} bytes", jpeg.len());
            assert_eq!(dimensions(&jpeg), (WIDTH, WIDTH * 9 / 16), "at {at_ms} ms");
        }
        // Past the end: the seek clamps to the last keyframe rather than failing.
        assert!(grab(&clip, 60_000).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_missing_file_is_an_error_not_a_hang() {
        gst::init().unwrap();
        let started = std::time::Instant::now();
        assert!(grab(Path::new("/nonexistent/clip.MP4"), 0).is_err());
        assert!(started.elapsed() < BUDGET);
    }
}
