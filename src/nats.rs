//! Control plane, wire-compatible with tripbot's libvlc vlc-server so nothing
//! upstream changes at cutover. Commands arrive over **core NATS** (fire-and-
//! forget, `tripbot.<env>.vlc.<verb>.<platform>`); the currently-playing clip and playback
//! position flow back through the `TRIPBOT_VLC_LASTPLAYED` JetStream last-value
//! cache, which a restarted instance reads to resume where it left off.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use futures::StreamExt;
use gst::glib;
use gstreamer as gst;
use serde::Deserialize;
use tracing::{info, warn};

use crate::SharedPlayer;

/// JetStream stream vlc-server declares for the lastplayed last-value cache.
const LASTPLAYED_STREAM: &str = "TRIPBOT_VLC_LASTPLAYED";

fn subject(env: &str, verb: &str) -> String {
    format!("tripbot.{env}.vlc.{verb}")
}

// Command payloads — the fields playout acts on. serde ignores the envelope's
// emitted_at and any other keys.
#[derive(Deserialize)]
struct PlayFile {
    file: String,
}

#[derive(Deserialize)]
struct PlayFileAt {
    file: String,
    #[serde(default)]
    position_ms: i64,
}

#[derive(Deserialize)]
struct NArg {
    #[serde(default)]
    n: i32,
}

#[derive(Deserialize)]
struct DeltaArg {
    #[serde(default)]
    delta_ms: i64,
}

#[derive(Deserialize)]
struct LastPlayed {
    file: String,
    #[serde(default)]
    position_ms: i64,
}

pub struct Control {
    client: async_nats::Client,
    env: String,
    platform: String,
}

/// Connect to NATS and ensure the lastplayed stream exists. Returns None if the
/// connection fails — the caller then runs without a control plane rather than
/// aborting the stream.
pub async fn connect(env: String, platform: String, url: String) -> Option<Control> {
    let client = match async_nats::connect(&url).await {
        Ok(c) => c,
        Err(e) => {
            warn!(err = %e, url = %url, "nats connect failed; control plane disabled");
            return None;
        }
    };
    let js = jetstream::new(client.clone());
    // Idempotent: vlc-server may already have declared this with the same
    // config. A mismatch just logs — the stream still exists, so publishes to
    // its subject are captured either way.
    let cfg = jetstream::stream::Config {
        name: LASTPLAYED_STREAM.to_string(),
        subjects: vec![format!("{}.*", subject(&env, "lastplayed"))],
        max_messages_per_subject: 1,
        ..Default::default()
    };
    if let Err(e) = js.create_stream(cfg).await {
        warn!(err = %e, "ensure lastplayed stream failed");
    }
    Some(Control {
        client,
        env,
        platform,
    })
}

impl Control {
    fn lastplayed_subject(&self) -> String {
        format!("{}.{}", subject(&self.env, "lastplayed"), self.platform)
    }

    /// Read this instance's last-value cache: the clip + position it published
    /// before restart, mapped to a playlist index. None when there's nothing to
    /// resume or the clip has since left the corpus.
    pub async fn resume_target(&self, player: &SharedPlayer) -> Option<(usize, i64)> {
        let js = jetstream::new(self.client.clone());
        let stream = js.get_stream(LASTPLAYED_STREAM).await.ok()?;
        let msg = stream
            .get_last_raw_message_by_subject(&self.lastplayed_subject())
            .await
            .ok()?;
        let ev: LastPlayed = serde_json::from_slice(&msg.payload).ok()?;
        let index = player.find(&ev.file)?;
        info!(file = %ev.file, position_ms = ev.position_ms, "resuming");
        Some((index, ev.position_ms))
    }

    /// Subscribe to the command subjects and dispatch onto the GLib main loop
    /// (`idle_add_once`) so every pipeline mutation is serialized with the
    /// natural-boundary teardown — no cross-thread races on the clip list.
    ///
    /// One explicit subscription per verb, each with this instance's platform
    /// leaf (`tripbot.<env>.vlc.<verb>.<platform>`) — the shape tripbot
    /// publishes. The leaf keeps platforms isolated: a Twitch-triggered skip
    /// must never advance the YouTube stream sharing the env's NATS.
    pub async fn run_commands(self: Arc<Self>, player: SharedPlayer) {
        const VERBS: [&str; 6] = [
            "play.random",
            "play.file",
            "play.at",
            "skip",
            "back",
            "seek",
        ];
        let base = subject(&self.env, ""); // "tripbot.<env>.vlc."
        let mut subs = Vec::new();
        for verb in VERBS {
            let subj = format!("{base}{verb}.{}", self.platform);
            match self.client.subscribe(subj.clone()).await {
                Ok(s) => subs.push(s),
                Err(e) => {
                    warn!(subject = %subj, err = %e, "nats subscribe failed; control plane disabled");
                    return;
                }
            }
            info!(subject = %subj, "nats subscribed");
        }
        let mut merged = futures::stream::select_all(subs);
        while let Some(msg) = merged.next().await {
            let Some(verb) = verb_of(msg.subject.as_str(), &base, &self.platform) else {
                continue;
            };
            let verb = verb.to_owned();
            let player = player.clone();
            let payload = msg.payload.clone();
            // seek resolves its landing spot before touching the pipeline:
            // the walk discovers clip durations (file I/O), which must stay
            // off the GLib main loop that clip teardown shares. Only the
            // final play_index hops onto it, like every other mutation.
            if verb == "seek" {
                crate::telemetry::COMMANDS.add(
                    1,
                    &crate::telemetry::attrs_with(opentelemetry::KeyValue::new("verb", verb)),
                );
                let delta_ms = serde_json::from_slice::<DeltaArg>(&payload)
                    .map(|a| a.delta_ms)
                    .unwrap_or(0);
                if delta_ms == 0 {
                    continue;
                }
                tokio::task::spawn_blocking(move || {
                    let (index, offset_ms) = player.seek_target(delta_ms);
                    info!(delta_ms, index, offset_ms, "seek");
                    glib::idle_add_once(move || player.play_index(index, offset_ms));
                });
                continue;
            }
            glib::idle_add_once(move || dispatch(&player, &verb, &payload));
        }
    }

    /// Republish the current clip + position every `interval` so the last-value
    /// cache tracks where playback is. Worst case a restart resumes one
    /// interval behind — matching vlc-server's ticker.
    pub async fn run_ticker(self: Arc<Self>, player: SharedPlayer, interval: Duration) {
        let subj = self.lastplayed_subject();
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            let Some((file, position_ms)) = player.playhead() else {
                continue;
            };
            // emitted_at is a debug-only latency field on vlc-server's side and
            // unused on resume; leave it empty rather than pull in a time-format
            // dependency just to stamp it.
            let payload = serde_json::json!({
                "emitted_at": "",
                "file": file,
                "position_ms": position_ms,
            })
            .to_string();
            let _ = self.client.publish(subj.clone(), payload.into()).await;
        }
    }
}

/// Command verb from a full subject: strips the `tripbot.<env>.vlc.` prefix
/// and this instance's `.<platform>` leaf. None for foreign subjects.
fn verb_of<'a>(subject: &'a str, base: &str, platform: &str) -> Option<&'a str> {
    subject
        .strip_prefix(base)?
        .strip_suffix(platform)?
        .strip_suffix('.')
}

/// Map a command verb + payload to a Player operation. Runs on the main loop.
fn dispatch(player: &SharedPlayer, verb: &str, payload: &[u8]) {
    if matches!(
        verb,
        "play.random" | "play.file" | "play.at" | "skip" | "back"
    ) {
        crate::telemetry::COMMANDS.add(
            1,
            &crate::telemetry::attrs_with(opentelemetry::KeyValue::new("verb", verb.to_string())),
        );
    }
    match verb {
        "play.random" => player.play_random(),
        "play.file" => {
            if let Ok(p) = serde_json::from_slice::<PlayFile>(payload) {
                player.play_file(&p.file);
            }
        }
        "play.at" => {
            if let Ok(p) = serde_json::from_slice::<PlayFileAt>(payload) {
                player.play_at(&p.file, p.position_ms);
            }
        }
        "skip" => {
            let n = serde_json::from_slice::<NArg>(payload)
                .map(|a| a.n)
                .unwrap_or(1);
            player.skip(n);
        }
        "back" => {
            let n = serde_json::from_slice::<NArg>(payload)
                .map(|a| a.n)
                .unwrap_or(1);
            player.back(n);
        }
        // Unknown verbs: ignore (only the subscribed command subjects arrive).
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::verb_of;

    #[test]
    fn verb_of_strips_base_and_platform_leaf() {
        let base = "tripbot.production.vlc.";
        assert_eq!(
            verb_of(
                "tripbot.production.vlc.play.random.youtube",
                base,
                "youtube"
            ),
            Some("play.random")
        );
        assert_eq!(
            verb_of("tripbot.production.vlc.skip.youtube", base, "youtube"),
            Some("skip")
        );
        // Another platform's leaf must not dispatch here.
        assert_eq!(
            verb_of("tripbot.production.vlc.skip.twitch", base, "youtube"),
            None
        );
        // Bare verb without a platform leaf is not a command.
        assert_eq!(
            verb_of("tripbot.production.vlc.skip", base, "youtube"),
            None
        );
        assert_eq!(verb_of("other.subject", base, "youtube"), None);
    }
}
