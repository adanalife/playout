//! Control plane. Commands arrive over **core NATS** (fire-and-forget,
//! `tripbot.<env>.<domain>.<verb>.<platform>`); the currently-playing clip and
//! playback position flow back through the `TRIPBOT_<DOMAIN>_LASTPLAYED`
//! JetStream last-value cache, which a restarted instance reads to resume where
//! it left off. Every wire name is served under two domains (see [`DOMAINS`])
//! while the consumers migrate from the legacy `vlc` token to `playout`.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use futures::StreamExt;
use gst::glib;
use gstreamer as gst;
use serde::Deserialize;
use tracing::{info, warn};

use crate::SharedPlayer;

/// Wire-name domains, in resume-precedence order. `playout` is the name;
/// `vlc` is the legacy token tripbot's playout-client and the console still
/// speak — it goes (along with its JetStream stream) once every consumer has
/// flipped to `playout`. Until then commands are accepted on both, and the
/// lastplayed cache is published to both.
const DOMAINS: [&str; 2] = ["playout", "vlc"];

/// JetStream stream backing one domain's lastplayed last-value cache.
fn lastplayed_stream(domain: &str) -> String {
    format!("TRIPBOT_{}_LASTPLAYED", domain.to_ascii_uppercase())
}

fn subject(env: &str, domain: &str, verb: &str) -> String {
    format!("tripbot.{env}.{domain}.{verb}")
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

/// Connect to NATS and ensure the lastplayed stream exists. Returns None only
/// on a non-retryable config error; a server that's merely unreachable yields a
/// client that keeps dialing in the background (`retry_on_initial_connect`).
///
/// That retry covers the boot-race where a node reboot brings NATS up alongside
/// playout — without it the control plane stays dead for the life of the
/// process. The command subscriptions queue client-side and flush the moment
/// NATS answers, and `playout_nats_connected` tracks the live state via the
/// event callback so the gap is visible on the dashboard.
pub async fn connect(env: String, platform: String, url: String) -> Option<Control> {
    let client = match async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .event_callback(|event| async move {
            match event {
                async_nats::Event::Connected => {
                    info!("nats connected");
                    crate::telemetry::set_nats_connected(true);
                }
                async_nats::Event::Disconnected => {
                    warn!("nats disconnected; control plane paused until reconnect");
                    crate::telemetry::set_nats_connected(false);
                }
                _ => {}
            }
        })
        .connect(&url)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(err = %e, url = %url, "nats connect failed; control plane disabled");
            return None;
        }
    };

    // retry_on_initial_connect returns before the first handshake completes, so
    // wait a bounded spell for it here — otherwise the resume read below races
    // the connection and every boot cold-starts on a random clip. If NATS is
    // genuinely down the wait times out and we proceed anyway: resume is skipped
    // this boot, but the queued subscriptions still wire the control plane up
    // once NATS recovers.
    wait_for_connect(&client, Duration::from_secs(10)).await;

    // Idempotent: the streams outlive any single instance, so most boots find
    // them already declared. A config mismatch just logs — the stream still
    // exists, so publishes to its subject are captured either way.
    let cfgs: Vec<_> = DOMAINS
        .iter()
        .map(|d| jetstream::stream::Config {
            name: lastplayed_stream(d),
            subjects: vec![format!("{}.*", subject(&env, d, "lastplayed"))],
            max_messages_per_subject: 1,
            ..Default::default()
        })
        .collect();
    // Declaring the stream is a JetStream round-trip, so it must not sit on the
    // boot path: against an unreachable server the request only burns its own
    // timeout, delaying first frame by that much for a stream nobody can read
    // yet. Hand it to a task that waits for a connection first — boot proceeds
    // either way, and the stream is still declared the moment NATS appears.
    let ensure = client.clone();
    tokio::spawn(async move {
        while ensure.connection_state() != async_nats::connection::State::Connected {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let js = jetstream::new(ensure);
        for cfg in cfgs {
            if let Err(e) = js.create_stream(cfg).await {
                warn!(err = %e, "ensure lastplayed stream failed");
            }
        }
    });
    Some(Control {
        client,
        env,
        platform,
    })
}

/// Poll the client's connection state until it's connected or `timeout`
/// elapses. Used right after `retry_on_initial_connect` so the startup resume
/// read lands on a live connection when NATS is merely slow to come up, without
/// blocking the stream indefinitely when it's down for good.
async fn wait_for_connect(client: &async_nats::Client, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while client.connection_state() != async_nats::connection::State::Connected {
        if tokio::time::Instant::now() >= deadline {
            warn!("nats not connected within startup window; resume may cold-start");
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

impl Control {
    fn lastplayed_subject(&self, domain: &str) -> String {
        format!(
            "{}.{}",
            subject(&self.env, domain, "lastplayed"),
            self.platform
        )
    }

    /// Read this instance's last-value cache: the clip + position it published
    /// before restart, mapped to a playlist index. None when there's nothing to
    /// resume or the clip has since left the corpus. Each domain's stream is
    /// tried in `DOMAINS` order, so an instance upgraded from a build that only
    /// wrote the legacy stream still resumes from it.
    pub async fn resume_target(&self, player: &SharedPlayer) -> Option<(usize, i64)> {
        // The startup window above has already settled whether NATS answers. If
        // it doesn't, a JetStream read here would spend its whole timeout
        // discovering that again — on the boot path, ahead of first frame — and
        // there is nothing to resume from either way.
        if self.client.connection_state() != async_nats::connection::State::Connected {
            warn!("nats not connected; skipping resume, starting on a random clip");
            return None;
        }
        let js = jetstream::new(self.client.clone());
        for domain in DOMAINS {
            let Ok(stream) = js.get_stream(lastplayed_stream(domain)).await else {
                continue;
            };
            let Ok(msg) = stream
                .get_last_raw_message_by_subject(&self.lastplayed_subject(domain))
                .await
            else {
                continue;
            };
            let Ok(ev) = serde_json::from_slice::<LastPlayed>(&msg.payload) else {
                continue;
            };
            let Some(index) = player.find(&ev.file) else {
                continue;
            };
            info!(file = %ev.file, position_ms = ev.position_ms, domain, "resuming");
            return Some((index, ev.position_ms));
        }
        None
    }

    /// Subscribe to the command subjects and dispatch onto the GLib main loop
    /// (`idle_add_once`) so every pipeline mutation is serialized with the
    /// natural-boundary teardown — no cross-thread races on the clip list.
    ///
    /// One explicit subscription per domain × verb, each with this instance's
    /// platform leaf (`tripbot.<env>.<domain>.<verb>.<platform>`) — the shape
    /// tripbot publishes. The leaf keeps platforms isolated: a Twitch-triggered
    /// skip must never advance the YouTube stream sharing the env's NATS.
    pub async fn run_commands(self: Arc<Self>, player: SharedPlayer) {
        const VERBS: [&str; 6] = [
            "play.random",
            "play.file",
            "play.at",
            "skip",
            "back",
            "seek",
        ];
        // "tripbot.<env>.<domain>." — one prefix per domain.
        let bases: Vec<String> = DOMAINS.iter().map(|d| subject(&self.env, d, "")).collect();
        let mut subs = Vec::new();
        for base in &bases {
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
        }
        let mut merged = futures::stream::select_all(subs);
        while let Some(msg) = merged.next().await {
            let Some(verb) = bases
                .iter()
                .find_map(|base| verb_of(msg.subject.as_str(), base, &self.platform))
            else {
                continue;
            };
            let verb = verb.to_owned();
            let player = player.clone();
            let payload = msg.payload.clone();
            // Counted here rather than in `dispatch`: every subject that lands
            // in this loop is a command, so one increment covers all the verbs
            // including seek, which takes its own path below.
            crate::telemetry::COMMANDS.add(
                1,
                &crate::telemetry::attrs_with(opentelemetry::KeyValue::new("verb", verb.clone())),
            );
            // seek resolves its landing spot before touching the pipeline:
            // the walk discovers clip durations (file I/O), which must stay
            // off the GLib main loop that clip teardown shares. Only the
            // final play_index hops onto it, like every other mutation.
            if verb == "seek" {
                let delta_ms = decode::<DeltaArg>(&verb, &payload).map_or(0, |a| a.delta_ms);
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
    /// cache tracks where playback is — once per domain, so both streams hold
    /// the same record. Worst case a restart resumes one interval behind.
    pub async fn run_ticker(self: Arc<Self>, player: SharedPlayer, interval: Duration) {
        let subjs: Vec<String> = DOMAINS.iter().map(|d| self.lastplayed_subject(d)).collect();
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            let Some((file, position_ms)) = player.playhead() else {
                continue;
            };
            // emitted_at is a debug-only latency field in the payload contract,
            // unread on resume; leave it empty rather than pull in a time-format
            // dependency just to stamp it.
            let payload = serde_json::json!({
                "emitted_at": "",
                "file": file,
                "position_ms": position_ms,
            })
            .to_string();
            for subj in &subjs {
                let _ = self
                    .client
                    .publish(subj.clone(), payload.clone().into())
                    .await;
            }
        }
    }
}

/// Command verb from a full subject: strips the `tripbot.<env>.<domain>.`
/// prefix and this instance's `.<platform>` leaf. None for foreign subjects.
fn verb_of<'a>(subject: &'a str, base: &str, platform: &str) -> Option<&'a str> {
    subject
        .strip_prefix(base)?
        .strip_suffix(platform)?
        .strip_suffix('.')
}

/// Decode a command payload, warning rather than dropping it in silence. Every
/// command that reaches this point has already been counted by `COMMANDS`, so a
/// payload the producer and consumer disagree about would otherwise vanish with
/// the counter still claiming it landed. Real traffic always decodes — the
/// publisher marshals a struct — so this only fires on genuine contract drift.
fn decode<T: serde::de::DeserializeOwned>(verb: &str, payload: &[u8]) -> Option<T> {
    match serde_json::from_slice(payload) {
        Ok(v) => Some(v),
        Err(err) => {
            warn!(
                verb,
                %err,
                payload = %String::from_utf8_lossy(payload),
                "undecodable command payload"
            );
            None
        }
    }
}

/// Map a command verb + payload to a Player operation. Runs on the main loop.
fn dispatch(player: &SharedPlayer, verb: &str, payload: &[u8]) {
    match verb {
        "play.random" => player.play_random(),
        "play.file" => {
            if let Some(p) = decode::<PlayFile>(verb, payload) {
                player.play_file(&p.file);
            }
        }
        "play.at" => {
            if let Some(p) = decode::<PlayFileAt>(verb, payload) {
                player.play_at(&p.file, p.position_ms);
            }
        }
        // skip/back fall back to one clip rather than dropping: the move is
        // what the viewer asked for and the count is the detail.
        "skip" => player.skip(decode::<NArg>(verb, payload).map_or(1, |a| a.n)),
        "back" => player.back(decode::<NArg>(verb, payload).map_or(1, |a| a.n)),
        // Unknown verbs: ignore (only the subscribed command subjects arrive).
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{DOMAINS, lastplayed_stream, subject, verb_of};

    /// The wire names are the contract tripbot and the console speak; both
    /// spellings must stay byte-exact until the legacy one is dropped.
    #[test]
    fn wire_names_match_the_contract() {
        assert_eq!(DOMAINS, ["playout", "vlc"]);
        assert_eq!(subject("prod", "vlc", "skip"), "tripbot.prod.vlc.skip");
        assert_eq!(
            subject("prod", "playout", "skip"),
            "tripbot.prod.playout.skip"
        );
        assert_eq!(lastplayed_stream("vlc"), "TRIPBOT_VLC_LASTPLAYED");
        assert_eq!(lastplayed_stream("playout"), "TRIPBOT_PLAYOUT_LASTPLAYED");
    }

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
