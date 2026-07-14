use crate::SharedPlayer;
use async_nats::jetstream::{self, Context};
use futures::StreamExt;

async fn get_stream(context: &Context) -> jetstream::stream::Stream {
    let subject = std::env::var("NATS_SUBJECT").unwrap_or_else(|_| "playback.cmd".to_string());
    match context.get_stream(&subject).await {
        Ok(stream) => stream,
        Err(_) => {
            let stream = context
                .create_stream(jetstream::stream::Config {
                    name: subject.clone(),
                    subjects: vec![subject],
                    ..Default::default()
                })
                .await
                .unwrap();
            stream
        }
    }
}

pub async fn run(player: SharedPlayer) {
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = async_nats::connect(nats_url).await.unwrap();
    let context = jetstream::new(client);

    let stream = get_stream(&context).await;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("playout".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut messages = consumer.messages().await.unwrap();
    while let Some(Ok(message)) = messages.next().await {
        let command = std::str::from_utf8(&message.payload).unwrap_or("");
        if command == "jump" {
            player.jump();
        }
        message.ack().await.unwrap();
    }
}
