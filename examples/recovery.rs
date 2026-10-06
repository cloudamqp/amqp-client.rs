//! Automatic recovery. Run it, then restart the broker (or close the
//! connection in its management UI) and watch publishing and consuming carry
//! on. The server-named queue comes back under a new name.
//!
//!     cargo run --example recovery [amqp-uri]

use std::time::Duration;

use amqp_client_rs::{
    Client, ExchangeDeclareOpts, QueueDeclareOpts, RecoveryConfig, SubscribeOpts,
};
use tokio::time::MissedTickBehavior;

#[tokio::main]
async fn main() -> Result<(), amqp_client_rs::Error> {
    tracing_subscriber::fmt().init();
    // Spawned so the Ctrl+C handler is installed right away, before connecting.
    let mut ctrl_c = tokio::spawn(tokio::signal::ctrl_c());
    let uri = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "amqp://guest:guest@localhost".into());

    // Opt in to recovery. build() still fails fast if the first connect fails.
    let client = Client::builder(&uri)?
        .recovery(RecoveryConfig::default())
        .build()
        .await?;

    // Declared through the client, so declared again after reconnecting.
    let exchange = client
        .topic("recovery-demo", ExchangeDeclareOpts::default())
        .await?;
    let exclusive = QueueDeclareOpts {
        exclusive: true,
        ..Default::default()
    };
    let queue = client.queue("", exclusive).await?;
    queue.bind(&exchange, "demo.#").await?;
    let mut queue_name = queue.name();
    println!("consuming from {queue_name}");

    let mut consumer = queue.subscribe::<Vec<u8>>(SubscribeOpts::default()).await?;
    let consuming = tokio::spawn(async move {
        // next() waits across recovery; it returns None once the client is
        // dropped.
        while let Some(delivery) = consumer.next().await {
            let body = String::from_utf8_lossy(&delivery.body);
            println!("received {body} ({})", delivery.routing_key);
            // Fails if the channel or connection is gone; the broker then
            // redelivers the message.
            if let Err(e) = delivery.ack() {
                println!("ack of {body} failed: {e}");
            }
        }
    });

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    for n in 1u64.. {
        tokio::select! {
            _ = &mut ctrl_c => break,
            _ = ticker.tick() => {}
        }
        // Waits at most RecoveryConfig::timeout (250 ms) for a new
        // connection, then fails; the next tick tries again. Publishes are
        // not confirmed: one sent just as the connection drops can be lost.
        let body = format!("tick {n}").into_bytes();
        tokio::select! {
            _ = &mut ctrl_c => break,
            result = client.publish(exchange.name(), "demo.tick", &body) => match result {
                Ok(()) => println!("published tick {n}"),
                Err(e) => println!("publish of tick {n} failed: {e}"),
            },
        }
        if queue.name() != queue_name {
            queue_name = queue.name();
            println!("queue is now {queue_name}");
        }
    }

    // A second Ctrl+C while shutting down exits at once.
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        std::process::exit(130);
    });
    let _ = client
        .channel()
        .exchange_delete(exchange.name(), false)
        .await;
    drop(client);
    let _ = consuming.await;
    Ok(())
}
