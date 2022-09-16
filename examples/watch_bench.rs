//! KV example.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use etcd_client::*;
use tokio::time::{self, sleep, Instant};

const NUM_PUBS: u32 = 16;
const NUM_SUBS: u32 = 262144;

async fn progress_reporter(counters: Vec<Arc<AtomicU64>>) {
    let mut interval = time::interval(Duration::from_millis(1000));
    let mut c_old = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let mut c_min_old = counters
        .iter()
        .map(|c| c.load(Ordering::Relaxed))
        .min()
        .unwrap_or(0);
    let mut started_at = None;
    let mut skipped: u64 = 0;
    loop {
        interval.tick().await;
        // print!(
        //     "cnts are {:?}",
        //     counters
        //         .iter()
        //         .map(|c| c.load(Ordering::Relaxed))
        //         .collect::<Vec<_>>()
        // );
        let c_new = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        let c_min_new = counters
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .min()
            .unwrap_or(0);
        if c_new > 0 && started_at.is_none() {
            started_at = Some(Instant::now());
            skipped = c_new;
        }
        let avg_rps = started_at.map(|s| {
            let dur = s.elapsed();
            let dur_secs = dur.as_secs() as f64 + (dur.subsec_millis() as f64) / 1000.0;
            let avg_rps = (c_new - skipped) as f64 / dur_secs;
            (dur, avg_rps)
        });
        println!(
            "sum rps {}, min rps {} total {}, total min {}, duration, avg sum rps {:?}",
            c_new - c_old,
            c_min_new - c_min_old,
            c_new,
            c_min_new,
            avg_rps
        );
        c_old = c_new;
        c_min_old = c_min_new;
    }
}

async fn publish(mut client: Client) {
    loop {
        for i in 0..NUM_SUBS {
            let key = format!("key_{}", i);
            let value = vec![0xFF; 512];
            client.put(key, value, None).await.unwrap();
        }
    }
}

async fn subscribe(mut client: Client, counter: Arc<AtomicU64>, i: u32) {
    let key = format!("key_{}", i);
    let (_watcher, mut stream) = client.watch(key, None).await.unwrap();
    while let Some(resp) = stream.message().await.unwrap() {
        // println!("receive watch response");

        if resp.canceled() {
            println!("watch canceled!");
            break;
        }

        for event in resp.events() {
            // println!("event type: {:?}", event.event_type());
            if let Some(kv) = event.kv() {
                // println!("got k {}", kv.key_str().unwrap(),);
            }
        }
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let client = Client::connect(["127.0.0.1:2379"], None).await.unwrap();

    let mut counters = Vec::with_capacity(NUM_SUBS as usize);
    for _ in 0..NUM_SUBS {
        counters.push(Arc::new(AtomicU64::new(0)));
    }
    let h = tokio::spawn(progress_reporter(counters.clone()));

    for i in 0..NUM_SUBS {
        let c = client.clone();
        // let mut c = Client::connect(["localhost:2379"], None).await.unwrap();

        tokio::spawn(subscribe(c, counters[i as usize].clone(), i));
    }
    let pub_client = Client::connect(["127.0.0.1:2379"], None).await.unwrap();
    for _i in 0..NUM_PUBS {
        tokio::spawn(publish(pub_client.clone()));
    }

    h.await.unwrap();
    Ok(())
}
