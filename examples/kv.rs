//! KV example.

use std::time::Duration;

use etcd_client::*;
use tokio::time::sleep;

const N: u32 = 100;

async fn pusher(mut client: Client) {
    loop {
        for i in 0..N {
            let key = format!("key_{}", i);
            let value = vec![0xFF; 1024];
            client.put(key, value, None).await.unwrap();
        }
    }
}

async fn watcher(mut client: Client, i: u32) {
    let key = format!("key_{}", i);
    let (_watcher, mut stream) = client.watch(key, None).await.unwrap();
    while let Some(resp) = stream.message().await.unwrap() {
        println!("receive watch response");

        if resp.canceled() {
            println!("watch canceled!");
            break;
        }

        for event in resp.events() {
            println!("event type: {:?}", event.event_type());
            if let Some(kv) = event.kv() {
                println!(
                    "kv: {{{}: {}}}",
                    kv.key_str().unwrap(),
                    kv.value_str().unwrap()
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await.unwrap();
    let lease = client.lease_grant(10, None).await?;
    let keys = 10000;
    let workers = 10000;
    let keys_in_worker = keys / workers;

    let handles = (0..workers)
        .map(|i| tokio::spawn(work(client.clone(), i, lease.id(), keys_in_worker)))
        .collect::<Vec<_>>();
    for h in handles {
        h.await.unwrap().unwrap();
    }

    // for i in 0..keys {
    // work(client.clone(), i, lease.id(), 1).await.unwrap();
    // }

    // println!("inserted");

    Ok(())
}
