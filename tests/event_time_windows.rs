mod common;

use db_rs::{clients::Client, stream};

#[tokio::test]
async fn change_events_include_ts_monotonic() {
    let (addr, _) = common::start_server().await;

    let mut subscriber = Client::connect(addr)
        .await
        .unwrap()
        .subscribe(vec!["__changes__".into()])
        .await
        .unwrap();

    let mut client = Client::connect(addr).await.unwrap();
    client.set("a", "1".into()).await.unwrap();
    client.set("b", "2".into()).await.unwrap();
    client.set("c", "3".into()).await.unwrap();

    let mut prev_ts = -1i64;
    for _ in 0..3 {
        let msg = subscriber.next_message().await.unwrap().unwrap();
        let (_, _, _, _, ts_ms) = stream::parse_change(&msg.content).unwrap();
        assert!(ts_ms >= 0);
        assert!(ts_ms >= prev_ts);
        prev_ts = ts_ms;
    }
}

#[tokio::test]
async fn window_aggregation_in_order() {
    let mut proc = stream::TumblingWindowProcessor::new(1000, 200);
    let events = [(0i64, "set"), (500, "set"), (1500, "set"), (2500, "set")];

    let mut all_out = Vec::new();
    for (ts, op) in events {
        all_out.extend(proc.process(ts, op));
    }

    let windows: Vec<_> = all_out.iter().filter(|(_, _, c)| !*c).copied().collect();
    assert_eq!(windows.len(), 2);
    assert!(windows.iter().any(|(we, c, _)| *we == 1000 && *c == 2));
    assert!(windows.iter().any(|(we, c, _)| *we == 2000 && *c == 1));
}

#[tokio::test]
async fn late_event_emits_correction() {
    let mut proc = stream::TumblingWindowProcessor::new(1000, 200);
    let events = [(0i64, "set"), (1500, "set"), (500, "set")];

    let mut all_out = Vec::new();
    for (ts, op) in events {
        all_out.extend(proc.process(ts, op));
    }

    let corrections: Vec<_> = all_out.iter().filter(|(_, _, c)| *c).copied().collect();
    assert!(!corrections.is_empty());
    assert!(corrections.iter().any(|(we, c, _)| *we == 1000 && *c == 1));
    assert!(corrections.iter().any(|(we, c, _)| *we == 1000 && *c == 2));
}
