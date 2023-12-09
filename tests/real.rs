extern crate faktory;
extern crate serde_json;
extern crate url;

use faktory::*;
use serde_json::Value;
use std::convert::TryInto;
use std::io;
use std::sync;

macro_rules! skip_check {
    () => {
        if std::env::var_os("FAKTORY_URL").is_none() {
            return;
        }
    };
}

#[test]
fn hello_p() {
    skip_check!();
    let p = Producer::connect(None).unwrap();
    drop(p);
}

#[test]
fn hello_c() {
    skip_check!();
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string())
        .wid("hello".to_string())
        .labels(vec!["foo".to_string(), "bar".to_string()]);
    c.register("never_called", |_| -> io::Result<()> { unreachable!() });
    let c = c.connect(None).unwrap();
    drop(c);
}

#[test]
fn roundtrip() {
    skip_check!();
    let local = "roundtrip";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    {
        let tx = sync::Arc::clone(&tx);
        c.register(local, move |j| -> io::Result<()> {
            tx.lock().unwrap().send(j).unwrap();
            Ok(())
        });
    }
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();
    p.enqueue(Job::new(local, vec!["z"]).on_queue(local))
        .unwrap();
    c.run_one(0, &[local]).unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from("z")]);
}

#[test]
fn multi() {
    skip_check!();
    let local = "multi";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    {
        let tx = sync::Arc::clone(&tx);
        c.register(local, move |j| -> io::Result<()> {
            tx.lock().unwrap().send(j).unwrap();
            Ok(())
        });
    }
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .unwrap();

    c.run_one(0, &[local]).unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from(1), Value::from("foo")]);

    c.run_one(0, &[local]).unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from(2), Value::from("bar")]);
}

#[test]
fn fail() {
    skip_check!();
    let local = "fail";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    {
        let tx = sync::Arc::clone(&tx);
        c.register(local, move |j| -> io::Result<()> {
            tx.lock().unwrap().send(j).unwrap();
            Err(io::Error::new(io::ErrorKind::Other, "nope"))
        });
    }
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();

    // note that *enqueueing* the jobs didn't fail!
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .unwrap();

    c.run_one(0, &[local]).unwrap();
    c.run_one(0, &[local]).unwrap();
    drop(c);
    assert_eq!(rx.into_iter().take(2).count(), 2);

    // TODO: check that jobs *actually* failed!
}

#[test]
fn queue() {
    skip_check!();
    let local = "pause";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));

    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    c.register(local, move |_job| tx.lock().unwrap().send(true));
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();
    p.enqueue(Job::new(local, vec![Value::from(1)]).on_queue(local))
        .unwrap();
    p.queue_pause(&[local]).unwrap();

    let had_job = c.run_one(0, &[local]).unwrap();
    assert!(!had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(!worker_executed);

    p.queue_resume(&[local]).unwrap();

    let had_job = c.run_one(0, &[local]).unwrap();
    assert!(had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(worker_executed);
}

#[test]
#[cfg(feature = "ent")]
fn expiring_job() {
    use std::{env, thread, time};

    let url = env::var_os("FAKTORY_URL").expect(
        "Enterprise Faktory should be running for this test, and 'FAKTORY_URL' environment variable should be provided",
    );
    let url = url.to_str().expect("Is a utf-8 string");

    // prepare a producer ("client" in Faktory terms)
    let mut producer = Producer::connect(Some(url)).unwrap();

    // prepare a consumer ("worker" in Faktory terms)
    let mut consumer = ConsumerBuilder::default();
    consumer.register("AnExpiringJob", move |job| -> io::Result<_> {
        Ok(eprintln!("{:?}", job))
    });
    let mut consumer = consumer.connect(Some(url)).unwrap();

    // prepare an expiring job
    let job_ttl_secs: u64 = 3;

    let ttl = chrono::Duration::seconds(job_ttl_secs as i64);
    let job1 = JobBuilder::default()
        .kind("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .expires_at(chrono::Utc::now() + ttl)
        .build()
        .unwrap();

    // enqueue and fetch immediately job1:
    producer.enqueue(job1).unwrap();
    let had_job = consumer.run_one(0, &["default"]).unwrap();
    assert!(had_job);

    // check that the queue is drained:
    let had_job = consumer.run_one(0, &["default"]).unwrap();
    assert!(!had_job);

    // prepare another one:
    let job2 = JobBuilder::default()
        .kind("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .expires_at(chrono::Utc::now() + ttl)
        .build()
        .unwrap();

    // enquere and then fetch job2, but after ttl:
    producer.enqueue(job2).unwrap();
    thread::sleep(time::Duration::from_secs(job_ttl_secs * 2));
    let had_job = consumer.run_one(0, &["default"]).unwrap();

    // For the non-enterprise edition of Faktory, this assertion will
    // fail, which should be take into account when running the test suite on CI.
    assert!(!had_job);
}
