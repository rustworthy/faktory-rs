extern crate faktory;
extern crate serde_json;
extern crate url;

use faktory::*;
use serde_json::Value;
use std::{
    borrow::BorrowMut,
    env, io,
    sync::{self, Arc, Mutex},
    thread, time,
};

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

#[cfg(feature = "ent")]
macro_rules! skip_if_not_enterprise {
    () => {
        if std::env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
    };
}

#[cfg(feature = "ent")]
fn learn_faktory_url() -> String {
    let url = std::env::var_os("FAKTORY_URL").expect(
        "Enterprise Faktory should be running for this test, and 'FAKTORY_URL' environment variable should be provided",
    );
    url.to_str().expect("Is a utf-8 string").to_owned()
}

#[test]
#[cfg(feature = "ent")]
fn ent_expiring_job() {
    use std::{thread, time};

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut producer = Producer::connect(Some(&url)).unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register("AnExpiringJob", move |job| -> io::Result<_> {
        Ok(eprintln!("{:?}", job))
    });
    let mut consumer = consumer.connect(Some(&url)).unwrap();

    // prepare an expiring job:
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

#[cfg(feature = "ent")]
#[test]
fn ent_unique_job() {
    use faktory::error;
    use serde_json::Value;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let job_type = "order";

    // prepare producer and consumer:
    let mut producer = Producer::connect(Some(&url)).unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register(job_type, |job| -> io::Result<_> {
        Ok(eprintln!("{:?}", job))
    });
    let mut consumer = consumer.connect(Some(&url)).unwrap();

    // Reminder. Jobs are considered unique for kind + args + queue.
    // So the following two jobs, will be accepted by Faktory, since we
    // are not setting 'unique_for' when creating those jobs:
    let queue_name = "ent_unique_job";
    let args = vec![Value::from("ISBN-13:9781718501850"), Value::from(100)];
    let job1 = JobBuilder::default()
        .args(args.clone())
        .kind(job_type)
        .queue(queue_name)
        .build()
        .unwrap();
    producer.enqueue(job1).unwrap();
    let job2 = JobBuilder::default()
        .args(args.clone())
        .kind(job_type)
        .queue(queue_name)
        .build()
        .unwrap();
    producer.enqueue(job2).unwrap();

    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_another_one);
    let and_that_is_it_for_now = !consumer.run_one(0, &[queue_name]).unwrap();
    assert!(and_that_is_it_for_now);

    // let's now create a unique job and followed by a job with
    // the same args and kind (jobtype in Faktory terms) and pushed
    // to the same queue:
    let unique_for_secs = 3;
    let job1 = JobBuilder::default()
        .args(args.clone())
        .kind(job_type)
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build()
        .unwrap();
    producer.enqueue(job1).unwrap();
    // this one is a 'duplicate' ...
    let job2 = JobBuilder::default()
        .args(args.clone())
        .kind(job_type)
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build()
        .unwrap();
    // ... so the server will respond accordingly:
    let res = producer.enqueue(job2).unwrap_err();
    if let error::Error::Protocol(error::Protocol::Internal { msg }) = res {
        assert_eq!(msg, "NOTUNIQUE Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).unwrap();

    // For the non-enterprise edition of Faktory, this assertion WILL FAIL:
    assert!(!had_another_one);

    // Now let's repeat the latter case, but providing different args to job2:
    let job1 = JobBuilder::default()
        .args(args.clone())
        .kind(job_type)
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build()
        .unwrap();
    producer.enqueue(job1).unwrap();
    // this one is *NOT* a 'duplicate' ...
    let job2 = JobBuilder::default()
        .args(vec![Value::from("ISBN-13:9781718501850"), Value::from(101)])
        .kind(job_type)
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build()
        .unwrap();
    // ... so the server will accept it:
    producer.enqueue(job2).unwrap();

    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_another_one);

    // and the queue is empty again:
    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(!had_job);
}

#[cfg(feature = "ent")]
#[test]
fn ent_unique_job_until_success() {
    use faktory::error;
    use std::thread;
    use std::time;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let queue_name = "ent_unique_job_until_success";
    let job_type = "order";

    // the job will be being executed for at least 3 seconds,
    // but is unique for 4 seconds;
    let difficulty_level = 3;
    let unique_for = 4;

    let url1 = url.clone();
    let handle = thread::spawn(move || {
        // prepare producer and consumer, where the former can
        // send a job difficulty level as a job's args and the lattter
        // will sleep for a corresponding period of time, pretending
        // to work hard:
        let mut producer_a = Producer::connect(Some(&url1)).unwrap();
        let mut consumer_a = ConsumerBuilder::default();
        consumer_a.register(job_type, |job| -> io::Result<_> {
            let args = job.args().to_owned();
            let mut args = args.iter();
            let diffuculty_level = args
                .next()
                .expect("job difficulty level is there")
                .to_owned();
            let sleep_secs =
                serde_json::from_value::<i64>(diffuculty_level).expect("a valid number");
            thread::sleep(time::Duration::from_secs(sleep_secs as u64));
            Ok(eprintln!("{:?}", job))
        });
        let mut consumer_a = consumer_a.connect(Some(&url1)).unwrap();
        let job = JobBuilder::default()
            .args(vec![difficulty_level])
            .kind(job_type)
            .queue(queue_name)
            .unique_for(unique_for)
            .unique_until_success() // Faktory's default
            .build()
            .unwrap();
        producer_a.enqueue(job).unwrap();
        let had_job = consumer_a.run_one(0, &[queue_name]).unwrap();
        assert!(had_job);
    });

    // let spawned thread gain momentum:
    thread::sleep(time::Duration::from_secs(1));

    // continue
    let mut producer_b = Producer::connect(Some(&url)).unwrap();

    // this one is a 'duplicate' because the job is still
    // being executed in the spawned thread:
    let job = JobBuilder::default()
        .args(vec![difficulty_level])
        .kind(job_type)
        .queue(queue_name)
        .unique_for(unique_for)
        .build()
        .unwrap();
    // as a result:
    let res = producer_b.enqueue(job).unwrap_err();
    if let error::Error::Protocol(error::Protocol::Internal { msg }) = res {
        assert_eq!(msg, "NOTUNIQUE Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    handle.join().expect("should join successfully");

    // Not that the job submitted in a spawned thread has been successfully executed
    // (with ACK sent to server), the producer 'B' can push another one:
    assert!(producer_b
        .enqueue(
            JobBuilder::default()
                .args(vec![difficulty_level])
                .kind(job_type)
                .queue(queue_name)
                .unique_for(unique_for)
                .build()
                .unwrap()
        )
        .is_ok());
}

#[cfg(feature = "ent")]
#[test]
fn ent_unique_job_until_start() {
    use std::thread;
    use std::time;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let queue_name = "ent_unique_job_until_start";
    let job_type = "order";
    let difficulty_level = 3;
    let unique_for = 4;

    let url1 = url.clone();
    let handle = thread::spawn(move || {
        let mut producer_a = Producer::connect(Some(&url1)).unwrap();
        let mut consumer_a = ConsumerBuilder::default();
        consumer_a.register(job_type, |job| -> io::Result<_> {
            let args = job.args().to_owned();
            let mut args = args.iter();
            let diffuculty_level = args
                .next()
                .expect("job difficulty level is there")
                .to_owned();
            let sleep_secs =
                serde_json::from_value::<i64>(diffuculty_level).expect("a valid number");
            thread::sleep(time::Duration::from_secs(sleep_secs as u64));
            Ok(eprintln!("{:?}", job))
        });
        let mut consumer_a = consumer_a.connect(Some(&url1)).unwrap();
        producer_a
            .enqueue(
                JobBuilder::default()
                    .args(vec![difficulty_level])
                    .kind(job_type)
                    .queue(queue_name)
                    .unique_for(unique_for)
                    .unique_until_start() // NB!
                    .build()
                    .unwrap(),
            )
            .unwrap();
        // as soon as the job is fetched, the unique lock gets released
        let had_job = consumer_a.run_one(0, &[queue_name]).unwrap();
        assert!(had_job);
    });

    // let spawned thread gain momentum:
    thread::sleep(time::Duration::from_secs(1));

    // the unique lock has been released by this time, so the job is enqueued successfully:
    let mut producer_b = Producer::connect(Some(&url)).unwrap();
    assert!(producer_b
        .enqueue(
            JobBuilder::default()
                .args(vec![difficulty_level])
                .kind(job_type)
                .queue(queue_name)
                .unique_for(unique_for)
                .build()
                .unwrap()
        )
        .is_ok());

    handle.join().expect("should join successfully");
}

#[test]
fn test_tracker_can_send_progress_update() {
    if env::var_os("FAKTORY_URL").is_none() || env::var_os("FAKTORY_ENT").is_none() {
        return;
    }

    let tracker = Arc::new(Mutex::new(
        Tracker::new(None).expect("job progress tracker created successfully"),
    ));
    let tracker_captured = Arc::clone(&tracker);

    let mut producer = Producer::connect(None).unwrap();
    let job_tackable = JobBuilder::default()
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .kind("order")
        .queue("test_tracker_can_send_progress_update")
        .trackable() // NB!
        .build()
        .expect("job built successfully");

    let job_ordinary = JobBuilder::default()
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .kind("order")
        .queue("test_tracker_can_send_progress_update")
        .build()
        .expect("job built successfully");

    // let's remember this job's id:
    let job_id = job_tackable.id().to_owned();
    let job_id_captured = job_id.clone();

    producer
        .enqueue(job_tackable)
        .expect("enqueued successfully");

    let mut consumer = ConsumerBuilder::default();
    consumer.register("order", move |job| -> io::Result<_> {
        // trying to set progress on a community edition of Faktory will give:
        // 'an internal server error occurred: tracking subsystem is only available in Faktory Enterprise'
        let result = tracker_captured
            .lock()
            .expect("lock acquired successfully")
            .set_progress(
                ProgressUpdateBuilder::default()
                    .jid(job_id_captured.clone())
                    .desc("I am still reading it...".to_owned())
                    .percent(32)
                    .build()
                    .unwrap(),
            );
        assert!(result.is_ok());
        // let's sleep for a while ...
        thread::sleep(time::Duration::from_secs(2));

        // ... and read the progress info
        let result = tracker_captured
            .lock()
            .expect("lock acquired successfully")
            .get_progress(job_id_captured.clone())
            .expect("Retrieved progress update over the wire");

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.jid, job_id_captured.clone());
        assert_eq!(result.state, "working");
        // assert!(result.updated_at) make some reasonable assertion here
        assert_eq!(result.desc, Some("I am still reading it...".to_owned()));
        assert_eq!(result.percent, Some(32));

        // considering the job done
        Ok(eprintln!("{:?}", job))
    });

    let mut consumer = consumer
        .connect(None)
        .expect("Successfully ran a handshake with 'Faktory'");
    let had_one_job = consumer
        .run_one(0, &["test_tracker_can_send_progress_update"])
        .expect("really had one");

    assert!(had_one_job);

    let result = tracker
        .lock()
        .expect("lock acquired successfully")
        .get_progress(job_id.clone())
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    assert_eq!(result.jid, job_id);
    // 'Faktory' will be keeping last known update for at least 30 minutes:
    assert_eq!(result.desc, Some("I am still reading it...".to_owned()));
    assert_eq!(result.percent, Some(32));

    // But it actually knows the job's real status, since the consumer (worker)
    // informed it immediately after finishing with the job:
    assert_eq!(result.state, "success");

    // What about 'ordinary' job ?
    let job_id = job_ordinary.id().to_owned().clone();

    // Sending it ...
    producer
        .enqueue(job_ordinary)
        .expect("Successfuly send to Faktory");

    // ... and asking for its progress
    let result = tracker
        .lock()
        .expect("lock acquired successfully")
        .get_progress(job_id.clone())
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    // From the docs:
    // There are several reasons why a job's state might be unknown:
    //    The JID is invalid or was never actually enqueued.
    //    The job was not tagged with the track variable in the job's custom attributes: custom:{"track":1}.
    //    The job's tracking structure has expired in Redis. It lives for 30 minutes and a big queue backlog can lead to expiration.
    assert_eq!(result.jid, job_id);
    assert_eq!(result.state, "unknown")
}
