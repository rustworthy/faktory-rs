extern crate faktory;
extern crate serde_json;
extern crate url;

use chrono::Utc;
use faktory::*;
use serde_json::Value;
use std::io;

macro_rules! skip_if_not_enterprise {
    () => {
        if std::env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
    };
}

fn learn_faktory_url() -> String {
    let url = std::env::var_os("FAKTORY_URL").expect(
        "Enterprise Faktory should be running for this test, and 'FAKTORY_URL' environment variable should be provided",
    );
    url.to_str().expect("Is a utf-8 string").to_owned()
}

#[test]
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
    let job1 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and fetch immediately job1:
    producer.enqueue(job1).unwrap();
    let had_job = consumer.run_one(0, &["default"]).unwrap();
    assert!(had_job);

    // check that the queue is drained:
    let had_job = consumer.run_one(0, &["default"]).unwrap();
    assert!(!had_job);

    // prepare another one:
    let job2 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and then fetch job2, but after ttl:
    producer.enqueue(job2).unwrap();
    thread::sleep(time::Duration::from_secs(job_ttl_secs * 2));
    let had_job = consumer.run_one(0, &["default"]).unwrap();

    // For the non-enterprise edition of Faktory, this assertion will
    // fail, which should be taken into account when running the test suite on CI.
    assert!(!had_job);
}

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
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
    producer.enqueue(job1).unwrap();
    let job2 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
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
    let job1 = Job::builder(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    producer.enqueue(job1).unwrap();
    // this one is a 'duplicate' ...
    let job2 = Job::builder(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will respond accordingly:
    let res = producer.enqueue(job2).unwrap_err();
    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    // Let's now consume the job which is 'holding' a unique lock:
    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);

    // And check that the queue is really empty (`job2` from above
    // has not been queued indeed):
    let queue_is_empty = !consumer.run_one(0, &[queue_name]).unwrap();
    assert!(queue_is_empty);

    // Now let's repeat the latter case, but providing different args to job2:
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    producer.enqueue(job1).unwrap();
    // this one is *NOT* a 'duplicate' ...
    let job2 = JobBuilder::new(job_type)
        .args(vec![Value::from("ISBN-13:9781718501850"), Value::from(101)])
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
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
        let job = JobBuilder::new(job_type)
            .args(vec![difficulty_level])
            .queue(queue_name)
            .unique_for(unique_for)
            .unique_until_success() // Faktory's default
            .build();
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
    let job = JobBuilder::new(job_type)
        .args(vec![difficulty_level])
        .queue(queue_name)
        .unique_for(unique_for)
        .build();

    // as a result:
    let res = producer_b.enqueue(job).unwrap_err();
    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    handle.join().expect("should join successfully");

    // Now that the job submitted in a spawned thread has been successfully executed
    // (with ACK sent to server), the producer 'B' can push another one:
    producer_b
        .enqueue(
            JobBuilder::new(job_type)
                .args(vec![difficulty_level])
                .queue(queue_name)
                .unique_for(unique_for)
                .build(),
        )
        .unwrap();
}

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
                JobBuilder::new(job_type)
                    .args(vec![difficulty_level])
                    .queue(queue_name)
                    .unique_for(unique_for)
                    .unique_until_start() // NB!
                    .build(),
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
    producer_b
        .enqueue(
            JobBuilder::new(job_type)
                .args(vec![difficulty_level])
                .queue(queue_name)
                .unique_for(unique_for)
                .build(),
        )
        .unwrap();

    handle.join().expect("should join successfully");
}

#[test]
fn ent_unique_job_bypass_unique_lock() {
    use faktory::error;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let mut producer = Producer::connect(Some(&url)).unwrap();

    let job1 = Job::builder("order")
        .queue("ent_unique_job_bypass_unique_lock")
        .unique_for(60)
        .build();

    // Now the following job is _technically_ a 'duplicate', BUT if the `unique_for` value is not set,
    // the uniqueness lock will be bypassed on the server. This special case is mentioned in the docs:
    // https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs#bypassing-uniqueness
    let job2 = Job::builder("order") // same jobtype and args (args are just not set)
        .queue("ent_unique_job_bypass_unique_lock") // same queue
        .build(); // NB: `unique_for` not set

    producer.enqueue(job1).unwrap();
    producer.enqueue(job2).unwrap(); // bypassing the lock!

    // This _is_ a 'duplicate'.
    let job3 = Job::builder("order")
        .queue("ent_unique_job_bypass_unique_lock")
        .unique_for(60) // NB
        .build();

    let res = producer.enqueue(job3).unwrap_err(); // NOT bypassing the lock!

    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }
}

#[test]
fn test_tracker_can_send_and_retrieve_job_execution_progress() {
    use std::{
        io,
        sync::{Arc, Mutex},
        thread, time,
    };

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let tracker = Arc::new(Mutex::new(
        Tracker::connect(Some(&url)).expect("job progress tracker created successfully"),
    ));
    let tracker_captured = Arc::clone(&tracker);

    let mut producer = Producer::connect(Some(&url)).unwrap();
    let job_tackable = JobBuilder::new("order")
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .queue("test_tracker_can_send_progress_update")
        .build_trackable();

    let job_ordinary = JobBuilder::new("order")
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .queue("test_tracker_can_send_progress_update")
        .build();

    // let's remember this job's id:
    let job_id = job_tackable.id().to_owned();
    let job_id_captured = job_id.clone();

    producer.enqueue(job_tackable).expect("enqueued");

    let mut consumer = ConsumerBuilder::default();
    consumer.register("order", move |job| -> io::Result<_> {
        // trying to set progress on a community edition of Faktory will give:
        // 'an internal server error occurred: tracking subsystem is only available in Faktory Enterprise'
        let result = tracker_captured
            .lock()
            .expect("lock acquired")
            .set_progress(
                ProgressUpdateBuilder::new(&job_id_captured)
                    .desc("I am still reading it...".to_owned())
                    .percent(32)
                    .build(),
            );
        assert!(result.is_ok());
        // let's sleep for a while ...
        thread::sleep(time::Duration::from_secs(2));

        // ... and read the progress info
        let result = tracker_captured
            .lock()
            .expect("lock acquired")
            .get_progress(job_id_captured.clone())
            .expect("Retrieved progress update over the wire");

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.jid, job_id_captured.clone());
        assert_eq!(result.state, "working");
        assert!(result.updated_at.is_some());
        assert_eq!(result.desc, Some("I am still reading it...".to_owned()));
        assert_eq!(result.percent, Some(32));

        // considering the job done
        Ok(eprintln!("{:?}", job))
    });

    let mut consumer = consumer
        .connect(Some(&url))
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
        .expect("lock acquired")
        .get_progress(job_id.clone())
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    // From the docs:
    // There are several reasons why a job's state might be unknown:
    //    The JID is invalid or was never actually enqueued.
    //    The job was not tagged with the track variable in the job's custom attributes: custom:{"track":1}.
    //    The job's tracking structure has expired in Redis. It lives for 30 minutes and a big queue backlog can lead to expiration.
    assert_eq!(result.jid, job_id);

    // Returned from Faktory: '{"jid":"f7APFzrS2RZi9eaA","state":"unknown","updated_at":""}'
    assert_eq!(result.state, "unknown");
    assert!(result.updated_at.is_none());
    assert!(result.percent.is_none());
    assert!(result.desc.is_none());
}

#[test]
fn test_batch_of_jobs_can_be_initiated() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    let mut producer = Producer::connect(Some(&url)).unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register("thumbnail", move |_job| -> io::Result<_> { Ok(()) });
    consumer.register("clean_up", move |_job| -> io::Result<_> { Ok(()) });
    let mut consumer = consumer.connect(Some(&url)).unwrap();
    let mut tracker =
        Tracker::connect(Some(&url)).expect("job progress tracker created successfully");

    let job_1 = Job::builder("thumbnail")
        .args(vec!["path/to/original/image1"])
        .queue("test_batch_of_jobs_can_be_initiated")
        .build();
    let job_2 = Job::builder("thumbnail")
        .args(vec!["path/to/original/image2"])
        .queue("test_batch_of_jobs_can_be_initiated")
        .build();
    let job_3 = Job::builder("thumbnail")
        .args(vec!["path/to/original/image3"])
        .queue("test_batch_of_jobs_can_be_initiated")
        .build();

    let cb_job = Job::builder("clean_up")
        .queue("test_batch_of_jobs_can_be_initiated__CALLBACKs")
        .build();

    let batch =
        Batch::builder("Image resizing workload".to_string()).with_complete_callback(cb_job);

    let time_just_before_batch_init = Utc::now();

    let mut batch = producer.start_batch(batch).unwrap();

    // let's remember batch id:
    let batch_id = batch.id().to_string();

    batch.add(job_1).unwrap();
    batch.add(job_2).unwrap();
    batch.add(job_3).unwrap();
    batch.commit().unwrap();

    // The batch has been committed, let's see its status:
    let time_just_before_getting_status = Utc::now();

    let status = tracker
        .get_batch_status(batch_id.clone())
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // Just to make a meaningfull assertion about the BatchStatus's 'created_at' field:
    assert!(status.created_at > time_just_before_batch_init);
    assert!(status.created_at < time_just_before_getting_status);
    assert_eq!(status.bid, batch_id);
    assert_eq!(status.description, Some("Image resizing workload".into()));
    assert_eq!(status.total, 3); // three jobs registered
    assert_eq!(status.pending, 3); // and none executed just yet
    assert_eq!(status.failed, 0);
    // Docs do not mention it, but the golang client does:
    // https://github.com/contribsys/faktory/blob/main/client/batch.go#L17-L19
    assert_eq!(status.success_state, ""); // we did not even provide the 'success' callback
    assert_eq!(status.complete_state, ""); // the 'complete' callback is pending

    // consume and execute job 1 ...
    let had_one = consumer
        .run_one(0, &["test_batch_of_jobs_can_be_initiated"])
        .unwrap();
    assert!(had_one);

    // ... and try consuming from the "callback" queue:
    let had_one = consumer
        .run_one(0, &["test_batch_of_jobs_can_be_initiated__CALLBACKs"])
        .unwrap();
    assert!(!had_one); // nothing in there

    // let's ask the Faktory server about the batch status after
    // we have consumed one job from this batch:
    let status = tracker
        .get_batch_status(batch_id.clone())
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 1 of 3 jobs:
    assert_eq!(status.total, 3);
    assert_eq!(status.pending, 2);
    assert_eq!(status.failed, 0);

    // now, consume and execute job 2
    let had_one = consumer
        .run_one(0, &["test_batch_of_jobs_can_be_initiated"])
        .unwrap();
    assert!(had_one);

    // ... and the "callback" queue again:
    let had_one = consumer
        .run_one(0, &["test_batch_of_jobs_can_be_initiated__CALLBACKs"])
        .unwrap();
    assert!(!had_one); // not just yet ...

    // let's check batch status once again:
    let status = tracker
        .get_batch_status(batch_id.clone())
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(status.total, 3);
    assert_eq!(status.pending, 1);
    assert_eq!(status.failed, 0);

    // finally, consume and execute job 3 - the last one from the batch
    let had_one = consumer
        .run_one(0, &["test_batch_of_jobs_can_be_initiated"])
        .unwrap();
    assert!(had_one);

    // let's check batch status to see what happens after
    // all the jobs from the batch have been executed:
    let status = tracker
        .get_batch_status(batch_id.clone())
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(status.total, 3);
    assert_eq!(status.pending, 0);
    assert_eq!(status.failed, 0);
    assert_eq!(status.complete_state, "1"); // callback has been enqueued!!

    // let's now consume from the "callback" queue:
    let had_one = consumer
        .run_one(0, &["test_batch_of_jobs_can_be_initiated__CALLBACKs"])
        .unwrap();
    assert!(had_one); // we successfully consumed the callback

    // let's check batch status one last time:
    let status = tracker
        .get_batch_status(batch_id.clone())
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(status.complete_state, "2"); // means calledback successfully executed
}

#[test]
fn test_batches_can_be_nested() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    // Set up 'producer', 'consumer', and 'tracker':
    let mut producer = Producer::connect(Some(&url)).unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register("jobtype", move |_job| -> io::Result<_> { Ok(()) });
    let mut _consumer = consumer.connect(Some(&url)).unwrap();
    let mut tracker =
        Tracker::connect(Some(&url)).expect("job progress tracker created successfully");

    // Prepare some jobs:
    let parent_job1 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();
    let child_job_1 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();
    let child_job_2 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();
    let grand_child_job_1 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();

    // Sccording to Faktory docs:
    // "The callback for a parent batch will not enqueue until the callback for the child batch has finished."
    // See: https://github.com/contribsys/faktory/wiki/Ent-Batches#guarantees
    let parent_cb_job = Job::builder("clean_up")
        .queue("test_batches_can_be_nested__CALLBACKs")
        .build();
    let child_cb_job = Job::builder("clean_up")
        .queue("test_batches_can_be_nested__CALLBACKs")
        .build();
    let grandchild_cb_job = Job::builder("clean_up")
        .queue("test_batches_can_be_nested__CALLBACKs")
        .build();

    // batches start
    let parent_batch =
        Batch::builder("Parent batch".to_string()).with_success_callback(parent_cb_job);
    let mut parent_batch = producer.start_batch(parent_batch).unwrap();
    let parent_batch_id = parent_batch.id().to_owned();
    parent_batch.add(parent_job1).unwrap();

    let child_batch = Batch::builder("Child batch".to_string()).with_success_callback(child_cb_job);
    let mut child_batch = parent_batch.start_batch(child_batch).unwrap();
    let child_batch_id = child_batch.id().to_owned();
    child_batch.add(child_job_1).unwrap();
    child_batch.add(child_job_2).unwrap();

    let grandchild_batch =
        Batch::builder("Grandchild batch".to_string()).with_success_callback(grandchild_cb_job);
    let mut grandchild_batch = child_batch.start_batch(grandchild_batch).unwrap();
    let grandchild_batch_id = grandchild_batch.id().to_owned();
    grandchild_batch.add(grand_child_job_1).unwrap();

    grandchild_batch.commit().unwrap();
    child_batch.commit().unwrap();
    parent_batch.commit().unwrap();
    // batches finish

    let parent_status = tracker
        .get_batch_status(parent_batch_id.clone())
        .unwrap()
        .unwrap();
    assert_eq!(parent_status.description, Some("Parent batch".to_string()));
    assert_eq!(parent_status.total, 1);
    assert_eq!(parent_status.parent_bid, None);

    let child_status = tracker
        .get_batch_status(child_batch_id.clone())
        .unwrap()
        .unwrap();
    assert_eq!(child_status.description, Some("Child batch".to_string()));
    assert_eq!(child_status.total, 2);
    assert_eq!(child_status.parent_bid, Some(parent_batch_id));

    let grandchild_status = tracker
        .get_batch_status(grandchild_batch_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        grandchild_status.description,
        Some("Grandchild batch".to_string())
    );
    assert_eq!(grandchild_status.total, 1);
    assert_eq!(grandchild_status.parent_bid, Some(child_batch_id));
}

fn some_jobs(
    kind: impl Into<String> + Clone + 'static,
    q: impl Into<String> + Clone + 'static,
    count: usize,
) -> impl Iterator<Item = Job> {
    (0..count)
        .into_iter()
        .map(move |_| Job::builder(kind.clone()).queue(q.clone()).build())
}

#[test]
fn test_can_open_batch_and_add_more_jobs_before_comiitting_again() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();
    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut t = Tracker::connect(Some(&url)).unwrap();
    let mut jobs = some_jobs(
        "order",
        "test_committed_batch_cannot_be_reopened_from_outside",
        4,
    );
    let mut callbacks = some_jobs(
        "order_clean_up",
        "test_committed_batch_cannot_be_reopened_from_outside__CALLBACKs",
        1,
    );

    let b = Batch::builder("Orders processing workload".to_string())
        .with_success_callback(callbacks.next().unwrap());

    let mut b = p.start_batch(b).unwrap();
    let bid = b.id().to_string();
    b.add(jobs.next().unwrap()).unwrap(); // 1 job
    b.add(jobs.next().unwrap()).unwrap(); // 2 jobs

    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.total, 2);
    assert_eq!(status.pending, 2);

    // opening an uncommitted batch:
    let mut b = p.open_batch(bid.clone()).unwrap();
    assert_eq!(b.id(), bid);
    b.add(jobs.next().unwrap()).unwrap(); // 3 jobs
    b.commit().unwrap();
    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.total, 3);
    assert_eq!(status.pending, 3);

    // opening an already committed batch:
    let mut b = p.open_batch(bid.clone()).unwrap();
    b.add(jobs.next().unwrap()).unwrap(); // 4 jobs
    b.commit().unwrap();

    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.total, 4);
    assert_eq!(status.pending, 4);

    // let's now consume those jobs:
    let mut c = ConsumerBuilder::default();
    c.register("order", move |_job| -> io::Result<_> { Ok(()) });
    let mut c = c.connect(Some(&url)).unwrap();

    let _: Vec<_> = (0..3) // let's consume 3 jobs
        .map(|_| {
            assert!(c
                .run_one(0, &["test_committed_batch_cannot_be_reopened_from_outside"])
                .unwrap())
        })
        .collect();

    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.total, 4);
    assert_eq!(status.pending, 1);
    assert_eq!(status.failed, 0);

    // now let's re-open the batch once again:
    let b = p.open_batch(bid.clone()).unwrap();

    // and commit the last pending job:
    assert!(c
        .run_one(0, &["test_committed_batch_cannot_be_reopened_from_outside"])
        .unwrap());

    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.total, 4);
    assert_eq!(status.pending, 0);
    assert_eq!(status.failed, 0);

    // let's check that were no callbacks in the success queue:
    let empty = !c
        .run_one(
            0,
            &["test_committed_batch_cannot_be_reopened_from_outside__CALLBACKs"],
        )
        .unwrap();
    assert!(empty);

    // now let's commit the batch:
    b.commit().unwrap();
    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.complete_state, "1"); // success callback has been enqueued

    // let's consume from the callbacks queue:
    let had_one = c
        .run_one(
            0,
            &["test_committed_batch_cannot_be_reopened_from_outside__CALLBACKs"],
        )
        .unwrap();
    assert!(had_one);

    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.complete_state, "2"); // callback executed
}
