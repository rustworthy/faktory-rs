use chrono::{DateTime, Utc};
use derive_builder::Builder;
use std::collections::HashMap;
use std::io::prelude::*;

mod cmd;
mod resp;
mod utils;

use crate::error::{self, Error};

pub use self::cmd::*;
pub use self::resp::*;

const JOB_DEFAULT_QUEUE: &str = "default";
const JOB_DEFAULT_RESERVED_FOR_SECS: usize = 600;
const JOB_DEFAULT_RETRY_COUNT: usize = 25;
const JOB_PRIORITY_MAX: u8 = 9;
const JOB_DEFAULT_PRIORITY: u8 = 5;
const JOB_DEFAULT_BACKTRACE: usize = 0;

/// A Faktory job.
///
/// To create a job, use 'Job::new' specifying 'kind' and 'args':
/// ```
/// use faktory::Job;
///
/// let _job = Job::new("order", vec!["ISBN-13:9781718501850"]);
/// ```
///
/// Alternatively, use 'JobBuilder' to construct a job:
/// ```
/// use faktory::JobBuilder;
///
/// let result = JobBuilder::default()
///     .kind("order")
///     .args(vec!["ISBN-13:9781718501850"])
///     .build();
/// if result.is_err() {
///     todo!("Handle me gracefully, please.")
/// };
/// let _job = result.unwrap();
/// ```
///
/// See also the [Faktory wiki](https://github.com/contribsys/faktory/wiki/The-Job-Payload).
#[derive(Serialize, Deserialize, Debug, Builder)]
#[builder(
    setter(into),
    build_fn(name = "try_build", private, validate = "Self::validate")
)]
pub struct Job {
    /// The job's unique identifier.
    #[builder(default = "utils::gen_random_jid()")]
    pub(crate) jid: String,

    /// The queue this job belongs to. Usually `default`.
    #[builder(default = "JOB_DEFAULT_QUEUE.into()")]
    pub queue: String,

    /// The job's type. Called `kind` because `type` is reserved.
    #[serde(rename = "jobtype")]
    pub(crate) kind: String,

    /// The arguments provided for this job.
    #[builder(setter(custom))]
    pub(crate) args: Vec<serde_json::Value>,

    /// When this job was created.
    // note that serializing works correctly here since the default chrono serialization
    // is RFC3339, which is also what Faktory expects.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "Some(Utc::now())")]
    pub created_at: Option<DateTime<Utc>>,

    /// When this job was supplied to the Faktory server.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub enqueued_at: Option<DateTime<Utc>>,

    /// When this job is scheduled for.
    ///
    /// Defaults to immediately.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub at: Option<DateTime<Utc>>,

    /// How long to allow this job to run for.
    ///
    /// Defaults to 600 seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "Some(JOB_DEFAULT_RESERVED_FOR_SECS)")]
    pub reserve_for: Option<usize>,

    /// Number of times to retry this job.
    ///
    /// Defaults to 25.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "Some(JOB_DEFAULT_RETRY_COUNT)")]
    pub retry: Option<usize>,

    /// The priority of this job from 1-9 (9 is highest).
    ///
    /// Pushing a job with priority 9 will effectively put it at the front of the queue.
    /// Defaults to 5.
    #[builder(default = "Some(JOB_DEFAULT_PRIORITY)")]
    pub priority: Option<u8>,

    /// Number of lines of backtrace to keep if this job fails.
    ///
    /// Defaults to 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "Some(JOB_DEFAULT_BACKTRACE)")]
    pub backtrace: Option<usize>,

    /// Data about this job's most recent failure.
    ///
    /// This field is read-only.
    #[serde(skip_serializing)]
    #[builder(setter(skip))]
    failure: Option<Failure>,

    /// Extra context to include with the job.
    ///
    /// Faktory workers can have plugins and middleware which need to store additional context with
    /// the job payload. Faktory supports a custom hash to store arbitrary key/values in the JSON.
    /// This can be extremely helpful for cross-cutting concerns which should propagate between
    /// systems, e.g. locale for user-specific text translations, request_id for tracing execution
    /// across a complex distributed system, etc.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default = "HashMap::default")]
    #[builder(default = "HashMap::default()")]
    pub custom: HashMap<String, serde_json::Value>,
}

impl JobBuilder {
    /// Setter for the arguments provided for this job.
    pub fn args<A>(&mut self, args: Vec<A>) -> &mut Self
    where
        A: Into<serde_json::Value>,
    {
        self.args = Some(args.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Set arbitrary key-value pairs to this job's custom data hash
    pub fn add_to_custom_data(&mut self, k: String, v: impl Into<serde_json::Value>) -> &mut Self {
        let custom = self.custom.get_or_insert_with(HashMap::new);
        custom.insert(k, v.into());
        self
    }

    /// When Faktory should expire this job.
    ///
    /// Faktory Enterprise allows for expiring jobs. This is setter for "expires_at"
    /// field in the job's custom data.
    /// ```
    /// use faktory::JobBuilder;
    /// use chrono::{Duration, Utc};
    ///
    /// let _job = JobBuilder::default()
    ///     .kind("order")
    ///     .args(vec!["ISBN-13:9781718501850"])
    ///     .expires_at(Utc::now() + Duration::hours(1))
    ///     .build()
    ///     .unwrap();
    #[cfg(feature = "ent")]
    pub fn expires_at(&mut self, dt: DateTime<Utc>) -> &mut Self {
        self.add_to_custom_data(
            "expires_at".into(),
            dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
    }

    /// In what period of time from now (UTC) the Faktory should expire this job.
    ///
    /// Use this setter when you are unwilling to populate the "expires_at" field in custom
    /// options with some exact date and time, e.g.:
    /// ```
    /// use faktory::JobBuilder;
    /// use chrono::Duration;
    ///
    /// let _job = JobBuilder::default()
    ///     .kind("order")
    ///     .args(vec!["ISBN-13:9781718501850"])
    ///     .expires_in(Duration::weeks(1))
    ///     .build()
    ///     .unwrap();
    /// ```
    #[cfg(feature = "ent")]
    pub fn expires_in(&mut self, ttl: chrono::Duration) -> &mut Self {
        self.expires_at(Utc::now() + ttl)
    }

    /// How long the Faktory will not accept duplicates of this job.
    ///
    /// The job will be considered unique for kind-args-queue combination.
    /// Note that uniqueness is best-effort, rather than a guarantee. Check out
    /// the Enterprise Faktory docs for details on how scheduling, retries and other
    /// features live together with 'unique_for'.
    #[cfg(feature = "ent")]
    pub fn unique_for(&mut self, secs: usize) -> &mut Self {
        self.add_to_custom_data("unique_for".into(), secs)
    }

    /// Remove unique lock for this job right before the job starts executing.
    #[cfg(feature = "ent")]
    pub fn unique_until_start(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until".into(), "start")
    }

    /// Do not remove unique lock for this job until it successfully finishes.
    ///
    /// The Faktory's default value for "unique_until" is "default". This method
    /// exists on the JobBuilder for completeness
    #[cfg(feature = "ent")]
    pub fn unique_until_success(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until".into(), "success")
    }

    fn validate(&self) -> Result<(), String> {
        if let Some(ref priority) = self.priority {
            if *priority > Some(JOB_PRIORITY_MAX) {
                return Err("`priority` must be in the range from 0 to 9 inclusive".to_string());
            }
        }
        Ok(())
    }

    /// Builds a new job.
    pub fn build(&self) -> Result<Job, error::Client> {
        let job = self
            .try_build()
            .map_err(|err| error::Client::MalformedJob {
                desc: err.to_string(),
            })?;
        Ok(job)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Failure {
    retry_count: usize,
    failed_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "errtype")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backtrace: Option<Vec<String>>,
}

impl Job {
    /// Create a new job of type `kind`, with the given arguments.
    pub fn new<S, A>(kind: S, args: Vec<A>) -> Self
    where
        S: Into<String>,
        A: Into<serde_json::Value>,
    {
        Job {
            jid: utils::gen_random_jid(),
            queue: JOB_DEFAULT_QUEUE.into(),
            kind: kind.into(),
            args: args.into_iter().map(|s| s.into()).collect(),

            created_at: Some(Utc::now()),
            enqueued_at: None,
            at: None,
            reserve_for: Some(JOB_DEFAULT_RESERVED_FOR_SECS),
            retry: Some(JOB_DEFAULT_RETRY_COUNT),
            priority: Some(JOB_DEFAULT_PRIORITY),
            backtrace: Some(JOB_DEFAULT_BACKTRACE),
            failure: None,
            custom: Default::default(),
        }
    }

    /// Place this job on the given `queue`.
    ///
    /// If this method is not called (or `self.queue` set otherwise), the queue will be set to
    /// "default".
    pub fn on_queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.queue = queue.into();
        self
    }

    /// This job's id.
    pub fn id(&self) -> &str {
        &self.jid
    }

    /// This job's type.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The arguments provided for this job.
    pub fn args(&self) -> &[serde_json::Value] {
        &self.args
    }

    /// Data about this job's most recent failure.
    pub fn failure(&self) -> &Option<Failure> {
        &self.failure
    }
}

pub fn write_command<W: Write, C: FaktoryCommand>(w: &mut W, command: &C) -> Result<(), Error> {
    command.issue::<W>(w)?;
    Ok(w.flush()?)
}

pub fn write_command_and_await_ok<X: BufRead + Write, C: FaktoryCommand>(
    x: &mut X,
    command: &C,
) -> Result<(), Error> {
    write_command(x, command)?;
    read_ok(x)
}

#[cfg(test)]
mod test {
    use super::*;

    // Returns date and time string in the format expected by Faktory.
    // Serializes date and time into a string as per RFC 3338 and ISO 8601
    // with nanoseconds precision and 'Z' literal for the timzone column.
    fn to_iso_string(dt: DateTime<Utc>) -> String {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    }

    #[test]
    fn test_job_build_fails_if_kind_missing() {
        let job = JobBuilder::default()
            .args(vec!["ISBN-13:9781718501850"])
            .build();
        let err = job.unwrap_err();
        assert_eq!(
            err.to_string(),
            "job is malformed: `kind` must be initialized"
        )
    }

    #[test]
    fn test_job_build_fails_if_args_missing() {
        let job = JobBuilder::default().kind("order").build();
        let err = job.unwrap_err();
        assert_eq!(
            err.to_string(),
            "job is malformed: `args` must be initialized"
        );
    }

    #[test]
    fn test_job_build_fails_if_priority_invalid() {
        let job = JobBuilder::default()
            .kind("order")
            .args(vec!["ISBN-13:9781718501850"])
            .priority(JOB_PRIORITY_MAX + 1)
            .build();
        let err = job.unwrap_err();
        assert_eq!(
            err.to_string(),
            "job is malformed: `priority` must be in the range from 0 to 9 inclusive"
        );
    }

    #[test]
    fn test_job_can_be_created_with_builder() {
        let job_kind = "order";
        let job_args = vec!["ISBN-13:9781718501850"];
        let job = JobBuilder::default()
            .kind(job_kind)
            .args(job_args.clone())
            .build()
            .unwrap();

        assert!(job.jid != "".to_owned());
        assert!(job.queue == JOB_DEFAULT_QUEUE.to_string());
        assert_eq!(job.kind, job_kind);
        assert_eq!(job.args, job_args);
        assert!(job.created_at < Some(Utc::now()));
        assert!(job.enqueued_at.is_none());
        assert!(job.at.is_none());
        assert_eq!(job.reserve_for, Some(JOB_DEFAULT_RESERVED_FOR_SECS));
        assert_eq!(job.retry, Some(JOB_DEFAULT_RETRY_COUNT));
        assert_eq!(job.priority, Some(JOB_DEFAULT_PRIORITY));
        assert_eq!(job.backtrace, Some(JOB_DEFAULT_BACKTRACE));
        assert!(job.failure.is_none());
        assert_eq!(job.custom, HashMap::default());
    }

    #[test]
    fn test_method_mew_and_builder_align() {
        let job1 = Job::new("order", vec!["ISBN-13:9781718501850"]);
        let job2 = JobBuilder::default()
            .kind("order")
            .args(vec!["ISBN-13:9781718501850"])
            .build()
            .unwrap();

        assert_eq!(job1.kind, job2.kind);
        assert_eq!(job1.args, job2.args);
        assert_eq!(job1.queue, job2.queue);
        assert_eq!(job1.enqueued_at, job2.enqueued_at);
        assert_eq!(job1.at, job2.at);
        assert_eq!(job1.reserve_for, job2.reserve_for);
        assert_eq!(job1.retry, job2.retry);
        assert_eq!(job1.priority, job2.priority);
        assert_eq!(job1.backtrace, job2.backtrace);
        assert_eq!(job1.failure, job2.failure);
        assert_eq!(job1.custom, job2.custom);

        assert_ne!(job1.jid, job2.jid);
        assert_ne!(job1.created_at, job2.created_at);
    }

    fn half_stuff() -> JobBuilder {
        let mut job = JobBuilder::default();
        job.args(vec!["ISBN-13:9781718501850"]);
        job.kind("order");
        job
    }

    #[test]
    fn test_arbitrary_custom_data_setter() {
        let exp_at = Utc::now() + chrono::Duration::seconds(300);
        let exp_at_iso = to_iso_string(exp_at);
        let job = half_stuff()
            .add_to_custom_data("expires_at".into(), exp_at_iso.clone())
            .build()
            .unwrap();
        assert_eq!(
            job.custom.get("expires_at").unwrap(),
            &serde_json::Value::from(exp_at_iso)
        );
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_expiration_feature_for_enterprise_faktory() {
        let five_min = chrono::Duration::seconds(300);
        let exp_at = Utc::now() + five_min;
        let job1 = half_stuff().expires_at(exp_at).build().unwrap();
        let stored = job1.custom.get("expires_at").unwrap();
        assert_eq!(stored, &serde_json::Value::from(to_iso_string(exp_at)));

        let job2 = half_stuff().expires_in(five_min).build().unwrap();
        assert!(job2.custom.get("expires_at").is_some());
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_uniqueness_faeture_for_enterprise_faktory() {
        let job = half_stuff()
            .unique_for(60)
            .unique_until_start()
            .build()
            .unwrap();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        let stored_unique_until = job.custom.get("unique_until").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(60));
        assert_eq!(stored_unique_until, &serde_json::Value::from("start"));

        let job = half_stuff()
            .unique_for(60)
            .unique_until_success()
            .build()
            .unwrap();

        let stored_unique_until = job.custom.get("unique_until").unwrap();
        assert_eq!(stored_unique_until, &serde_json::Value::from("success"));
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_same_purpose_setters_applied_simultaneously() {
        let expires_at1 = Utc::now() + chrono::Duration::seconds(300);
        let expires_at2 = Utc::now() + chrono::Duration::seconds(300);
        let job = half_stuff()
            .unique_for(60)
            .add_to_custom_data("unique_for".into(), 600)
            .unique_for(40)
            .add_to_custom_data("expires_at".into(), to_iso_string(expires_at1))
            .expires_at(expires_at2)
            .build()
            .unwrap();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(40));
        let stored_expires_at = job.custom.get("expires_at").unwrap();
        assert_eq!(
            stored_expires_at,
            &serde_json::Value::from(to_iso_string(expires_at2))
        )
    }
}
