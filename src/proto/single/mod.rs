use chrono::{DateTime, Utc};
use derive_builder::Builder;
use std::collections::HashMap;
use std::io::prelude::*;

mod cmd;
mod resp;
mod utils;

use crate::error::Error;

pub use self::cmd::*;
pub use self::resp::*;
pub use self::utils::{gen_random_jid, gen_random_wid};

const JOB_DEFAULT_QUEUE: &str = "default";
const JOB_DEFAULT_RESERVED_FOR_SECS: usize = 600;
const JOB_DEFAULT_RETRY_COUNT: usize = 25;
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
/// let _job = JobBuilder::new("order")
///     .args(vec!["ISBN-13:9781718501850"])
///     .build();
/// ```
///
/// Equivalently:
/// ```
/// use faktory::Job;
///
/// let _job = Job::builder("order")
///     .args(vec!["ISBN-13:9781718501850"])
///     .build();
/// ```
///
/// In case no arguments are expected 'on the other side', you can simply go with:
/// ```
/// use faktory::Job;
///
/// let _job = Job::builder("rebuild_index").build();
/// ```
///
/// See also the [Faktory wiki](https://github.com/contribsys/faktory/wiki/The-Job-Payload).
#[derive(Serialize, Deserialize, Debug, Builder)]
#[builder(
    custom_constructor,
    setter(into),
    build_fn(name = "try_build", private)
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
    #[builder(setter(custom))]
    pub(crate) kind: String,

    /// The arguments provided for this job.
    #[builder(setter(custom), default = "Vec::new()")]
    pub(crate) args: Vec<serde_json::Value>,

    /// When this job was created.
    // note that serializing works correctly here since the default chrono serialization
    // is RFC3339, which is also what Faktory expects.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "Some(Utc::now())")]
    pub created_at: Option<DateTime<Utc>>,

    /// When this job was supplied to the Faktory server.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(skip))]
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
    /// Create a new instance of 'JobBuilder'
    pub fn new(kind: impl Into<String>) -> JobBuilder {
        JobBuilder {
            kind: Some(kind.into()),
            ..JobBuilder::create_empty()
        }
    }

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
    /// The Faktory's default value for "unique_until" is "success". This method
    /// exists on the JobBuilder for completeness.
    #[cfg(feature = "ent")]
    pub fn unique_until_success(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until".into(), "success")
    }

    /// Builds a new `Job`
    pub fn build(&self) -> Job {
        self.try_build()
            .expect("All required fields have been set.")
    }

    /// Builds a new _trackable_ `Job``.
    ///
    /// Progress update can be sent and received only for the jobs that have
    /// been explicitly marked as trackable via `"track":1` in the job's
    /// custom hash.
    /// ```
    /// use faktory::JobBuilder;
    ///
    /// let _job = JobBuilder::new("order")
    ///     .args(vec!["ISBN-13:9781718501850"])
    ///     .build_trackable();
    /// ```
    #[cfg(feature = "ent")]
    pub fn build_trackable(&mut self) -> Job {
        self.add_to_custom_data("track".into(), 1);
        self.build()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
        JobBuilder::new(kind).args(args).build()
    }

    /// Create an instance of JobBuilder with 'kind' already set.
    ///
    /// Equivalent to 'JobBuilder::new'
    pub fn builder<S: Into<String>>(kind: S) -> JobBuilder {
        JobBuilder::new(kind)
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

#[cfg(feature = "ent")]
pub use self::utils::parse_datetime;

/// Info on job execution progress (retrieved).
///
/// The tracker is guaranteed to get the following details: the job's id (though
/// they should know it beforehand in order to be ably to track the job), its last
/// know state (e.g."enqueued", "working", "success", "unknown") and the date and time
/// the job was last updated. Additionally, information on what's going on with the job
/// ([desc](struct.ProgressUpdate.html#structfield.desc)) and completion percentage
/// ([percent](struct.ProgressUpdate.html#structfield.percent)) may be available,
/// if the worker provided those details.
#[cfg(feature = "ent")]
#[derive(Debug, Clone, Deserialize)]
pub struct Progress {
    /// Id of the tracked job.
    pub jid: String,

    /// Job's state.
    pub state: String,

    /// When this job was last updated.
    #[serde(deserialize_with = "parse_datetime")]
    pub updated_at: Option<DateTime<Utc>>,

    /// Percentage of the job's completion.
    pub percent: Option<u8>,

    /// Arbitrary description that may be useful to whoever is tracking the job's progress.
    pub desc: Option<String>,
}

/// Info on job execution progress (sent).
///
/// In Enterprise Faktory, a client executing a job can report on the execution
/// progress, provided the job is trackable. A trackable job is the one with "track":1
/// specified in the custom data hash.
#[cfg(feature = "ent")]
#[derive(Debug, Clone, Serialize, Builder)]
#[builder(
    custom_constructor,
    setter(into),
    build_fn(name = "try_build", private)
)]
pub struct ProgressUpdate {
    /// Id of the tracked job.
    #[builder(setter(custom))]
    pub jid: String,

    /// Percentage of the job's completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub percent: Option<u8>,

    /// Arbitrary description that may be useful to whoever is tracking the job's progress.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub desc: Option<String>,

    /// Allows to extend the job's reservation, if more time needed to execute it.
    ///
    /// Note that you cannot decrease the initial [reservation](struct.Job.html#structfield.reserve_for).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub reserve_until: Option<DateTime<Utc>>,
}

#[cfg(feature = "ent")]
impl ProgressUpdate {
    /// Create a new instance of `ProgressUpdateBuilder` with job ID already set.
    ///
    /// Equivalent to creating a [new](struct.ProgressUpdateBuilder.html#method.new)
    /// `ProgressUpdateBuilder`.
    pub fn builder(jid: impl Into<String>) -> ProgressUpdateBuilder {
        ProgressUpdateBuilder {
            jid: Some(jid.into()),
            ..ProgressUpdateBuilder::create_empty()
        }
    }

    /// Create a new instance of `ProgressUpdate`.
    ///
    /// While job ID is specified at `ProgressUpdate`'s creation time,
    /// the rest of the [fields](struct.ProgressUpdate.html) are defaulted to _None_.
    pub fn new(jid: impl Into<String>) -> ProgressUpdate {
        ProgressUpdateBuilder::new(jid).build()
    }
}

#[cfg(feature = "ent")]
impl ProgressUpdateBuilder {
    /// Builds an instance of ProgressUpdate.
    pub fn build(&self) -> ProgressUpdate {
        self.try_build()
            .expect("All required fields have been set.")
    }

    /// Create a new instance of 'JobBuilder'
    pub fn new(jid: impl Into<String>) -> ProgressUpdateBuilder {
        ProgressUpdateBuilder {
            jid: Some(jid.into()),
            ..ProgressUpdateBuilder::create_empty()
        }
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
    fn test_job_can_be_created_with_builder() {
        let job_kind = "order";
        let job_args = vec!["ISBN-13:9781718501850"];
        let job = JobBuilder::new(job_kind).args(job_args.clone()).build();

        assert!(job.jid != "".to_owned());
        assert!(job.queue == JOB_DEFAULT_QUEUE.to_string());
        assert_eq!(job.kind, job_kind);
        assert_eq!(job.args, job_args);

        assert!(job.created_at.is_some());
        assert!(job.created_at < Some(Utc::now()));

        assert!(job.enqueued_at.is_none());
        assert!(job.at.is_none());
        assert_eq!(job.reserve_for, Some(JOB_DEFAULT_RESERVED_FOR_SECS));
        assert_eq!(job.retry, Some(JOB_DEFAULT_RETRY_COUNT));
        assert_eq!(job.priority, Some(JOB_DEFAULT_PRIORITY));
        assert_eq!(job.backtrace, Some(JOB_DEFAULT_BACKTRACE));
        assert!(job.failure.is_none());
        assert_eq!(job.custom, HashMap::default());

        let job = JobBuilder::new(job_kind).build();
        assert!(job.args.is_empty());
    }

    #[test]
    fn test_all_job_creation_variants_align() {
        let job1 = Job::new("order", vec!["ISBN-13:9781718501850"]);
        let job2 = JobBuilder::new("order")
            .args(vec!["ISBN-13:9781718501850"])
            .build();
        assert_eq!(job1.kind, job2.kind);
        assert_eq!(job1.args, job2.args);
        assert_eq!(job1.queue, job2.queue);
        assert_eq!(job1.enqueued_at, job2.enqueued_at);
        assert_eq!(job1.at, job2.at);
        assert_eq!(job1.reserve_for, job2.reserve_for);
        assert_eq!(job1.retry, job2.retry);
        assert_eq!(job1.priority, job2.priority);
        assert_eq!(job1.backtrace, job2.backtrace);
        assert_eq!(job1.custom, job2.custom);

        assert_ne!(job1.jid, job2.jid);
        assert_ne!(job1.created_at, job2.created_at);

        let job3 = Job::builder("order")
            .args(vec!["ISBN-13:9781718501850"])
            .build();
        assert_eq!(job2.kind, job3.kind);
        assert_eq!(job1.args, job2.args);

        assert_ne!(job2.jid, job3.jid);
        assert_ne!(job2.created_at, job3.created_at);
    }

    fn half_stuff() -> JobBuilder {
        let mut job = JobBuilder::new("order");
        job.args(vec!["ISBN-13:9781718501850"]);
        job
    }

    #[test]
    fn test_arbitrary_custom_data_setter() {
        let exp_at = Utc::now() + chrono::Duration::seconds(300);
        let exp_at_iso = to_iso_string(exp_at);
        let job = half_stuff()
            .add_to_custom_data("expires_at".into(), exp_at_iso.clone())
            .build();
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
        let job1 = half_stuff().expires_at(exp_at).build();
        let stored = job1.custom.get("expires_at").unwrap();
        assert_eq!(stored, &serde_json::Value::from(to_iso_string(exp_at)));

        let job2 = half_stuff().expires_in(five_min).build();
        assert!(job2.custom.get("expires_at").is_some());
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_uniqueness_faeture_for_enterprise_faktory() {
        let job = half_stuff().unique_for(60).unique_until_start().build();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        let stored_unique_until = job.custom.get("unique_until").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(60));
        assert_eq!(stored_unique_until, &serde_json::Value::from("start"));

        let job = half_stuff().unique_for(60).unique_until_success().build();

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
            .build();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(40));
        let stored_expires_at = job.custom.get("expires_at").unwrap();
        assert_eq!(
            stored_expires_at,
            &serde_json::Value::from(to_iso_string(expires_at2))
        )
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_buid_trackable_job() {
        let job = JobBuilder::new("thumbnail")
            .args(vec!["https://provider.io/bucket/key/"])
            .build_trackable();
        assert_eq!(job.custom.get("track").unwrap(), 1);

        // the JobBuilder's terminal method 'new_trackable'
        // will set 'track' to 1 on custom data:
        let mut custom_data = HashMap::new();
        custom_data.insert("lib".into(), serde_json::Value::from("sharp@1"));
        custom_data.insert("track".into(), serde_json::Value::from("whatever"));

        let job = JobBuilder::new("thumbnail")
            .args(vec!["https://provider.io/bucket/key/"])
            .custom(custom_data)
            .add_to_custom_data("track".into(), "2")
            .add_to_custom_data("quality".into(), 80)
            .build_trackable();

        // and still 'track:1' ...
        assert_eq!(job.custom.get("track").unwrap(), 1);

        // ... while the rest of custom data is intact:
        assert_eq!(
            job.custom.get("lib").unwrap(),
            &serde_json::Value::from("sharp@1")
        );
        assert_eq!(
            job.custom.get("quality").unwrap(),
            &serde_json::Value::from(80)
        );
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_progress_update_can_be_created_with_builder() {
        let tracked = utils::gen_random_jid();
        let progress1 = ProgressUpdateBuilder::new(&tracked).build();

        assert_eq!(progress1.jid, tracked);
        assert!(progress1.desc.is_none());
        assert!(progress1.percent.is_none());
        assert!(progress1.reserve_until.is_none());

        let progress2 = ProgressUpdate::new(&tracked);
        assert_eq!(progress1.desc, progress2.desc);
        assert_eq!(progress1.percent, progress2.percent);
        assert_eq!(progress1.reserve_until, progress2.reserve_until);
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_progress_builder_serialized_correctly() {
        let tracked = utils::gen_random_jid();
        let extra_time_needed = chrono::Duration::nanoseconds(111_111_111);
        let extend = Utc::now() + extra_time_needed;

        let progress = ProgressUpdateBuilder::new(&tracked)
            .desc("Resizing the image...".to_string())
            .percent(67)
            .reserve_until(extend.clone())
            .build();

        let serialized = serde_json::to_string(&progress).unwrap();
        assert!(serialized.contains("jid"));
        assert!(serialized.contains(&tracked));

        assert!(serialized.contains("desc"));
        assert!(serialized.contains("Resizing the image..."));

        assert!(serialized.contains("percent"));
        assert!(serialized.contains("67"));

        assert!(serialized.contains("reserve_until"));
        assert!(serialized.contains(&to_iso_string(extend)));

        let progress = ProgressUpdate::builder(&tracked).build();
        let serialized = serde_json::to_string(&progress).unwrap();

        assert!(serialized.contains("jid"));
        assert!(serialized.contains(&tracked));

        assert!(!serialized.contains("percent"));
        assert!(!serialized.contains("desc"));
        assert!(!serialized.contains("reserve_until"));
    }

    #[test]
    #[cfg(feature = "ent")]
    fn test_progress_deserialized_correctly() {
        let raw = b"
        {
            \"jid\":\"W8qyVle9vXzUWQOf\",
            \"updated_at\":\"2023-12-13T21:01:35.244381344Z\",
            \"state\":\"working\",
            \"desc\":\"Resizing the image...\",
            \"percent\":88
        }";

        let progress = serde_json::from_slice::<Progress>(raw).unwrap();
        assert_eq!(progress.jid, "W8qyVle9vXzUWQOf");
        assert_eq!(
            progress.updated_at,
            Some(
                DateTime::parse_from_rfc3339("2023-12-13T21:01:35.244381344Z")
                    .unwrap()
                    .into()
            )
        );
        assert_eq!(progress.state, "working");
        assert_eq!(progress.desc, Some("Resizing the image...".into()));
        assert_eq!(progress.percent, Some(88));

        let raw = b"
        {
            \"jid\":\"44124a8gs87722qaa\",
            \"updated_at\":\"1970-12-13T06:52:27.765011869Z\",
            \"state\":\"unknown\"
        }";

        let progress = serde_json::from_slice::<Progress>(raw).unwrap();
        assert_eq!(progress.jid, "44124a8gs87722qaa");
        assert_eq!(
            progress.updated_at,
            Some(
                DateTime::parse_from_rfc3339("1970-12-13T06:52:27.765011869Z")
                    .unwrap()
                    .into()
            )
        );
        assert_eq!(progress.state, "unknown");
        assert_eq!(progress.desc, None);
        assert_eq!(progress.percent, None);

        // empty string for 'updated_at' is fine:
        let raw = b"
        {
            \"jid\":\"44124a8gs87722qaa\",
            \"updated_at\":\"\",
            \"state\":\"unknown\"
        }";
        let progress = serde_json::from_slice::<Progress>(raw).unwrap();
        assert_eq!(progress.jid, "44124a8gs87722qaa");
        assert_eq!(progress.state, "unknown");
        assert_eq!(progress.updated_at, None);

        // but the field cannot be missing:
        let raw = b"
        {
            \"jid\":\"44124a8gs87722qaa\",
            \"state\":\"unknown\"
        }";
        let error = serde_json::from_slice::<Progress>(raw).unwrap_err();
        assert!(error.to_string().contains("missing field `updated_at`"));

        // neither can it be anything else rather than string:
        let raw = b"
        {
            \"jid\":\"44124a8gs87722qaa\",
            \"updated_at\":1,
            \"state\":\"unknown\"
        }";
        let error = serde_json::from_slice::<Progress>(raw).unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid type: integer `1`, expected a string"));
    }
}
