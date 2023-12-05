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

const JOB_DEFAULT_RESERVED_FOR_SECS: usize = 600;
const JOB_DEFAULT_RETRIES_COUNT: usize = 25;
const JOB_PRIORITY_MAX: u8 = 9;
const JOB_DEFAULT_PRIORITY: u8 = 5;

/// A Faktory job.
///
/// See also the [Faktory wiki](https://github.com/contribsys/faktory/wiki/The-Job-Payload).
#[derive(Serialize, Deserialize, Debug, Builder)]
#[builder(
    setter(into),
    build_fn(name = "try_build", validate = "Self::validate")
)]
pub struct Job {
    /// The job's unique identifier.
    #[builder(default = "utils::gen_random_jid()")]
    pub(crate) jid: String,

    /// The queue this job belongs to. Usually `default`.
    #[builder(default = r#"String::from("default")"#)]
    pub queue: String,

    /// The job's type. Called `kind` because `type` is reserved.
    #[serde(rename = "jobtype")]
    pub(crate) kind: String,

    /// The arguments provided for this job.
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
    #[builder(default = "Some(JOB_DEFAULT_RETRIES_COUNT)")]
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
    pub backtrace: Option<usize>,

    /// Data about this job's most recent failure.
    ///
    /// This field is read-only.
    #[serde(skip_serializing)]
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
    pub custom: HashMap<String, serde_json::Value>,
}

impl JobBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(ref priority) = self.priority {
            if *priority > Some(JOB_PRIORITY_MAX) {
                return Err("`priority` must be in the range from 0 to 9 inclusive".to_string());
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn build(&self) -> Result<Job, error::Client> {
        let job = self
            .try_build()
            .map_err(|err| error::Client::MalformedJob {
                desc: err.to_string(),
            })?;
        Ok(job)
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
        Job {
            jid: utils::gen_random_jid(),
            queue: "default".into(),
            kind: kind.into(),
            args: args.into_iter().map(|s| s.into()).collect(),

            created_at: Some(Utc::now()),
            enqueued_at: None,
            at: None,
            reserve_for: Some(600),
            retry: Some(25),
            priority: Some(5),
            backtrace: Some(0),
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

    #[test]
    fn test_job_build_fails_if_kind_missing() {
        let job = JobBuilder::default()
            .args(vec![serde_json::Value::from("ISBN-13:9781718501850")])
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
            .args(vec![])
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
        let job = JobBuilder::default()
            .kind("order")
            .args(vec![serde_json::Value::from("ISBN-13:9781718501850")])
            .build();

        let err = job.unwrap_err();
        assert_eq!(
            err.to_string(),
            "job is malformed: `backtrace` must be initialized"
        )
    }
}
