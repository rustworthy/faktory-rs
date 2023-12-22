use derive_builder::Builder;

use crate::Job;

/// Batch of jobs.
///
/// Faktory guarantees a callback (`success` and/or `failure`) will be triggered after the execution
/// of all the jobs belonging to the same batch has finished (successfully or with errors accordingly).
/// Batches can be nested. They can also be re-opened, but - once a batch is committed - only those jobs
/// that belong to this batch can re-open it.
///
/// An empty batch can be committed just fine. That will make Faktory immediately fire a callback (i.e. put
/// the job specified in `complete` and/or the one specified in `success` onto the queues).
///
/// If you open a batch, but - for some reason - do not commit it within _30 minutes_, it will simply expire
/// on the Faktory server (which means no callbackes will be fired).
#[derive(Serialize, Debug, Builder)]
#[builder(
    custom_constructor,
    setter(into),
    build_fn(name = "try_build", private)
)]
pub struct Batch {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(skip))]
    parent_bid: Option<String>,

    /// Batch description for Faktory WEB UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// On success callback.
    ///
    /// This job will be queued by the Faktory server provided
    /// all the jobs belonging to this batch have been executed successfully.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
    pub(crate) success: Option<Job>,

    /// On complete callback.
    ///
    /// This job will be queue by the Faktory server after all the jobs
    /// belonging to this batch have been executed, even if one/some/all
    /// of the workers have failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
    pub(crate) complete: Option<Job>,
}

impl Batch {
    /// Create a new `BatchBuilder`.
    pub fn builder() -> BatchBuilder {
        BatchBuilder::create_empty()
    }
}

impl BatchBuilder {
    fn build(&mut self) -> Batch {
        self.try_build()
            .expect("All required fields have been set.")
    }

    /// Create a new `BatchBuilder` with optional description of the batch.
    pub fn new(description: impl Into<Option<String>>) -> BatchBuilder {
        BatchBuilder {
            description: Some(description.into()),
            ..Self::create_empty()
        }
    }

    fn success(&mut self, success_cb: impl Into<Option<Job>>) -> &mut Self {
        self.success = Some(success_cb.into());
        self
    }

    fn complete(&mut self, complete_cb: impl Into<Option<Job>>) -> &mut Self {
        self.complete = Some(complete_cb.into());
        self
    }

    /// Create a `Batch` with only `success` callback specified.
    pub fn with_success_callback(&mut self, success_cb: Job) -> Batch {
        self.success(success_cb);
        self.complete(None);
        self.build()
    }

    /// Create a `Batch` with only `complete` callback specified.
    pub fn with_complete_callback(&mut self, complete_cb: Job) -> Batch {
        self.complete(complete_cb);
        self.success(None);
        self.build()
    }

    /// Create a `Batch` with both `success` and `complete` callbacks specified.
    pub fn with_callbacks(&mut self, success_cb: Job, complete_cb: Job) -> Batch {
        self.success(success_cb);
        self.complete(complete_cb);
        self.build()
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn test_batch_creation() {
        let b = BatchBuilder::new("Image processing batch".to_string())
            .with_success_callback(Job::builder("thumbnail").build());
        assert!(b.complete.is_none());
        assert!(b.parent_bid.is_none());
        assert!(b.success.is_some());
        assert_eq!(b.description, Some("Image processing batch".into()));

        let b = BatchBuilder::new("Image processing batch".to_string())
            .with_complete_callback(Job::builder("thumbnail").build());
        assert!(b.complete.is_some());
        assert!(b.success.is_none());

        let b = BatchBuilder::new(None).with_callbacks(
            Job::builder("thumbnail").build(),
            Job::builder("thumbnail").build(),
        );
        assert!(b.description.is_none());
        assert!(b.complete.is_some());
        assert!(b.success.is_some());
    }

    #[test]
    fn test_batch_serialized_correctly() {
        let prepare_test_job = |jobtype: String| {
            let jid = "LFluKy1Baak83p54";
            let dt = "2023-12-22T07:00:52.546258624Z";
            let created_at = DateTime::<Utc>::from_str(dt).unwrap();
            Job::builder(jobtype)
                .jid(jid)
                .created_at(created_at)
                .build()
        };

        // with description and on success callback:
        let got = serde_json::to_string(
            &BatchBuilder::new("Image processing workload".to_string())
                .with_success_callback(prepare_test_job("thumbnail_clean_up".into())),
        )
        .unwrap();
        let expected = r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#;
        assert_eq!(got, expected);

        // without description and with on complete callback:
        let got = serde_json::to_string(
            &BatchBuilder::new(None)
                .with_complete_callback(prepare_test_job("thumbnail_info".into())),
        )
        .unwrap();
        let expected = r#"{"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#;
        assert_eq!(got, expected);

        // with description and both callbacks:
        let got = serde_json::to_string(
            &BatchBuilder::new("Image processing workload".to_string()).with_callbacks(
                prepare_test_job("thumbnail_clean_up".into()),
                prepare_test_job("thumbnail_info".into()),
            ),
        )
        .unwrap();
        let expected = r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0},"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#;
        assert_eq!(got, expected);
    }
}
