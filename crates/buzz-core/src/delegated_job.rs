//! Normative delegated-job event schema and lifecycle reconstruction.
//!
//! Job events are append-only. A request is only a proposed delegation; the
//! target agent's signed acceptance is the executable-work boundary.

use nostr::Event;
use uuid::Uuid;

use crate::kind::{
    event_kind_u32, KIND_JOB_ACCEPTED, KIND_JOB_BLOCKED, KIND_JOB_COMPLETED, KIND_JOB_DELEGATED,
    KIND_JOB_REJECTED, KIND_JOB_REQUEST,
};

/// Maximum UTF-8 assignment/reason content stored in a job event.
pub const MAX_JOB_CONTENT_BYTES: usize = 64 * 1024;

/// Parsed request identity, immutable for the life of the job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRequest {
    /// Stable UUID selected by the requester.
    pub job_id: Uuid,
    /// Signed request event ID.
    pub request_event_id: String,
    /// Requester pubkey from the signed envelope.
    pub requester: String,
    /// Exactly one managed target agent from `job-target`.
    pub target_agent: String,
    /// Originating channel from the single `h` tag.
    pub channel_id: Uuid,
    /// Assignment content carried by the request.
    pub assignment: String,
}

/// Lifecycle event type after the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobAction {
    /// Target accepts responsibility; non-terminal.
    Accepted,
    /// Target declines before acceptance; terminal.
    Rejected,
    /// Accepted work completed; terminal BTOM disposition.
    Completed,
    /// Accepted work cannot currently proceed; terminal BTOM disposition.
    Blocked,
    /// Accepted work was transferred to a successor; terminal BTOM disposition.
    Delegated,
}

impl JobAction {
    /// Map a delegated-job event kind to its lifecycle action.
    pub fn from_kind(kind: u32) -> Option<Self> {
        match kind {
            KIND_JOB_ACCEPTED => Some(Self::Accepted),
            KIND_JOB_REJECTED => Some(Self::Rejected),
            KIND_JOB_COMPLETED => Some(Self::Completed),
            KIND_JOB_BLOCKED => Some(Self::Blocked),
            KIND_JOB_DELEGATED => Some(Self::Delegated),
            _ => None,
        }
    }

    /// Whether this action is terminal.
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Accepted)
    }
}

/// Parsed lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobLifecycleEvent {
    /// Stable job UUID.
    pub job_id: Uuid,
    /// Referenced immutable request event ID.
    pub request_event_id: String,
    /// Immediate predecessor event ID in the single legal lifecycle chain.
    pub parent_event_id: String,
    /// Originating channel.
    pub channel_id: Uuid,
    /// Event signer, required to be the target agent by relay policy.
    pub author: String,
    /// State transition.
    pub action: JobAction,
    /// Required successor target for delegated/transferred; absent otherwise.
    pub successor_agent: Option<String>,
    /// Optional structured human-readable reason/result.
    pub content: String,
}

/// Materialized current job state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Proposed delegation; target has not accepted.
    Requested,
    /// Target accepted responsibility.
    Accepted,
    /// Target declined responsibility.
    Rejected,
    /// Accepted job completed.
    Completed,
    /// Accepted job blocked.
    Blocked,
    /// Accepted job delegated/transferred.
    Delegated,
}

impl JobState {
    /// Stable storage/wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::Delegated => "delegated/transferred",
        }
    }

    /// Parse a persisted state label.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "requested" => Some(Self::Requested),
            "accepted" => Some(Self::Accepted),
            "rejected" => Some(Self::Rejected),
            "completed" => Some(Self::Completed),
            "blocked" => Some(Self::Blocked),
            "delegated/transferred" => Some(Self::Delegated),
            _ => None,
        }
    }

    /// Whether the job can no longer transition.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Rejected | Self::Completed | Self::Blocked | Self::Delegated
        )
    }

    /// Apply a lifecycle action, rejecting illegal orderings.
    pub fn apply(self, action: JobAction) -> Result<Self, JobProtocolError> {
        match (self, action) {
            (Self::Requested, JobAction::Accepted) => Ok(Self::Accepted),
            (Self::Requested, JobAction::Rejected) => Ok(Self::Rejected),
            (Self::Accepted, JobAction::Completed) => Ok(Self::Completed),
            (Self::Accepted, JobAction::Blocked) => Ok(Self::Blocked),
            (Self::Accepted, JobAction::Delegated) => Ok(Self::Delegated),
            _ => Err(JobProtocolError::IllegalTransition {
                state: self,
                action,
            }),
        }
    }
}

/// Delegated-job schema or transition error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JobProtocolError {
    /// Event kind is outside the delegated-job protocol.
    #[error("not a delegated-job event")]
    WrongKind,
    /// Required tag is missing, duplicated, malformed, or contradictory.
    #[error("invalid job envelope: {0}")]
    InvalidEnvelope(String),
    /// Lifecycle transition is illegal.
    #[error("illegal job transition: {state:?} + {action:?}")]
    IllegalTransition {
        /// Existing materialized state.
        state: JobState,
        /// Requested action.
        action: JobAction,
    },
}

/// Parse and validate a job request envelope.
pub fn parse_job_request(event: &Event) -> Result<JobRequest, JobProtocolError> {
    if event_kind_u32(event) != KIND_JOB_REQUEST {
        return Err(JobProtocolError::WrongKind);
    }
    validate_content(&event.content, true)?;
    let job_id = exactly_one_tag(event, "d")?
        .parse::<Uuid>()
        .map_err(|_| JobProtocolError::InvalidEnvelope("d must be a job UUID".into()))?;
    let target_agent = validate_pubkey(exactly_one_tag(event, "job-target")?, "job-target")?;
    let routed_target = validate_pubkey(exactly_one_tag(event, "p")?, "p")?;
    if routed_target != target_agent {
        return Err(JobProtocolError::InvalidEnvelope(
            "p and job-target must identify the same target agent".into(),
        ));
    }
    let channel_id = exactly_one_tag(event, "h")?
        .parse::<Uuid>()
        .map_err(|_| JobProtocolError::InvalidEnvelope("h must be a channel UUID".into()))?;
    forbid_tags(event, &["job-request", "job-parent", "job-successor"])?;
    Ok(JobRequest {
        job_id,
        request_event_id: event.id.to_hex(),
        requester: event.pubkey.to_hex(),
        target_agent,
        channel_id,
        assignment: event.content.clone(),
    })
}

/// Parse and validate one post-request lifecycle envelope.
pub fn parse_job_lifecycle(event: &Event) -> Result<JobLifecycleEvent, JobProtocolError> {
    let action = JobAction::from_kind(event_kind_u32(event)).ok_or(JobProtocolError::WrongKind)?;
    validate_content(&event.content, false)?;
    let job_id = exactly_one_tag(event, "d")?
        .parse::<Uuid>()
        .map_err(|_| JobProtocolError::InvalidEnvelope("d must be a job UUID".into()))?;
    let request_event_id =
        validate_event_id(exactly_one_tag(event, "job-request")?, "job-request")?;
    let parent_event_id = validate_event_id(exactly_one_tag(event, "job-parent")?, "job-parent")?;
    let channel_id = exactly_one_tag(event, "h")?
        .parse::<Uuid>()
        .map_err(|_| JobProtocolError::InvalidEnvelope("h must be a channel UUID".into()))?;
    forbid_tags(event, &["job-target", "p"])?;
    let successor_values = tag_values(event, "job-successor")?;
    let successor_agent = match action {
        JobAction::Delegated => {
            if successor_values.len() != 1 {
                return Err(JobProtocolError::InvalidEnvelope(
                    "delegated event requires exactly one job-successor".into(),
                ));
            }
            Some(validate_pubkey(
                successor_values[0].clone(),
                "job-successor",
            )?)
        }
        _ => {
            if !successor_values.is_empty() {
                return Err(JobProtocolError::InvalidEnvelope(
                    "job-successor is only valid on delegated events".into(),
                ));
            }
            None
        }
    };
    Ok(JobLifecycleEvent {
        job_id,
        request_event_id,
        parent_event_id,
        channel_id,
        author: event.pubkey.to_hex(),
        action,
        successor_agent,
        content: event.content.clone(),
    })
}

fn validate_content(content: &str, required: bool) -> Result<(), JobProtocolError> {
    if content.len() > MAX_JOB_CONTENT_BYTES {
        return Err(JobProtocolError::InvalidEnvelope(
            "content too large".into(),
        ));
    }
    if required && content.trim().is_empty() {
        return Err(JobProtocolError::InvalidEnvelope(
            "request assignment content must not be empty".into(),
        ));
    }
    Ok(())
}

fn tag_values(event: &Event, name: &str) -> Result<Vec<String>, JobProtocolError> {
    let mut values = Vec::new();
    for tag in event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().map(String::as_str) == Some(name))
    {
        let parts = tag.as_slice();
        if parts.len() != 2 {
            return Err(JobProtocolError::InvalidEnvelope(format!(
                "{name} tag must have exactly two elements"
            )));
        }
        values.push(parts[1].clone());
    }
    Ok(values)
}

fn exactly_one_tag(event: &Event, name: &str) -> Result<String, JobProtocolError> {
    let values = tag_values(event, name)?;
    if values.len() != 1 {
        return Err(JobProtocolError::InvalidEnvelope(format!(
            "expected exactly one {name} tag"
        )));
    }
    Ok(values[0].clone())
}

fn forbid_tags(event: &Event, names: &[&str]) -> Result<(), JobProtocolError> {
    for name in names {
        if !tag_values(event, name)?.is_empty() {
            return Err(JobProtocolError::InvalidEnvelope(format!(
                "{name} tag is not valid on this event"
            )));
        }
    }
    Ok(())
}

fn validate_pubkey(value: String, label: &str) -> Result<String, JobProtocolError> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(JobProtocolError::InvalidEnvelope(format!(
            "{label} must be a 64-character hex pubkey"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

fn validate_event_id(value: String, label: &str) -> Result<String, JobProtocolError> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(JobProtocolError::InvalidEnvelope(format!(
            "{label} must be a 64-character hex event ID"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    fn request(tags: Vec<Tag>) -> Event {
        EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), "implement it")
            .tags(tags)
            .sign_with_keys(&Keys::generate())
            .expect("sign")
    }

    fn base_tags(target: &str) -> Vec<Tag> {
        vec![
            Tag::parse(["d", &Uuid::nil().to_string()]).expect("tag"),
            Tag::parse(["job-target", target]).expect("tag"),
            Tag::parse(["p", target]).expect("tag"),
            Tag::parse(["h", &Uuid::nil().to_string()]).expect("tag"),
        ]
    }

    #[test]
    fn request_requires_exactly_one_valid_target_and_job_id() {
        let target = "ab".repeat(32);
        assert!(parse_job_request(&request(base_tags(&target))).is_ok());
        let mut zero = base_tags(&target);
        zero.retain(|tag| tag.as_slice()[0] != "job-target");
        assert!(parse_job_request(&request(zero)).is_err());
        let mut many = base_tags(&target);
        many.push(Tag::parse(["job-target", &"cd".repeat(32)]).expect("tag"));
        assert!(parse_job_request(&request(many)).is_err());
        let mut bad_id = base_tags(&target);
        bad_id[0] = Tag::parse(["d", "not-a-uuid"]).expect("tag");
        assert!(parse_job_request(&request(bad_id)).is_err());
    }

    #[test]
    fn lifecycle_state_machine_matches_btom_boundary() {
        assert_eq!(
            JobState::Requested.apply(JobAction::Accepted),
            Ok(JobState::Accepted)
        );
        assert_eq!(
            JobState::Requested.apply(JobAction::Rejected),
            Ok(JobState::Rejected)
        );
        for (action, state) in [
            (JobAction::Completed, JobState::Completed),
            (JobAction::Blocked, JobState::Blocked),
            (JobAction::Delegated, JobState::Delegated),
        ] {
            assert_eq!(JobState::Accepted.apply(action), Ok(state));
            assert!(state.is_terminal());
            assert!(JobState::Requested.apply(action).is_err());
        }
        assert!(JobState::Completed.apply(JobAction::Accepted).is_err());
        assert!(JobState::Accepted.apply(JobAction::Accepted).is_err());
    }

    fn lifecycle(kind: u32, tags: Vec<Tag>) -> Event {
        EventBuilder::new(Kind::Custom(kind as u16), "reason")
            .tags(tags)
            .sign_with_keys(&Keys::generate())
            .expect("sign")
    }

    fn lifecycle_tags() -> Vec<Tag> {
        vec![
            Tag::parse(["d", &Uuid::nil().to_string()]).expect("tag"),
            Tag::parse(["job-request", &"ab".repeat(32)]).expect("tag"),
            Tag::parse(["job-parent", &"ab".repeat(32)]).expect("tag"),
            Tag::parse(["h", &Uuid::nil().to_string()]).expect("tag"),
        ]
    }

    #[test]
    fn lifecycle_requires_immutable_request_and_channel_coordinates() {
        let valid = lifecycle(KIND_JOB_ACCEPTED, lifecycle_tags());
        assert!(parse_job_lifecycle(&valid).is_ok());

        let mut missing_request = lifecycle_tags();
        missing_request.retain(|tag| tag.as_slice()[0] != "job-request");
        assert!(parse_job_lifecycle(&lifecycle(KIND_JOB_ACCEPTED, missing_request)).is_err());

        let mut changed_shape = lifecycle_tags();
        changed_shape[3] = Tag::parse(["h", "not-a-uuid"]).expect("tag");
        assert!(parse_job_lifecycle(&lifecycle(KIND_JOB_ACCEPTED, changed_shape)).is_err());
    }

    #[test]
    fn successor_is_required_only_for_delegation() {
        assert!(parse_job_lifecycle(&lifecycle(KIND_JOB_DELEGATED, lifecycle_tags())).is_err());

        let mut delegated = lifecycle_tags();
        delegated.push(Tag::parse(["job-successor", &"cd".repeat(32)]).expect("tag"));
        assert!(parse_job_lifecycle(&lifecycle(KIND_JOB_DELEGATED, delegated)).is_ok());

        let mut completed = lifecycle_tags();
        completed.push(Tag::parse(["job-successor", &"cd".repeat(32)]).expect("tag"));
        assert!(parse_job_lifecycle(&lifecycle(KIND_JOB_COMPLETED, completed)).is_err());
    }

    #[test]
    fn ordinary_messages_cannot_parse_as_jobs() {
        let event = EventBuilder::new(Kind::Custom(9), "please do this")
            .tags(base_tags(&"ab".repeat(32)))
            .sign_with_keys(&Keys::generate())
            .expect("sign");
        assert_eq!(parse_job_request(&event), Err(JobProtocolError::WrongKind));
        assert_eq!(
            parse_job_lifecycle(&event),
            Err(JobProtocolError::WrongKind)
        );
    }

    #[test]
    fn requests_reject_empty_or_oversized_assignments() {
        let target = "ab".repeat(32);
        let empty = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), "   ")
            .tags(base_tags(&target))
            .sign_with_keys(&Keys::generate())
            .expect("sign");
        assert!(parse_job_request(&empty).is_err());

        let oversized = EventBuilder::new(
            Kind::Custom(KIND_JOB_REQUEST as u16),
            "x".repeat(MAX_JOB_CONTENT_BYTES + 1),
        )
        .tags(base_tags(&target))
        .sign_with_keys(&Keys::generate())
        .expect("sign");
        assert!(parse_job_request(&oversized).is_err());
    }
}
