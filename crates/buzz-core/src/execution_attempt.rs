//! Execution-attempt metadata for accepted delegated jobs.
//!
//! The delegated-job lifecycle is authoritative. These events only describe
//! bounded runtime attempts and never close or otherwise disposition a job.

use nostr::Event;
use uuid::Uuid;

use crate::kind::{event_kind_u32, KIND_JOB_EXECUTION_ATTEMPT};

/// Maximum runtime outcome text persisted on an attempt event.
pub const MAX_ATTEMPT_OUTCOME_BYTES: usize = 4096;

/// Attempt-control operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptAction {
    /// A continuation generation is durably runnable.
    Runnable,
    /// A runtime turn claimed the runnable generation.
    Claim,
    /// The bounded runtime effort ended without disposing the job.
    Finish,
}

impl AttemptAction {
    /// Stable wire value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runnable => "runnable",
            Self::Claim => "claim",
            Self::Finish => "finish",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "runnable" => Some(Self::Runnable),
            "claim" => Some(Self::Claim),
            "finish" => Some(Self::Finish),
            _ => None,
        }
    }
}

/// Validated execution-attempt control envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAttemptEvent {
    /// Immutable delegated-job identity.
    pub job_id: Uuid,
    /// Immutable request event coordinate.
    pub request_event_id: String,
    /// Stable attempt identity for this generation.
    pub attempt_id: Uuid,
    /// Monotonic continuation generation, starting at one.
    pub generation: i64,
    /// Managed agent that owns the accepted job.
    pub target_agent: String,
    /// Originating job channel.
    pub channel_id: Uuid,
    /// Attempt operation.
    pub action: AttemptAction,
    /// Immediate predecessor control event for claim/finish.
    pub parent_event_id: Option<String>,
    /// Harness runtime turn identity for claim/finish.
    pub turn_id: Option<String>,
    /// ACP session identity when available.
    pub session_id: Option<String>,
    /// Claim lease expiry as a Unix timestamp.
    pub lease_until: Option<i64>,
    /// Runtime outcome label for finish.
    pub outcome: Option<String>,
    /// Optional bounded diagnostic detail; never a job disposition.
    pub detail: String,
}

/// Invalid attempt envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttemptProtocolError {
    /// Event kind is not the attempt-control kind.
    #[error("not a job execution-attempt event")]
    WrongKind,
    /// Required tags are missing, duplicated, malformed, or contradictory.
    #[error("invalid execution-attempt envelope: {0}")]
    InvalidEnvelope(String),
}

/// Parse one execution-attempt control event.
pub fn parse_execution_attempt(
    event: &Event,
) -> Result<ExecutionAttemptEvent, AttemptProtocolError> {
    if event_kind_u32(event) != KIND_JOB_EXECUTION_ATTEMPT {
        return Err(AttemptProtocolError::WrongKind);
    }
    if event.content.len() > MAX_ATTEMPT_OUTCOME_BYTES {
        return Err(AttemptProtocolError::InvalidEnvelope(
            "content too large".into(),
        ));
    }
    let job_id = uuid_tag(event, "d", "job")?;
    let request_event_id = event_id_tag(event, "job-request")?;
    let attempt_id = uuid_tag(event, "attempt", "attempt")?;
    let generation = one(event, "generation")?
        .parse::<i64>()
        .map_err(|_| invalid("generation must be a positive integer"))?;
    if generation < 1 {
        return Err(invalid("generation must be a positive integer"));
    }
    let target_agent = hex_tag(event, "job-target", "target pubkey")?;
    if target_agent != event.pubkey.to_hex() {
        return Err(invalid("job-target must match the event signer"));
    }
    let channel_id = uuid_tag(event, "h", "channel")?;
    let action = AttemptAction::parse(&one(event, "attempt-action")?)
        .ok_or_else(|| invalid("unknown attempt-action"))?;

    let parent_event_id = optional(event, "attempt-parent")?
        .map(|value| validate_event_id(value, "attempt-parent"))
        .transpose()?;
    let turn_id = optional(event, "turn-id")?;
    let session_id = optional(event, "session-id")?;
    let lease_until = optional(event, "lease-until")?
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| invalid("lease-until must be a Unix timestamp"))
        })
        .transpose()?;
    let outcome = optional(event, "attempt-outcome")?;

    match action {
        AttemptAction::Runnable => {
            require_absent(
                &parent_event_id,
                &turn_id,
                &session_id,
                &lease_until,
                &outcome,
            )?;
            if !event.content.is_empty() {
                return Err(invalid("runnable content must be empty"));
            }
        }
        AttemptAction::Claim => {
            if parent_event_id.is_none() || turn_id.as_deref().is_none_or(str::is_empty) {
                return Err(invalid("claim requires attempt-parent and turn-id"));
            }
            if lease_until.is_none() || outcome.is_some() || !event.content.is_empty() {
                return Err(invalid(
                    "claim requires lease-until, forbids outcome, and has empty content",
                ));
            }
        }
        AttemptAction::Finish => {
            if parent_event_id.is_none()
                || turn_id.as_deref().is_none_or(str::is_empty)
                || outcome.as_deref().is_none_or(str::is_empty)
                || lease_until.is_some()
            {
                return Err(invalid(
                    "finish requires attempt-parent, turn-id, and attempt-outcome and forbids lease-until",
                ));
            }
        }
    }

    Ok(ExecutionAttemptEvent {
        job_id,
        request_event_id,
        attempt_id,
        generation,
        target_agent,
        channel_id,
        action,
        parent_event_id,
        turn_id,
        session_id,
        lease_until,
        outcome,
        detail: event.content.clone(),
    })
}

fn require_absent(
    parent: &Option<String>,
    turn: &Option<String>,
    session: &Option<String>,
    lease: &Option<i64>,
    outcome: &Option<String>,
) -> Result<(), AttemptProtocolError> {
    if parent.is_some()
        || turn.is_some()
        || session.is_some()
        || lease.is_some()
        || outcome.is_some()
    {
        return Err(invalid("runnable forbids claim/outcome metadata"));
    }
    Ok(())
}

fn values(event: &Event, name: &str) -> Result<Vec<String>, AttemptProtocolError> {
    let mut found = Vec::new();
    for tag in event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().map(String::as_str) == Some(name))
    {
        let parts = tag.as_slice();
        if parts.len() != 2 {
            return Err(invalid(&format!(
                "{name} tag must have exactly two elements"
            )));
        }
        found.push(parts[1].clone());
    }
    Ok(found)
}

fn one(event: &Event, name: &str) -> Result<String, AttemptProtocolError> {
    let found = values(event, name)?;
    if found.len() != 1 {
        return Err(invalid(&format!("expected exactly one {name} tag")));
    }
    Ok(found[0].clone())
}

fn optional(event: &Event, name: &str) -> Result<Option<String>, AttemptProtocolError> {
    let found = values(event, name)?;
    if found.len() > 1 {
        return Err(invalid(&format!("expected at most one {name} tag")));
    }
    Ok(found.into_iter().next())
}

fn uuid_tag(event: &Event, name: &str, label: &str) -> Result<Uuid, AttemptProtocolError> {
    one(event, name)?
        .parse()
        .map_err(|_| invalid(&format!("{name} must be a {label} UUID")))
}

fn hex_tag(event: &Event, name: &str, label: &str) -> Result<String, AttemptProtocolError> {
    let value = one(event, name)?;
    if value.len() != 64 || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(invalid(&format!(
            "{name} must be a 64-character hex {label}"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

fn event_id_tag(event: &Event, name: &str) -> Result<String, AttemptProtocolError> {
    validate_event_id(one(event, name)?, name)
}

fn validate_event_id(value: String, name: &str) -> Result<String, AttemptProtocolError> {
    if value.len() != 64 || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(invalid(&format!(
            "{name} must be a 64-character hex event ID"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

fn invalid(message: &str) -> AttemptProtocolError {
    AttemptProtocolError::InvalidEnvelope(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    fn event(action: &str, extra: &[Tag], content: &str) -> Event {
        let keys = Keys::generate();
        let mut tags = vec![
            Tag::parse(["d", &Uuid::new_v4().to_string()]).expect("tag"),
            Tag::parse(["job-request", &"ab".repeat(32)]).expect("tag"),
            Tag::parse(["attempt", &Uuid::new_v4().to_string()]).expect("tag"),
            Tag::parse(["generation", "1"]).expect("tag"),
            Tag::parse(["job-target", &keys.public_key().to_hex()]).expect("tag"),
            Tag::parse(["h", &Uuid::new_v4().to_string()]).expect("tag"),
            Tag::parse(["attempt-action", action]).expect("tag"),
        ];
        tags.extend_from_slice(extra);
        EventBuilder::new(Kind::Custom(KIND_JOB_EXECUTION_ATTEMPT as u16), content)
            .tags(tags)
            .sign_with_keys(&keys)
            .expect("sign")
    }

    #[test]
    fn runnable_is_minimal_and_generation_is_positive() {
        let envelope = event("runnable", &[], "");
        let parsed = parse_execution_attempt(&envelope);
        assert!(parsed.is_ok(), "{parsed:?}");
        assert!(parse_execution_attempt(&event(
            "runnable",
            &[Tag::parse(["turn-id", "turn"]).expect("tag")],
            ""
        ))
        .is_err());
    }

    #[test]
    fn claim_and_finish_require_structural_runtime_coordinates() {
        let parent = Tag::parse(["attempt-parent", &"cd".repeat(32)]).expect("tag");
        let turn = Tag::parse(["turn-id", "turn-1"]).expect("tag");
        let lease = Tag::parse(["lease-until", "4102444800"]).expect("tag");
        let parsed =
            parse_execution_attempt(&event("claim", &[parent.clone(), turn.clone(), lease], ""));
        assert!(parsed.is_ok(), "{parsed:?}");
        let outcome = Tag::parse(["attempt-outcome", "end_turn"]).expect("tag");
        assert!(parse_execution_attempt(&event("finish", &[parent, turn, outcome], "")).is_ok());
    }
}
