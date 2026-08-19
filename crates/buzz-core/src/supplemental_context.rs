//! Durable admission envelope for user context received while a delegated job
//! owns a channel.

use nostr::Event;
use uuid::Uuid;

use crate::kind::{event_kind_u32, KIND_JOB_SUPPLEMENTAL_CONTEXT};

/// Maximum serialized admitted message body.
pub const MAX_SUPPLEMENTAL_CONTEXT_BYTES: usize = 64 * 1024;

/// Relay-validated admission of one exact source message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupplementalContextEvent {
    /// Accepted delegated-job identity.
    pub job_id: Uuid,
    /// Immutable request event ID.
    pub request_event_id: String,
    /// Managed agent that owns the accepted job and signed this admission.
    pub target_agent: String,
    /// Originating job channel.
    pub channel_id: Uuid,
    /// Exact already-persisted user message admitted by ACP policy.
    pub source_event_id: String,
    /// Author of the exact source event.
    pub source_author: String,
    /// Continuation generation that must receive this context.
    pub continuation_generation: i64,
    /// Immutable source message content copied for deterministic recovery.
    pub content: String,
}

/// Invalid supplemental-context control envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SupplementalContextProtocolError {
    /// Event has a different kind.
    #[error("not a job supplemental-context event")]
    WrongKind,
    /// Required tags or content are malformed.
    #[error("invalid supplemental-context envelope: {0}")]
    InvalidEnvelope(String),
}

#[derive(serde::Deserialize)]
struct ContentEnvelope {
    content: String,
}

/// Parse and structurally validate one kind-43008 admission event.
pub fn parse_supplemental_context(
    event: &Event,
) -> Result<SupplementalContextEvent, SupplementalContextProtocolError> {
    if event_kind_u32(event) != KIND_JOB_SUPPLEMENTAL_CONTEXT {
        return Err(SupplementalContextProtocolError::WrongKind);
    }
    if event.content.len() > MAX_SUPPLEMENTAL_CONTEXT_BYTES {
        return Err(invalid("content too large"));
    }
    let job_id = one(event, "d")?
        .parse()
        .map_err(|_| invalid("d must be a job UUID"))?;
    let request_event_id = event_id(one(event, "job-request")?, "job-request")?;
    let target_agent = hex_id(one(event, "job-target")?, "job-target")?;
    if target_agent != event.pubkey.to_hex() {
        return Err(invalid("job-target must match the event signer"));
    }
    let channel_id = one(event, "h")?
        .parse()
        .map_err(|_| invalid("h must be a channel UUID"))?;
    let source_event_id = event_id(one(event, "supplemental-event")?, "supplemental-event")?;
    let source_author = hex_id(one(event, "supplemental-author")?, "supplemental-author")?;
    let continuation_generation = one(event, "continuation-generation")?
        .parse::<i64>()
        .map_err(|_| invalid("continuation-generation must be a positive integer"))?;
    if continuation_generation < 1 {
        return Err(invalid(
            "continuation-generation must be a positive integer",
        ));
    }
    let content: ContentEnvelope =
        serde_json::from_str(&event.content).map_err(|_| invalid("content must be valid JSON"))?;
    if content.content.len() > MAX_SUPPLEMENTAL_CONTEXT_BYTES {
        return Err(invalid("admitted source content too large"));
    }
    Ok(SupplementalContextEvent {
        job_id,
        request_event_id,
        target_agent,
        channel_id,
        source_event_id,
        source_author,
        continuation_generation,
        content: content.content,
    })
}

fn one(event: &Event, name: &str) -> Result<String, SupplementalContextProtocolError> {
    let values: Vec<_> = event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().map(String::as_str) == Some(name))
        .collect();
    if values.len() != 1 || values[0].as_slice().len() != 2 {
        return Err(invalid(&format!(
            "expected exactly one two-element {name} tag"
        )));
    }
    Ok(values[0].as_slice()[1].clone())
}

fn event_id(value: String, name: &str) -> Result<String, SupplementalContextProtocolError> {
    hex_id(value, name)
}

fn hex_id(value: String, name: &str) -> Result<String, SupplementalContextProtocolError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(&format!("{name} must be 64-character hex")));
    }
    Ok(value.to_ascii_lowercase())
}

fn invalid(message: &str) -> SupplementalContextProtocolError {
    SupplementalContextProtocolError::InvalidEnvelope(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    fn event(keys: &Keys) -> Event {
        EventBuilder::new(
            Kind::Custom(KIND_JOB_SUPPLEMENTAL_CONTEXT as u16),
            serde_json::json!({"content":"status?"}).to_string(),
        )
        .tags([
            Tag::parse(["d", &Uuid::new_v4().to_string()]).unwrap(),
            Tag::parse(["job-request", &"ab".repeat(32)]).unwrap(),
            Tag::parse(["job-target", &keys.public_key().to_hex()]).unwrap(),
            Tag::parse(["h", &Uuid::new_v4().to_string()]).unwrap(),
            Tag::parse(["supplemental-event", &"cd".repeat(32)]).unwrap(),
            Tag::parse(["supplemental-author", &"ef".repeat(32)]).unwrap(),
            Tag::parse(["continuation-generation", "2"]).unwrap(),
        ])
        .sign_with_keys(keys)
        .unwrap()
    }

    #[test]
    fn validates_exact_admission_envelope() {
        let keys = Keys::generate();
        let parsed = parse_supplemental_context(&event(&keys)).unwrap();
        assert_eq!(parsed.target_agent, keys.public_key().to_hex());
        assert_eq!(parsed.continuation_generation, 2);
        assert_eq!(parsed.content, "status?");
    }

    #[test]
    fn rejects_signer_mismatch_and_duplicate_tags() {
        let keys = Keys::generate();
        let mut wrong = event(&keys);
        let other = Keys::generate();
        wrong.tags = wrong
            .tags
            .into_iter()
            .map(|tag| {
                if tag.as_slice().first().map(String::as_str) == Some("job-target") {
                    Tag::parse(["job-target", &other.public_key().to_hex()]).unwrap()
                } else {
                    tag
                }
            })
            .collect();
        assert!(parse_supplemental_context(&wrong).is_err());

        let mut duplicate = event(&keys);
        duplicate
            .tags
            .push(Tag::parse(["supplemental-event", &"01".repeat(32)]).unwrap());
        assert!(parse_supplemental_context(&duplicate).is_err());
    }
}
