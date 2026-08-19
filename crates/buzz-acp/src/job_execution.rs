//! Reconciliation of durable execution attempts for accepted delegated jobs.

use std::collections::{HashMap, HashSet};

use buzz_core::delegated_job::{parse_job_lifecycle, parse_job_request, JobState};
use buzz_core::execution_attempt::{parse_execution_attempt, AttemptAction};
use buzz_core::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_BLOCKED, KIND_JOB_COMPLETED, KIND_JOB_DELEGATED,
    KIND_JOB_EXECUTION_ATTEMPT, KIND_JOB_REJECTED, KIND_JOB_REQUEST,
};
use nostr::{Event, EventBuilder, Filter, Kind, Tag};
use uuid::Uuid;

use crate::relay::{RelayError, RestClient};

/// Runtime metadata attached to a continuation queue item.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobExecutionContext {
    /// Stable job identity.
    pub job_id: Uuid,
    /// Immutable request event ID.
    pub request_event_id: String,
    /// Stable attempt identity.
    pub attempt_id: Uuid,
    /// Monotonic continuation generation.
    pub generation: i64,
    /// Runnable event that a runtime turn must claim.
    pub runnable_event_id: String,
    /// Claim event binding the runtime turn.
    pub claim_event_id: String,
    /// Stable runtime turn identity allocated before dispatch.
    pub turn_id: String,
    /// Claim deadline; a queued continuation must not dispatch after this.
    pub lease_until: i64,
}

const PROMPT_TAG_PREFIX: &str = "delegated-job-continuation:";

/// Encode attempt metadata into the harness-internal queue tag.
pub fn prompt_tag(execution: &JobExecutionContext) -> Result<String, serde_json::Error> {
    Ok(format!(
        "{PROMPT_TAG_PREFIX}{}",
        serde_json::to_string(execution)?
    ))
}

/// Decode attempt metadata from the harness-internal queue tag.
pub fn from_prompt_tag(value: &str) -> Option<JobExecutionContext> {
    serde_json::from_str(value.strip_prefix(PROMPT_TAG_PREFIX)?).ok()
}

/// One recovered continuation ready for normal ACP dispatch.
#[derive(Debug, Clone)]
pub struct ContinuationWork {
    /// Immutable original request event used as the assignment root.
    pub request: Event,
    /// Attempt metadata subordinate to that request.
    pub execution: JobExecutionContext,
}

#[derive(Debug, Clone)]
struct JobProjection {
    request: Event,
    state: JobState,
    head_event_id: String,
}

fn project_jobs(events: &[Event]) -> HashMap<Uuid, JobProjection> {
    let mut jobs = HashMap::<Uuid, JobProjection>::new();
    for event in events {
        if let Ok(request) = parse_job_request(event) {
            jobs.entry(request.job_id).or_insert(JobProjection {
                request: event.clone(),
                state: JobState::Requested,
                head_event_id: event.id.to_hex(),
            });
        }
    }

    // Relay query order and same-second event IDs are not lifecycle order.
    // Advance only through the immutable parent chain, retaining unmatched
    // events until their predecessor has been applied.
    let mut lifecycle: Vec<(Event, buzz_core::delegated_job::JobLifecycleEvent)> = events
        .iter()
        .filter_map(|event| {
            parse_job_lifecycle(event)
                .ok()
                .map(|parsed| (event.clone(), parsed))
        })
        .collect();
    loop {
        let mut advanced = false;
        lifecycle.retain(|(event, transition)| {
            let Some(job) = jobs.get_mut(&transition.job_id) else {
                return false;
            };
            let Ok(request) = parse_job_request(&job.request) else {
                return false;
            };
            if transition.request_event_id != request.request_event_id
                || transition.channel_id != request.channel_id
                || transition.author != request.target_agent
                || transition.parent_event_id != job.head_event_id
            {
                return true;
            }
            let Ok(next) = job.state.apply(transition.action) else {
                return true;
            };
            job.state = next;
            job.head_event_id = event.id.to_hex();
            advanced = true;
            false
        });
        if !advanced {
            break;
        }
    }
    jobs
}

/// Recover accepted work and ensure each open job has one runnable generation.
pub async fn reconcile(
    rest: &RestClient,
    already_queued: &HashSet<Uuid>,
    lease_seconds: u64,
) -> Result<Vec<ContinuationWork>, RelayError> {
    let pubkey = rest.keys.public_key().to_hex();
    let filters: Vec<Filter> = serde_json::from_value(serde_json::json!([
        {"kinds":[KIND_JOB_REQUEST], "#p":[pubkey]},
        {"kinds":[KIND_JOB_ACCEPTED,KIND_JOB_REJECTED,KIND_JOB_COMPLETED,KIND_JOB_BLOCKED,KIND_JOB_DELEGATED], "authors":[pubkey]},
        {"kinds":[KIND_JOB_EXECUTION_ATTEMPT], "authors":[pubkey]}
    ]))
    .map_err(RelayError::Json)?;
    let mut events = Vec::new();
    for filter in &filters {
        events.extend(rest.query_all(filter).await?);
    }
    events.sort_by_key(|event| (event.created_at, event.id));

    let jobs = project_jobs(&events);

    let mut attempts: HashMap<
        Uuid,
        Vec<(Event, buzz_core::execution_attempt::ExecutionAttemptEvent)>,
    > = HashMap::new();
    for event in events {
        if let Ok(attempt) = parse_execution_attempt(&event) {
            attempts
                .entry(attempt.job_id)
                .or_default()
                .push((event, attempt));
        }
    }

    let mut work = Vec::new();
    for (job_id, job) in jobs {
        if job.state != JobState::Accepted {
            continue;
        }
        let mut history = attempts.remove(&job_id).unwrap_or_default();
        history.sort_by_key(|(event, attempt)| (attempt.generation, event.created_at, event.id));
        let next = reconcile_decision(&history, chrono::Utc::now().timestamp());
        let (runnable_event, runnable) = match next {
            ReconcileDecision::Use(index) => history[index].clone(),
            ReconcileDecision::Create(generation) => {
                let request = parse_job_request(&job.request)
                    .map_err(|error| RelayError::Http(error.to_string()))?;
                let attempt_id = Uuid::new_v4();
                let event = build_runnable(rest, &request, attempt_id, generation)?;
                require_accepted(rest.submit_event(&event).await?)?;
                let parsed = parse_execution_attempt(&event)
                    .map_err(|error| RelayError::Http(error.to_string()))?;
                (event, parsed)
            }
            ReconcileDecision::None => continue,
        };
        if already_queued.contains(&runnable.attempt_id) {
            continue;
        }
        let turn_id = Uuid::new_v4().to_string();
        let lease_until =
            chrono::Utc::now().timestamp() + i64::try_from(lease_seconds).unwrap_or(i64::MAX / 2);
        let execution = JobExecutionContext {
            job_id,
            request_event_id: runnable.request_event_id,
            attempt_id: runnable.attempt_id,
            generation: runnable.generation,
            runnable_event_id: runnable_event.id.to_hex(),
            claim_event_id: String::new(),
            turn_id: turn_id.clone(),
            lease_until,
        };
        let claim_event = claim(
            rest,
            &execution,
            runnable.channel_id,
            &turn_id,
            None,
            lease_until,
        )
        .await?;
        work.push(ContinuationWork {
            request: job.request,
            execution: JobExecutionContext {
                claim_event_id: claim_event.id.to_hex(),
                ..execution
            },
        });
    }
    Ok(work)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileDecision {
    None,
    Use(usize),
    Create(i64),
}

fn reconcile_decision(
    history: &[(Event, buzz_core::execution_attempt::ExecutionAttemptEvent)],
    now: i64,
) -> ReconcileDecision {
    let Some(generation) = history.iter().map(|(_, attempt)| attempt.generation).max() else {
        return ReconcileDecision::Create(1);
    };
    let Some((runnable_index, (runnable_event, _))) =
        history.iter().enumerate().find(|(_, (_, attempt))| {
            attempt.generation == generation && attempt.action == AttemptAction::Runnable
        })
    else {
        // A relay page containing only a partial generation is not enough to
        // mint a successor. Fail closed until the structural root is visible.
        return ReconcileDecision::None;
    };
    let runnable_id = runnable_event.id.to_hex();
    let claim = history.iter().find(|(_, attempt)| {
        attempt.generation == generation
            && attempt.action == AttemptAction::Claim
            && attempt.parent_event_id.as_deref() == Some(runnable_id.as_str())
    });
    let Some((claim_event, claim)) = claim else {
        return ReconcileDecision::Use(runnable_index);
    };
    let claim_id = claim_event.id.to_hex();
    let finished = history.iter().any(|(_, attempt)| {
        attempt.generation == generation
            && attempt.action == AttemptAction::Finish
            && attempt.parent_event_id.as_deref() == Some(claim_id.as_str())
            && attempt.turn_id == claim.turn_id
    });
    if finished || claim.lease_until.is_some_and(|lease| lease <= now) {
        ReconcileDecision::Create(generation + 1)
    } else {
        ReconcileDecision::None
    }
}

fn build_runnable(
    rest: &RestClient,
    request: &buzz_core::delegated_job::JobRequest,
    attempt_id: Uuid,
    generation: i64,
) -> Result<Event, RelayError> {
    EventBuilder::new(Kind::Custom(KIND_JOB_EXECUTION_ATTEMPT as u16), "")
        .tags([
            Tag::parse(["d", &request.job_id.to_string()])?,
            Tag::parse(["job-request", &request.request_event_id])?,
            Tag::parse(["attempt", &attempt_id.to_string()])?,
            Tag::parse(["generation", &generation.to_string()])?,
            Tag::parse(["job-target", &request.target_agent])?,
            Tag::parse(["h", &request.channel_id.to_string()])?,
            Tag::parse(["attempt-action", "runnable"])?,
        ])
        .sign_with_keys(&rest.keys)
        .map_err(Into::into)
}

/// Build and submit the claim that binds a runtime turn to a runnable attempt.
pub async fn claim(
    rest: &RestClient,
    execution: &JobExecutionContext,
    channel_id: Uuid,
    turn_id: &str,
    session_id: Option<&str>,
    lease_until: i64,
) -> Result<Event, RelayError> {
    let mut tags = vec![
        Tag::parse(["d", &execution.job_id.to_string()])?,
        Tag::parse(["job-request", &execution.request_event_id])?,
        Tag::parse(["attempt", &execution.attempt_id.to_string()])?,
        Tag::parse(["generation", &execution.generation.to_string()])?,
        Tag::parse(["job-target", &rest.keys.public_key().to_hex()])?,
        Tag::parse(["h", &channel_id.to_string()])?,
        Tag::parse(["attempt-action", "claim"])?,
        Tag::parse(["attempt-parent", &execution.runnable_event_id])?,
        Tag::parse(["turn-id", turn_id])?,
        Tag::parse(["lease-until", &lease_until.to_string()])?,
    ];
    if let Some(session_id) = session_id {
        tags.push(Tag::parse(["session-id", session_id])?);
    }
    let event = EventBuilder::new(Kind::Custom(KIND_JOB_EXECUTION_ATTEMPT as u16), "")
        .tags(tags)
        .sign_with_keys(&rest.keys)?;
    require_accepted(rest.submit_event(&event).await?)?;
    Ok(event)
}

/// Record a bounded runtime outcome. This never dispositions the job.
pub async fn finish(
    rest: &RestClient,
    execution: &JobExecutionContext,
    channel_id: Uuid,
    turn_id: &str,
    claim_event_id: &str,
    outcome: &str,
    detail: &str,
) -> Result<(), RelayError> {
    let event = EventBuilder::new(Kind::Custom(KIND_JOB_EXECUTION_ATTEMPT as u16), detail)
        .tags([
            Tag::parse(["d", &execution.job_id.to_string()])?,
            Tag::parse(["job-request", &execution.request_event_id])?,
            Tag::parse(["attempt", &execution.attempt_id.to_string()])?,
            Tag::parse(["generation", &execution.generation.to_string()])?,
            Tag::parse(["job-target", &rest.keys.public_key().to_hex()])?,
            Tag::parse(["h", &channel_id.to_string()])?,
            Tag::parse(["attempt-action", "finish"])?,
            Tag::parse(["attempt-parent", claim_event_id])?,
            Tag::parse(["turn-id", turn_id])?,
            Tag::parse(["attempt-outcome", outcome])?,
        ])
        .sign_with_keys(&rest.keys)?;
    require_accepted(rest.submit_event(&event).await?)?;
    Ok(())
}

fn require_accepted(response: serde_json::Value) -> Result<(), RelayError> {
    if response
        .get("accepted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(RelayError::Http(format!(
            "relay rejected execution-attempt event: {response}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{Keys, Timestamp};

    fn attempt_event(
        action: &str,
        generation: i64,
        lease: Option<i64>,
    ) -> (Event, buzz_core::execution_attempt::ExecutionAttemptEvent) {
        let keys = Keys::generate();
        let mut tags = vec![
            Tag::parse(["d", &Uuid::nil().to_string()]).expect("tag"),
            Tag::parse(["job-request", &"ab".repeat(32)]).expect("tag"),
            Tag::parse(["attempt", &Uuid::new_v4().to_string()]).expect("tag"),
            Tag::parse(["generation", &generation.to_string()]).expect("tag"),
            Tag::parse(["job-target", &keys.public_key().to_hex()]).expect("tag"),
            Tag::parse(["h", &Uuid::nil().to_string()]).expect("tag"),
            Tag::parse(["attempt-action", action]).expect("tag"),
        ];
        if action != "runnable" {
            tags.push(Tag::parse(["attempt-parent", &"cd".repeat(32)]).expect("tag"));
            tags.push(Tag::parse(["turn-id", "turn"]).expect("tag"));
        }
        if let Some(lease) = lease {
            tags.push(Tag::parse(["lease-until", &lease.to_string()]).expect("tag"));
        }
        if action == "finish" {
            tags.push(Tag::parse(["attempt-outcome", "end_turn"]).expect("tag"));
        }
        let event = EventBuilder::new(Kind::Custom(KIND_JOB_EXECUTION_ATTEMPT as u16), "")
            .tags(tags)
            .sign_with_keys(&keys)
            .expect("event");
        let parsed = parse_execution_attempt(&event).expect("parse");
        (event, parsed)
    }

    #[test]
    fn reconciliation_is_idempotent_and_advances_only_after_end_or_expiry() {
        assert_eq!(reconcile_decision(&[], 10), ReconcileDecision::Create(1));
        let runnable = attempt_event("runnable", 1, None);
        assert_eq!(
            reconcile_decision(std::slice::from_ref(&runnable), 10),
            ReconcileDecision::Use(0)
        );
        let mut active = attempt_event("claim", 1, Some(20));
        active.1.parent_event_id = Some(runnable.0.id.to_hex());
        assert_eq!(
            reconcile_decision(&[active.clone(), runnable.clone()], 10),
            ReconcileDecision::None
        );
        assert_eq!(
            reconcile_decision(&[runnable.clone(), active.clone()], 21),
            ReconcileDecision::Create(2)
        );
        let mut ended = attempt_event("finish", 1, None);
        ended.1.parent_event_id = Some(active.0.id.to_hex());
        ended.1.turn_id = active.1.turn_id.clone();
        assert_eq!(
            reconcile_decision(&[ended, runnable, active], 10),
            ReconcileDecision::Create(2)
        );
    }

    #[test]
    fn every_non_disposition_runtime_end_requires_one_next_generation() {
        for outcome in [
            "end_turn",
            "stop",
            "cancelled",
            "cancel_and_merge",
            "session_replaced",
            "worker_loss",
            "worker_teardown",
            "max_turn_requests",
            "idle_timeout",
            "hard_timeout",
            "provider_error",
        ] {
            let runnable = attempt_event("runnable", 7, None);
            let mut active = attempt_event("claim", 7, Some(20));
            active.1.parent_event_id = Some(runnable.0.id.to_hex());
            let mut ended = attempt_event("finish", 7, None);
            ended.1.parent_event_id = Some(active.0.id.to_hex());
            ended.1.turn_id = active.1.turn_id.clone();
            ended.1.outcome = Some(outcome.into());
            assert_eq!(
                reconcile_decision(&[ended, active, runnable], 10),
                ReconcileDecision::Create(8),
                "runtime outcome {outcome} must not close the job"
            );
        }
    }

    #[test]
    fn requested_rejected_and_terminal_jobs_are_filtered_before_attempt_reconciliation() {
        for state in [
            JobState::Requested,
            JobState::Rejected,
            JobState::Completed,
            JobState::Blocked,
            JobState::Delegated,
        ] {
            assert_ne!(state, JobState::Accepted);
        }
    }

    #[test]
    fn lifecycle_projection_follows_parent_chain_not_query_order() {
        let requester = Keys::generate();
        let target = Keys::generate();
        let job_id = Uuid::new_v4();
        let channel = Uuid::new_v4();
        let created_at = Timestamp::from(42);
        let request = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), "assignment")
            .custom_created_at(created_at)
            .tags([
                Tag::parse(["d", &job_id.to_string()]).expect("tag"),
                Tag::parse(["job-target", &target.public_key().to_hex()]).expect("tag"),
                Tag::parse(["p", &target.public_key().to_hex()]).expect("tag"),
                Tag::parse(["h", &channel.to_string()]).expect("tag"),
            ])
            .sign_with_keys(&requester)
            .expect("request");
        let acceptance = EventBuilder::new(Kind::Custom(KIND_JOB_ACCEPTED as u16), "")
            .custom_created_at(created_at)
            .tags([
                Tag::parse(["d", &job_id.to_string()]).expect("tag"),
                Tag::parse(["job-request", &request.id.to_hex()]).expect("tag"),
                Tag::parse(["job-parent", &request.id.to_hex()]).expect("tag"),
                Tag::parse(["h", &channel.to_string()]).expect("tag"),
            ])
            .sign_with_keys(&target)
            .expect("acceptance");
        let completion = EventBuilder::new(Kind::Custom(KIND_JOB_COMPLETED as u16), "done")
            .custom_created_at(created_at)
            .tags([
                Tag::parse(["d", &job_id.to_string()]).expect("tag"),
                Tag::parse(["job-request", &request.id.to_hex()]).expect("tag"),
                Tag::parse(["job-parent", &acceptance.id.to_hex()]).expect("tag"),
                Tag::parse(["h", &channel.to_string()]).expect("tag"),
            ])
            .sign_with_keys(&target)
            .expect("completion");

        let projected = project_jobs(&[completion, acceptance, request]);
        assert_eq!(
            projected.get(&job_id).map(|job| job.state),
            Some(JobState::Completed)
        );
    }
}
