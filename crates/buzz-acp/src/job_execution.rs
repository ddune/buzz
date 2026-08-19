//! Reconciliation of durable execution attempts for accepted delegated jobs.

use std::collections::{HashMap, HashSet};

use buzz_core::delegated_job::{parse_job_lifecycle, parse_job_request, JobState};
use buzz_core::execution_attempt::{parse_execution_attempt, AttemptAction};
use buzz_core::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_BLOCKED, KIND_JOB_COMPLETED, KIND_JOB_DELEGATED,
    KIND_JOB_EXECUTION_ATTEMPT, KIND_JOB_REJECTED, KIND_JOB_REQUEST, KIND_JOB_SUPPLEMENTAL_CONTEXT,
};
use buzz_core::supplemental_context::{
    parse_supplemental_context, SupplementalContextEvent as SupplementalAdmission,
};
use nostr::{Event, EventBuilder, Filter, Kind, Tag};
use uuid::Uuid;

use crate::relay::{RelayError, RestClient};

/// Runtime metadata attached to a continuation queue item.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
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

/// Durable user context that arrived while a tracked generation was active.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct JobSupplementalMessage {
    pub event_id: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_content: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JobContinuationPromptEnvelope {
    execution: JobExecutionContext,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    supplemental_messages: Vec<JobSupplementalMessage>,
}

/// Runtime classification for the restricted conversational response that
/// may run between an interrupted generation and its durable continuation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct JobFollowupContext {
    pub job_id: Uuid,
    pub request_event_id: String,
    pub interrupted_attempt_id: Option<Uuid>,
    pub interrupted_generation: Option<i64>,
    pub interrupted_turn_id: Option<String>,
}

/// Durable accepted-job ownership projected onto one conversation channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedJobGuard {
    pub job_id: Uuid,
    pub request_event_id: String,
    pub channel_id: Uuid,
    pub last_execution: Option<JobExecutionContext>,
}

/// Identity of a proposed delegated job whose accept/reject decision is being
/// made by an ordinary ACP turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobEvaluationContext {
    pub job_id: Uuid,
    pub request_event_id: String,
    pub channel_id: Uuid,
}

pub fn evaluation_context(event: &Event) -> Option<JobEvaluationContext> {
    let request = parse_job_request(event).ok()?;
    Some(JobEvaluationContext {
        job_id: request.job_id,
        request_event_id: request.request_event_id,
        channel_id: request.channel_id,
    })
}

const PROMPT_TAG_PREFIX: &str = "delegated-job-continuation:";
const FOLLOWUP_PROMPT_TAG_PREFIX: &str = "delegated-job-followup-readonly:";

/// Encode attempt metadata into the harness-internal queue tag.
#[cfg(test)]
pub fn prompt_tag(execution: &JobExecutionContext) -> Result<String, serde_json::Error> {
    Ok(format!(
        "{PROMPT_TAG_PREFIX}{}",
        serde_json::to_string(execution)?
    ))
}

/// Decode attempt metadata from the harness-internal queue tag.
pub fn from_prompt_tag(value: &str) -> Option<JobExecutionContext> {
    let payload = value.strip_prefix(PROMPT_TAG_PREFIX)?;
    serde_json::from_str(payload).ok().or_else(|| {
        serde_json::from_str::<JobContinuationPromptEnvelope>(payload)
            .ok()
            .map(|envelope| envelope.execution)
    })
}

/// Bind already-durable supplemental messages to the claimed continuation's
/// harness prompt metadata.
pub fn prompt_tag_with_supplemental_messages(
    execution: &JobExecutionContext,
    supplemental_messages: Vec<JobSupplementalMessage>,
) -> Result<String, serde_json::Error> {
    Ok(format!(
        "{PROMPT_TAG_PREFIX}{}",
        serde_json::to_string(&JobContinuationPromptEnvelope {
            execution: execution.clone(),
            supplemental_messages,
        })?
    ))
}

/// Recover supplemental context bound to a claimed continuation.
pub fn supplemental_messages_from_prompt_tag(value: &str) -> Vec<JobSupplementalMessage> {
    value
        .strip_prefix(PROMPT_TAG_PREFIX)
        .and_then(|payload| serde_json::from_str::<JobContinuationPromptEnvelope>(payload).ok())
        .map(|envelope| envelope.supplemental_messages)
        .unwrap_or_default()
}

/// Encode a supplemental conversational turn that must not inherit job
/// execution authority.
pub fn followup_prompt_tag(context: &JobFollowupContext) -> Result<String, serde_json::Error> {
    Ok(format!(
        "{FOLLOWUP_PROMPT_TAG_PREFIX}{}",
        serde_json::to_string(context)?
    ))
}

/// Decode a restricted accepted-job follow-up queue tag.
pub fn followup_from_prompt_tag(value: &str) -> Option<JobFollowupContext> {
    serde_json::from_str(value.strip_prefix(FOLLOWUP_PROMPT_TAG_PREFIX)?).ok()
}

/// One recovered continuation ready for normal ACP dispatch.
#[derive(Debug, Clone)]
pub struct ContinuationWork {
    /// Immutable original request event used as the assignment root.
    pub request: Event,
    /// Attempt metadata subordinate to that request.
    pub execution: JobExecutionContext,
    /// Relay-recovered context for the interrupted generation.
    pub supplemental_messages: Vec<JobSupplementalMessage>,
}

/// One authoritative reconciliation snapshot. Guards are returned even when
/// no generation can yet be dispatched.
#[derive(Debug, Clone)]
pub struct ReconcileSnapshot {
    pub work: Vec<ContinuationWork>,
    pub accepted_jobs: Vec<AcceptedJobGuard>,
}

/// Observable result of an exact-job reconciliation pass.
#[derive(Debug)]
pub struct JobReconcileResult {
    pub work: Vec<ContinuationWork>,
    pub state: Option<JobState>,
    pub queried_events: usize,
    pub attempts: usize,
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
    deferred_channels: &HashSet<Uuid>,
    lease_seconds: u64,
) -> Result<ReconcileSnapshot, RelayError> {
    let events = query_events(rest, None).await?;
    reconcile_events(
        rest,
        already_queued,
        deferred_channels,
        lease_seconds,
        events,
        None,
    )
    .await
}

/// Reconcile one accepted job through the same canonical state machine used by
/// broad startup/periodic recovery. The broad ownership projection is
/// intentional: a supposedly exact job may not claim a channel when another
/// accepted job already owns that same channel.
pub async fn reconcile_job(
    rest: &RestClient,
    already_queued: &HashSet<Uuid>,
    lease_seconds: u64,
    evaluation: &JobEvaluationContext,
) -> Result<JobReconcileResult, RelayError> {
    let events = query_events(rest, None).await?;
    let queried_events = events.len();
    let attempts = events
        .iter()
        .filter(|event| parse_execution_attempt(event).is_ok())
        .count();
    let projection = project_jobs(&events).remove(&evaluation.job_id);
    let state = projection.as_ref().and_then(|job| {
        let request = parse_job_request(&job.request).ok()?;
        (request.request_event_id == evaluation.request_event_id
            && request.channel_id == evaluation.channel_id)
            .then_some(job.state)
    });
    tracing::debug!(
        job_id = %evaluation.job_id,
        request_event_id = %evaluation.request_event_id,
        channel_id = %evaluation.channel_id,
        queried_events,
        attempts,
        projected_state = ?state,
        "exact delegated-job reconciliation snapshot"
    );
    let work = if state.is_some() {
        reconcile_events(
            rest,
            already_queued,
            &HashSet::new(),
            lease_seconds,
            events,
            Some(evaluation.job_id),
        )
        .await?
        .work
    } else {
        Vec::new()
    };
    Ok(JobReconcileResult {
        work,
        state,
        queried_events,
        attempts,
    })
}

async fn query_events(rest: &RestClient, job_id: Option<Uuid>) -> Result<Vec<Event>, RelayError> {
    let pubkey = rest.keys.public_key().to_hex();
    let filters = reconcile_filters(&pubkey, job_id)?;
    let mut events = Vec::new();
    for filter in &filters {
        events.extend(rest.query_all(filter).await?);
    }
    events.sort_by_key(|event| (event.created_at, event.id));
    Ok(events)
}

fn reconcile_filters(pubkey: &str, job_id: Option<Uuid>) -> Result<Vec<Filter>, RelayError> {
    let raw_filters = if let Some(job_id) = job_id {
        let coordinate = [job_id.to_string()];
        serde_json::json!([
            {"kinds":[KIND_JOB_REQUEST], "#p":[pubkey], "#d":coordinate},
            {"kinds":[KIND_JOB_ACCEPTED,KIND_JOB_REJECTED,KIND_JOB_COMPLETED,KIND_JOB_BLOCKED,KIND_JOB_DELEGATED], "authors":[pubkey], "#d":coordinate},
            {"kinds":[KIND_JOB_EXECUTION_ATTEMPT,KIND_JOB_SUPPLEMENTAL_CONTEXT], "authors":[pubkey], "#d":coordinate}
        ])
    } else {
        serde_json::json!([
            {"kinds":[KIND_JOB_REQUEST], "#p":[pubkey]},
            {"kinds":[KIND_JOB_ACCEPTED,KIND_JOB_REJECTED,KIND_JOB_COMPLETED,KIND_JOB_BLOCKED,KIND_JOB_DELEGATED], "authors":[pubkey]},
            {"kinds":[KIND_JOB_EXECUTION_ATTEMPT,KIND_JOB_SUPPLEMENTAL_CONTEXT], "authors":[pubkey]}
        ])
    };
    serde_json::from_value(raw_filters).map_err(RelayError::Json)
}

async fn reconcile_events(
    rest: &RestClient,
    already_queued: &HashSet<Uuid>,
    deferred_channels: &HashSet<Uuid>,
    lease_seconds: u64,
    events: Vec<Event>,
    only_job_id: Option<Uuid>,
) -> Result<ReconcileSnapshot, RelayError> {
    let jobs = project_jobs(&events);

    // Relay schema permits more than one accepted job in a channel. ACP has
    // only one serial execution lane per channel, so fail closed instead of
    // nondeterministically assigning that lane to one of multiple owners.
    let mut accepted_channel_counts = HashMap::<Uuid, usize>::new();
    for job in jobs.values().filter(|job| job.state == JobState::Accepted) {
        if let Ok(request) = parse_job_request(&job.request) {
            *accepted_channel_counts
                .entry(request.channel_id)
                .or_default() += 1;
        }
    }

    let mut attempts: HashMap<
        Uuid,
        Vec<(Event, buzz_core::execution_attempt::ExecutionAttemptEvent)>,
    > = HashMap::new();
    let mut supplemental_admissions = HashMap::<Uuid, Vec<(Event, SupplementalAdmission)>>::new();
    for event in events {
        if let Ok(attempt) = parse_execution_attempt(&event) {
            attempts
                .entry(attempt.job_id)
                .or_default()
                .push((event, attempt));
        } else if let Ok(admission) = parse_supplemental_admission(&event) {
            supplemental_admissions
                .entry(admission.job_id)
                .or_default()
                .push((event, admission));
        }
    }

    let mut work = Vec::new();
    let mut accepted_jobs = Vec::new();
    for (job_id, job) in jobs {
        if job.state != JobState::Accepted {
            continue;
        }
        let mut history = attempts.remove(&job_id).unwrap_or_default();
        history.sort_by_key(|(event, attempt)| (attempt.generation, event.created_at, event.id));
        let request =
            parse_job_request(&job.request).map_err(|error| RelayError::Http(error.to_string()))?;
        accepted_jobs.push(AcceptedJobGuard {
            job_id,
            request_event_id: request.request_event_id.clone(),
            channel_id: request.channel_id,
            last_execution: latest_execution_context(&history),
        });
        if only_job_id.is_some_and(|only| only != job_id) {
            continue;
        }
        if accepted_channel_counts
            .get(&request.channel_id)
            .copied()
            .unwrap_or_default()
            != 1
        {
            tracing::error!(
                job_id = %job_id,
                channel_id = %request.channel_id,
                "refusing delegated-job claim because multiple accepted jobs own one channel"
            );
            continue;
        }
        if deferred_channels.contains(&request.channel_id) {
            continue;
        }
        let next = reconcile_decision(&history, chrono::Utc::now().timestamp());
        let continuation_generation = match next {
            ReconcileDecision::Use(index) => history[index].1.generation,
            ReconcileDecision::Recover { runnable, .. } => history[runnable].1.generation,
            ReconcileDecision::Create(generation) => generation,
            ReconcileDecision::None => continue,
        };
        let (supplemental_messages, _unanswered_supplemental_events) =
            recover_supplemental_messages(
                rest,
                request.channel_id,
                &request.request_event_id,
                supplemental_admissions.remove(&job_id).unwrap_or_default(),
                continuation_generation,
            )
            .await?;
        let (runnable_event, runnable) = match next {
            ReconcileDecision::Use(index) => history[index].clone(),
            ReconcileDecision::Recover { runnable, .. } => history[runnable].clone(),
            ReconcileDecision::Create(generation) => {
                let attempt_id = Uuid::new_v4();
                let event = build_runnable(rest, &request, attempt_id, generation)?;
                require_accepted(rest.submit_event(&event).await?)?;
                tracing::info!(
                    job_id = %job_id,
                    attempt_id = %attempt_id,
                    generation,
                    runnable_event_id = %event.id,
                    "delegated-job runnable persisted"
                );
                let parsed = parse_execution_attempt(&event)
                    .map_err(|error| RelayError::Http(error.to_string()))?;
                (event, parsed)
            }
            ReconcileDecision::None => continue,
        };
        if already_queued.contains(&runnable.attempt_id) {
            continue;
        }
        if let ReconcileDecision::Recover { claim, .. } = next {
            let (claim_event, claim) = &history[claim];
            let Some(turn_id) = claim.turn_id.clone() else {
                continue;
            };
            let Some(lease_until) = claim.lease_until else {
                continue;
            };
            tracing::warn!(
                job_id = %job_id,
                attempt_id = %runnable.attempt_id,
                generation = runnable.generation,
                claim_event_id = %claim_event.id,
                %turn_id,
                lease_until,
                "recovering durably claimed but locally undispatched generation"
            );
            work.push(ContinuationWork {
                request: job.request,
                execution: JobExecutionContext {
                    job_id,
                    request_event_id: runnable.request_event_id,
                    attempt_id: runnable.attempt_id,
                    generation: runnable.generation,
                    runnable_event_id: runnable_event.id.to_hex(),
                    claim_event_id: claim_event.id.to_hex(),
                    turn_id,
                    lease_until,
                },
                supplemental_messages,
            });
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
        tracing::info!(
            job_id = %job_id,
            attempt_id = %runnable.attempt_id,
            generation = runnable.generation,
            runnable_event_id = %runnable_event.id,
            claim_event_id = %claim_event.id,
            turn_id = %turn_id,
            lease_until,
            "delegated-job generation claimed before enqueue"
        );
        work.push(ContinuationWork {
            request: job.request,
            execution: JobExecutionContext {
                claim_event_id: claim_event.id.to_hex(),
                ..execution
            },
            supplemental_messages,
        });
    }
    Ok(ReconcileSnapshot {
        work,
        accepted_jobs,
    })
}

fn latest_execution_context(
    history: &[(Event, buzz_core::execution_attempt::ExecutionAttemptEvent)],
) -> Option<JobExecutionContext> {
    let (_, claim) = history
        .iter()
        .filter(|(_, attempt)| attempt.action == AttemptAction::Claim)
        .max_by_key(|(event, attempt)| (attempt.generation, event.created_at, event.id))?;
    let claim_event = history.iter().find(|(_, candidate)| {
        candidate.action == AttemptAction::Claim
            && candidate.attempt_id == claim.attempt_id
            && candidate.generation == claim.generation
    })?;
    let runnable = history.iter().find(|(event, candidate)| {
        candidate.action == AttemptAction::Runnable
            && candidate.attempt_id == claim.attempt_id
            && claim.parent_event_id == Some(event.id.to_hex())
    })?;
    Some(JobExecutionContext {
        job_id: claim.job_id,
        request_event_id: claim.request_event_id.clone(),
        attempt_id: claim.attempt_id,
        generation: claim.generation,
        runnable_event_id: runnable.0.id.to_hex(),
        claim_event_id: claim_event.0.id.to_hex(),
        turn_id: claim.turn_id.clone()?,
        lease_until: claim.lease_until.unwrap_or_default(),
    })
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FollowupBoundaryDetail {
    supplemental_event_id: String,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
struct SupplementalAdmissionContent {
    content: String,
}

/// Durably attest that an inbound event passed ACP author/rule admission while
/// an accepted job owned the channel. Recovery consumes only these exact,
/// agent-signed identities; it never projects arbitrary later channel traffic.
pub async fn persist_supplemental_admission(
    rest: &RestClient,
    guard: &AcceptedJobGuard,
    source: &Event,
    continuation_generation: i64,
) -> Result<Event, RelayError> {
    if continuation_generation < 1 {
        return Err(RelayError::Http(
            "continuation generation must be positive".into(),
        ));
    }
    let content = serde_json::to_string(&SupplementalAdmissionContent {
        content: source.content.clone(),
    })?;
    let event = EventBuilder::new(Kind::Custom(KIND_JOB_SUPPLEMENTAL_CONTEXT as u16), content)
        .tags([
            Tag::parse(["d", &guard.job_id.to_string()])?,
            Tag::parse(["job-request", &guard.request_event_id])?,
            Tag::parse(["job-target", &rest.keys.public_key().to_hex()])?,
            Tag::parse(["h", &guard.channel_id.to_string()])?,
            Tag::parse(["supplemental-event", &source.id.to_hex()])?,
            Tag::parse(["supplemental-author", &source.pubkey.to_hex()])?,
            Tag::parse([
                "continuation-generation",
                &continuation_generation.to_string(),
            ])?,
        ])
        .sign_with_keys(&rest.keys)?;
    require_accepted(rest.submit_event(&event).await?)?;
    Ok(event)
}

fn parse_supplemental_admission(event: &Event) -> Result<SupplementalAdmission, RelayError> {
    parse_supplemental_context(event).map_err(|error| RelayError::Http(error.to_string()))
}

pub fn followup_boundary_detail(event_id: &str) -> Result<String, serde_json::Error> {
    serde_json::to_string(&FollowupBoundaryDetail {
        supplemental_event_id: event_id.to_owned(),
    })
}

async fn recover_supplemental_messages(
    rest: &RestClient,
    channel_id: Uuid,
    request_event_id: &str,
    mut admissions: Vec<(Event, SupplementalAdmission)>,
    continuation_generation: i64,
) -> Result<(Vec<JobSupplementalMessage>, Vec<Event>), RelayError> {
    admissions.retain(|(_, admission)| {
        admission.channel_id == channel_id
            && admission.request_event_id == request_event_id
            && admission.continuation_generation == continuation_generation
    });
    admissions.sort_by_key(|(event, _)| (event.created_at, event.id));
    let mut seen = HashSet::new();
    admissions.retain(|(_, admission)| seen.insert(admission.source_event_id.clone()));
    if admissions.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let source_ids: Vec<String> = admissions
        .iter()
        .map(|(_, admission)| admission.source_event_id.clone())
        .collect();
    let source_filter: Filter = serde_json::from_value(serde_json::json!({
        "ids": source_ids,
        "kinds": [buzz_core::kind::KIND_STREAM_MESSAGE],
        "#h": [channel_id.to_string()]
    }))?;
    let sources = rest.query_all(&source_filter).await?;
    let response_filter: Filter = serde_json::from_value(serde_json::json!({
        "kinds": [buzz_core::kind::KIND_STREAM_MESSAGE],
        "authors": [rest.keys.public_key().to_hex()],
        "#h": [channel_id.to_string()],
        "#e": source_ids
    }))?;
    let responses = rest.query_all(&response_filter).await?;
    Ok(project_admitted_supplemental(
        admissions, &sources, &responses,
    ))
}

fn project_admitted_supplemental(
    admissions: Vec<(Event, SupplementalAdmission)>,
    sources: &[Event],
    responses: &[Event],
) -> (Vec<JobSupplementalMessage>, Vec<Event>) {
    let mut messages = Vec::new();
    let mut unanswered = Vec::new();
    for (_, admission) in admissions {
        let source = sources.iter().find(|event| {
            event.id.to_hex() == admission.source_event_id
                && event.pubkey.to_hex() == admission.source_author
                && event.content == admission.content
        });
        let response = responses.iter().find(|response| {
            crate::queue::parse_thread_tags(response)
                .parent_event_id
                .as_deref()
                == Some(admission.source_event_id.as_str())
        });
        messages.push(JobSupplementalMessage {
            event_id: admission.source_event_id,
            content: admission.content,
            response_event_id: response.map(|event| event.id.to_hex()),
            response_content: response.map(|event| event.content.clone()),
        });
        if response.is_none() {
            if let Some(source) = source {
                unanswered.push(source.clone());
            }
        }
    }
    (messages, unanswered)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileDecision {
    None,
    Use(usize),
    Recover { runnable: usize, claim: usize },
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
    let claim = history.iter().enumerate().find(|(_, (_, attempt))| {
        attempt.generation == generation
            && attempt.action == AttemptAction::Claim
            && attempt.parent_event_id.as_deref() == Some(runnable_id.as_str())
    });
    let Some((claim_index, (claim_event, claim))) = claim else {
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
        ReconcileDecision::Recover {
            runnable: runnable_index,
            claim: claim_index,
        }
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
            ReconcileDecision::Recover {
                runnable: 1,
                claim: 0
            },
            "restart recovery must reuse the live durable claim"
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
    fn cancel_and_merge_finish_mints_next_generation_without_followup_reply() {
        let runnable = attempt_event("runnable", 1, None);
        let mut claim = attempt_event("claim", 1, Some(200));
        claim.1.parent_event_id = Some(runnable.0.id.to_hex());
        let mut finish = attempt_event("finish", 1, None);
        finish.1.parent_event_id = Some(claim.0.id.to_hex());
        finish.1.turn_id = claim.1.turn_id.clone();
        finish.1.outcome = Some("cancel_and_merge".into());
        finish.1.detail = followup_boundary_detail(&"ab".repeat(32)).expect("detail");

        assert_eq!(
            reconcile_decision(&[runnable, claim, finish], 100),
            ReconcileDecision::Create(2),
            "a conversational response is optional and cannot gate continuation"
        );
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
    fn exact_reconciliation_filters_bind_every_query_to_one_job() {
        let job_id = Uuid::new_v4();
        let filters = reconcile_filters(&"ab".repeat(32), Some(job_id)).expect("filters");
        assert_eq!(filters.len(), 3);
        for filter in filters {
            let value = serde_json::to_value(filter).expect("serialize");
            assert_eq!(
                value.get("#d"),
                Some(&serde_json::json!([job_id.to_string()]))
            );
        }

        let broad = reconcile_filters(&"ab".repeat(32), None).expect("filters");
        assert!(broad.into_iter().all(|filter| {
            serde_json::to_value(filter)
                .expect("serialize")
                .get("#d")
                .is_none()
        }));
    }

    #[test]
    fn evaluation_context_is_bound_to_the_immutable_request_coordinates() {
        let requester = Keys::generate();
        let target = Keys::generate();
        let job_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let request = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), "assignment")
            .tags([
                Tag::parse(["d", &job_id.to_string()]).expect("tag"),
                Tag::parse(["job-target", &target.public_key().to_hex()]).expect("tag"),
                Tag::parse(["p", &target.public_key().to_hex()]).expect("tag"),
                Tag::parse(["h", &channel_id.to_string()]).expect("tag"),
            ])
            .sign_with_keys(&requester)
            .expect("request");

        assert_eq!(
            evaluation_context(&request),
            Some(JobEvaluationContext {
                job_id,
                request_event_id: request.id.to_hex(),
                channel_id,
            })
        );
    }

    #[test]
    fn continuation_prompt_preserves_execution_identity_and_supplemental_context() {
        let execution = JobExecutionContext {
            job_id: Uuid::new_v4(),
            request_event_id: "ab".repeat(32),
            attempt_id: Uuid::new_v4(),
            generation: 2,
            runnable_event_id: "cd".repeat(32),
            claim_event_id: "ef".repeat(32),
            turn_id: "turn-2".into(),
            lease_until: i64::MAX,
        };
        let supplemental = vec![JobSupplementalMessage {
            event_id: "01".repeat(32),
            content: "What remains?".into(),
            response_event_id: Some("02".repeat(32)),
            response_content: Some("Checkpoint retained.".into()),
        }];
        let encoded = prompt_tag_with_supplemental_messages(&execution, supplemental.clone())
            .expect("prompt tag");

        let decoded = from_prompt_tag(&encoded).expect("execution");
        assert_eq!(decoded.job_id, execution.job_id);
        assert_eq!(decoded.attempt_id, execution.attempt_id);
        assert_eq!(decoded.generation, 2);
        assert_eq!(
            supplemental_messages_from_prompt_tag(&encoded),
            supplemental
        );

        let legacy = prompt_tag(&execution).expect("legacy prompt tag");
        assert_eq!(
            from_prompt_tag(&legacy).map(|context| context.attempt_id),
            Some(execution.attempt_id)
        );
        assert!(supplemental_messages_from_prompt_tag(&legacy).is_empty());
    }

    #[test]
    fn followup_prompt_tag_cannot_be_mistaken_for_execution_authority() {
        let followup = JobFollowupContext {
            job_id: Uuid::new_v4(),
            request_event_id: "ab".repeat(32),
            interrupted_attempt_id: Some(Uuid::new_v4()),
            interrupted_generation: Some(1),
            interrupted_turn_id: Some("turn-1".into()),
        };
        let encoded = followup_prompt_tag(&followup).expect("follow-up tag");

        assert_eq!(followup_from_prompt_tag(&encoded), Some(followup));
        assert!(from_prompt_tag(&encoded).is_none());
    }

    #[test]
    fn supplemental_admission_is_exactly_bound_to_job_generation_and_source() {
        let user = Keys::generate();
        let agent = Keys::generate();
        let job_id = Uuid::new_v4();
        let channel = Uuid::new_v4();
        let source = EventBuilder::new(
            Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE as u16),
            "status?",
        )
        .tags([Tag::parse(["h", &channel.to_string()]).expect("tag")])
        .sign_with_keys(&user)
        .expect("event");
        let admission = EventBuilder::new(
            Kind::Custom(KIND_JOB_SUPPLEMENTAL_CONTEXT as u16),
            serde_json::to_string(&SupplementalAdmissionContent {
                content: source.content.clone(),
            })
            .expect("content"),
        )
        .tags([
            Tag::parse(["d", &job_id.to_string()]).expect("tag"),
            Tag::parse(["job-request", &"01".repeat(32)]).expect("tag"),
            Tag::parse(["job-target", &agent.public_key().to_hex()]).expect("tag"),
            Tag::parse(["h", &channel.to_string()]).expect("tag"),
            Tag::parse(["supplemental-event", &source.id.to_hex()]).expect("tag"),
            Tag::parse(["supplemental-author", &user.public_key().to_hex()]).expect("tag"),
            Tag::parse(["continuation-generation", "2"]).expect("tag"),
        ])
        .sign_with_keys(&agent)
        .expect("event");
        let parsed = parse_supplemental_admission(&admission).expect("admission");

        assert_eq!(parsed.job_id, job_id);
        assert_eq!(parsed.channel_id, channel);
        assert_eq!(parsed.source_event_id, source.id.to_hex());
        assert_eq!(parsed.source_author, user.public_key().to_hex());
        assert_eq!(parsed.continuation_generation, 2);
        assert_eq!(parsed.content, "status?");

        let unrelated = EventBuilder::new(
            Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE as u16),
            "unrelated later chatter",
        )
        .tags([Tag::parse(["h", &channel.to_string()]).expect("tag")])
        .sign_with_keys(&user)
        .expect("event");
        let (messages, unanswered) = project_admitted_supplemental(
            vec![(admission.clone(), parsed.clone())],
            &[source.clone(), unrelated],
            &[],
        );
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].event_id, source.id.to_hex());
        assert_eq!(unanswered, vec![source.clone()]);

        let (messages, unanswered) =
            project_admitted_supplemental(vec![(admission, parsed)], &[], &[]);
        assert_eq!(
            messages.len(),
            1,
            "durable content survives source deletion"
        );
        assert!(
            unanswered.is_empty(),
            "optional reply is skipped, not gated"
        );
    }

    #[test]
    fn durable_followup_boundary_detail_round_trips() {
        let event_id = "ab".repeat(32);
        let detail = followup_boundary_detail(&event_id).expect("detail");
        let decoded: FollowupBoundaryDetail = serde_json::from_str(&detail).expect("decode");
        assert_eq!(decoded.supplemental_event_id, event_id);
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
