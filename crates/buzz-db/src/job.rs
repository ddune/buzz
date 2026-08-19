//! Atomic persistence and reconstruction for delegated-job events.

use buzz_core::delegated_job::{JobAction, JobLifecycleEvent, JobRequest, JobState};
use buzz_core::{CommunityId, StoredEvent};
use chrono::{DateTime, Utc};
use nostr::Event;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::DbError;

/// Accepted write outcome.
#[derive(Debug)]
pub struct JobWriteOutcome {
    /// Stored event wrapper for relay fan-out.
    pub stored_event: StoredEvent,
    /// False only for replay of the identical signed event.
    pub was_inserted: bool,
}

/// Current durable state returned by job queries.
#[derive(Debug, Clone)]
pub struct JobRecord {
    /// Stable job UUID.
    pub job_id: Uuid,
    /// Original signed request event ID.
    pub request_event_id: Vec<u8>,
    /// Requester pubkey.
    pub requester: Vec<u8>,
    /// Target managed-agent pubkey.
    pub target_agent: Vec<u8>,
    /// Originating channel.
    pub channel_id: Uuid,
    /// Current lifecycle state.
    pub state: JobState,
    /// Acceptance event when accepted.
    pub acceptance_event_id: Option<Vec<u8>>,
    /// Terminal lifecycle event when closed.
    pub terminal_event_id: Option<Vec<u8>>,
    /// Successor target for delegated/transferred.
    pub successor_agent: Option<Vec<u8>>,
}

/// Protocol rejection or persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum JobWriteError {
    /// Signed event conflicts with current durable job state.
    #[error("{0}")]
    Rejected(String),
    /// Database failure.
    #[error(transparent)]
    Database(#[from] DbError),
}

impl From<sqlx::Error> for JobWriteError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(DbError::Sqlx(value))
    }
}

/// Atomically create one requested job and store its request event.
pub async fn accept_request(
    pool: &PgPool,
    community: CommunityId,
    event: &Event,
    request: &JobRequest,
) -> Result<JobWriteOutcome, JobWriteError> {
    let mut tx = pool.begin().await?;
    lock_job(&mut tx, community, request.job_id).await?;

    if let Some(existing) = load_job_for_update(&mut tx, community, request.job_id).await? {
        if existing.request_event_id == event.id.as_bytes().as_slice() {
            tx.rollback().await?;
            return Ok(replay_outcome(event, request.channel_id));
        }
        return Err(JobWriteError::Rejected(
            "conflicting reuse of existing job ID".into(),
        ));
    }

    let requester = hex::decode(&request.requester)
        .map_err(|_| JobWriteError::Rejected("malformed requester pubkey".into()))?;
    let target = hex::decode(&request.target_agent)
        .map_err(|_| JobWriteError::Rejected("malformed target pubkey".into()))?;
    let created_at = event_timestamp(event)?;
    let assignment_hash = Sha256::digest(request.assignment.as_bytes()).to_vec();

    insert_event(&mut tx, community, event, request.channel_id, created_at).await?;
    sqlx::query(
        "INSERT INTO delegated_jobs \
         (community_id,job_id,request_event_id,requester,target_agent,channel_id,assignment_hash,state,created_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,'requested',$8)",
    )
    .bind(community.as_uuid())
    .bind(request.job_id)
    .bind(event.id.as_bytes().as_slice())
    .bind(requester)
    .bind(target)
    .bind(request.channel_id)
    .bind(assignment_hash)
    .bind(created_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(inserted_outcome(event, request.channel_id))
}

/// Atomically validate and apply one target-authored lifecycle event.
pub async fn accept_lifecycle(
    pool: &PgPool,
    community: CommunityId,
    event: &Event,
    lifecycle: &JobLifecycleEvent,
) -> Result<JobWriteOutcome, JobWriteError> {
    let mut tx = pool.begin().await?;
    lock_job(&mut tx, community, lifecycle.job_id).await?;

    if event_exists(&mut tx, community, event).await? {
        tx.rollback().await?;
        return Ok(replay_outcome(event, lifecycle.channel_id));
    }
    let existing = load_job_for_update(&mut tx, community, lifecycle.job_id)
        .await?
        .ok_or_else(|| JobWriteError::Rejected("unknown job ID".into()))?;
    if existing.request_event_id != hex::decode(&lifecycle.request_event_id).unwrap_or_default() {
        return Err(JobWriteError::Rejected(
            "job-request does not identify the immutable request event".into(),
        ));
    }
    if existing.channel_id != lifecycle.channel_id {
        return Err(JobWriteError::Rejected(
            "job lifecycle event changed originating channel".into(),
        ));
    }
    if existing.target_agent != event.pubkey.to_bytes().as_slice() {
        return Err(JobWriteError::Rejected(
            "job lifecycle event must be authored by target agent".into(),
        ));
    }
    let expected_parent = match existing.state {
        JobState::Requested => existing.request_event_id.as_slice(),
        JobState::Accepted => existing.acceptance_event_id.as_deref().ok_or_else(|| {
            JobWriteError::Rejected("accepted job has no acceptance event".into())
        })?,
        _ => existing.request_event_id.as_slice(),
    };
    if expected_parent != hex::decode(&lifecycle.parent_event_id).unwrap_or_default() {
        return Err(JobWriteError::Rejected(
            "job-parent does not identify the current lifecycle head".into(),
        ));
    }
    let next = existing
        .state
        .apply(lifecycle.action)
        .map_err(|error| JobWriteError::Rejected(error.to_string()))?;
    let created_at = event_timestamp(event)?;
    insert_event(&mut tx, community, event, lifecycle.channel_id, created_at).await?;
    let successor = lifecycle
        .successor_agent
        .as_ref()
        .map(hex::decode)
        .transpose()
        .map_err(|_| JobWriteError::Rejected("malformed successor pubkey".into()))?;
    let acceptance_id =
        (lifecycle.action == JobAction::Accepted).then(|| event.id.as_bytes().as_slice().to_vec());
    let terminal_id = lifecycle
        .action
        .is_terminal()
        .then(|| event.id.as_bytes().as_slice().to_vec());
    sqlx::query(
        "UPDATE delegated_jobs SET state=$3, \
         acceptance_event_id=COALESCE(acceptance_event_id,$4), terminal_event_id=$5, \
         successor_agent=$6, updated_at=now() WHERE community_id=$1 AND job_id=$2",
    )
    .bind(community.as_uuid())
    .bind(lifecycle.job_id)
    .bind(next.as_str())
    .bind(acceptance_id)
    .bind(terminal_id)
    .bind(successor)
    .execute(&mut *tx)
    .await?;
    if lifecycle.action.is_terminal() {
        sqlx::query(
            "UPDATE job_execution_attempts SET status='suppressed', updated_at=now() \
             WHERE community_id=$1 AND job_id=$2 AND status IN ('runnable','active')",
        )
        .bind(community.as_uuid())
        .bind(lifecycle.job_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(inserted_outcome(event, lifecycle.channel_id))
}

/// Fetch one materialized job by stable ID.
pub async fn get_job(
    pool: &PgPool,
    community: CommunityId,
    job_id: Uuid,
) -> crate::Result<Option<JobRecord>> {
    let row = sqlx::query(
        "SELECT job_id,request_event_id,requester,target_agent,channel_id,state,\
         acceptance_event_id,terminal_event_id,successor_agent \
         FROM delegated_jobs WHERE community_id=$1 AND job_id=$2",
    )
    .bind(community.as_uuid())
    .bind(job_id)
    .fetch_optional(pool)
    .await?;
    row.map(row_to_record).transpose()
}

/// List jobs targeting one agent, optionally limited to accepted non-terminal work.
pub async fn list_jobs_for_agent(
    pool: &PgPool,
    community: CommunityId,
    target: &[u8],
    accepted_non_terminal_only: bool,
) -> crate::Result<Vec<JobRecord>> {
    let rows = sqlx::query(
        "SELECT job_id,request_event_id,requester,target_agent,channel_id,state,\
         acceptance_event_id,terminal_event_id,successor_agent \
         FROM delegated_jobs WHERE community_id=$1 AND target_agent=$2 \
         AND (NOT $3 OR state='accepted') ORDER BY created_at DESC",
    )
    .bind(community.as_uuid())
    .bind(target)
    .bind(accepted_non_terminal_only)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(row_to_record).collect()
}

pub(crate) async fn lock_job(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    community: CommunityId,
    job_id: Uuid,
) -> Result<(), sqlx::Error> {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&job_id.as_bytes()[..8]);
    let key = i64::from_be_bytes(bytes)
        ^ i64::from_be_bytes(
            community.as_uuid().as_bytes()[..8]
                .try_into()
                .unwrap_or([0; 8]),
        );
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(crate) async fn load_job_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    community: CommunityId,
    job_id: Uuid,
) -> Result<Option<JobRecord>, DbError> {
    let row = sqlx::query(
        "SELECT job_id,request_event_id,requester,target_agent,channel_id,state,\
         acceptance_event_id,terminal_event_id,successor_agent \
         FROM delegated_jobs WHERE community_id=$1 AND job_id=$2 FOR UPDATE",
    )
    .bind(community.as_uuid())
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(row_to_record).transpose()
}

fn row_to_record(row: sqlx::postgres::PgRow) -> crate::Result<JobRecord> {
    let state_text: String = row.try_get("state")?;
    let state = JobState::parse(&state_text)
        .ok_or_else(|| DbError::InvalidData(format!("unknown delegated job state {state_text}")))?;
    Ok(JobRecord {
        job_id: row.try_get("job_id")?,
        request_event_id: row.try_get("request_event_id")?,
        requester: row.try_get("requester")?,
        target_agent: row.try_get("target_agent")?,
        channel_id: row.try_get("channel_id")?,
        state,
        acceptance_event_id: row.try_get("acceptance_event_id")?,
        terminal_event_id: row.try_get("terminal_event_id")?,
        successor_agent: row.try_get("successor_agent")?,
    })
}

pub(crate) async fn event_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    community: CommunityId,
    event: &Event,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM events WHERE community_id=$1 AND id=$2)")
        .bind(community.as_uuid())
        .bind(event.id.as_bytes().as_slice())
        .fetch_one(&mut **tx)
        .await
}

pub(crate) async fn insert_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    community: CommunityId,
    event: &Event,
    channel_id: Uuid,
    created_at: DateTime<Utc>,
) -> Result<(), JobWriteError> {
    let tags = serde_json::to_value(&event.tags).map_err(DbError::from)?;
    let d_tag = event
        .tags
        .iter()
        .find(|tag| tag.as_slice().first().is_some_and(|value| value == "d"))
        .and_then(|tag| tag.as_slice().get(1))
        .cloned();
    let sig = event.sig.serialize();
    let result = sqlx::query(
        "INSERT INTO events \
         (community_id,id,pubkey,created_at,kind,tags,content,sig,received_at,channel_id,d_tag) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now(),$9,$10) ON CONFLICT DO NOTHING",
    )
    .bind(community.as_uuid())
    .bind(event.id.as_bytes().as_slice())
    .bind(event.pubkey.to_bytes().as_slice())
    .bind(created_at)
    .bind(buzz_core::kind::event_kind_i32(event))
    .bind(tags)
    .bind(&event.content)
    .bind(sig.as_slice())
    .bind(channel_id)
    .bind(d_tag)
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() != 1 {
        return Err(JobWriteError::Rejected("duplicate event ID".into()));
    }
    Ok(())
}

pub(crate) fn event_timestamp(event: &Event) -> Result<DateTime<Utc>, JobWriteError> {
    DateTime::from_timestamp(event.created_at.as_secs() as i64, 0).ok_or_else(|| {
        JobWriteError::Database(DbError::InvalidTimestamp(event.created_at.as_secs() as i64))
    })
}

pub(crate) fn inserted_outcome(event: &Event, channel_id: Uuid) -> JobWriteOutcome {
    JobWriteOutcome {
        stored_event: StoredEvent::with_received_at(
            event.clone(),
            Utc::now(),
            Some(channel_id),
            true,
        ),
        was_inserted: true,
    }
}

pub(crate) fn replay_outcome(event: &Event, channel_id: Uuid) -> JobWriteOutcome {
    JobWriteOutcome {
        stored_event: StoredEvent::with_received_at(
            event.clone(),
            Utc::now(),
            Some(channel_id),
            true,
        ),
        was_inserted: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::delegated_job::{parse_job_lifecycle, parse_job_request};
    use nostr::{EventBuilder, Keys, Kind, Tag};

    const TEST_DB_URL: &str = "postgres://buzz:buzz_dev@localhost:5432/buzz";

    async fn fixture() -> (PgPool, CommunityId, Uuid) {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| TEST_DB_URL.into());
        let pool = PgPool::connect(&url).await.expect("connect test DB");
        if std::env::var_os("BUZZ_TEST_SCHEMA_READY").is_none() {
            crate::Db::from_pool(pool.clone())
                .migrate()
                .await
                .expect("migrate test DB");
        }
        let community_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let creator = Keys::generate().public_key().to_bytes();
        sqlx::query("INSERT INTO communities (id,host) VALUES ($1,$2)")
            .bind(community_id)
            .bind(format!("job-test-{}.example", community_id.simple()))
            .execute(&pool)
            .await
            .expect("community");
        sqlx::query(
            "INSERT INTO channels (community_id,id,name,created_by) VALUES ($1,$2,'jobs',$3)",
        )
        .bind(community_id)
        .bind(channel_id)
        .bind(creator.as_slice())
        .execute(&pool)
        .await
        .expect("channel");
        (pool, CommunityId::from_uuid(community_id), channel_id)
    }

    async fn cleanup(pool: &PgPool, community: CommunityId) {
        sqlx::query("DELETE FROM events WHERE community_id=$1")
            .bind(community.as_uuid())
            .execute(pool)
            .await
            .expect("cleanup events");
        sqlx::query("DELETE FROM channels WHERE community_id=$1")
            .bind(community.as_uuid())
            .execute(pool)
            .await
            .expect("cleanup channels");
        sqlx::query("DELETE FROM communities WHERE id=$1")
            .bind(community.as_uuid())
            .execute(pool)
            .await
            .expect("cleanup");
    }

    fn request_event(
        requester: &Keys,
        target: &Keys,
        job: Uuid,
        channel: Uuid,
        content: &str,
    ) -> Event {
        EventBuilder::new(
            Kind::Custom(buzz_core::kind::KIND_JOB_REQUEST as u16),
            content,
        )
        .tags([
            Tag::parse(["d", &job.to_string()]).expect("job tag"),
            Tag::parse(["job-target", &target.public_key().to_hex()]).expect("target tag"),
            Tag::parse(["p", &target.public_key().to_hex()]).expect("routing tag"),
            Tag::parse(["h", &channel.to_string()]).expect("channel tag"),
        ])
        .sign_with_keys(requester)
        .expect("request event")
    }

    fn lifecycle_event(
        author: &Keys,
        kind: u32,
        job: Uuid,
        request: &Event,
        parent: &Event,
        channel: Uuid,
    ) -> Event {
        EventBuilder::new(Kind::Custom(kind as u16), "state change")
            .tags([
                Tag::parse(["d", &job.to_string()]).expect("job tag"),
                Tag::parse(["job-request", &request.id.to_hex()]).expect("request tag"),
                Tag::parse(["job-parent", &parent.id.to_hex()]).expect("parent tag"),
                Tag::parse(["h", &channel.to_string()]).expect("channel tag"),
            ])
            .sign_with_keys(author)
            .expect("lifecycle event")
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn request_replay_is_idempotent_but_job_id_reuse_is_rejected() {
        let (pool, community, channel) = fixture().await;
        let requester = Keys::generate();
        let target = Keys::generate();
        let job = Uuid::new_v4();
        let event = request_event(&requester, &target, job, channel, "first assignment");
        let parsed = parse_job_request(&event).expect("parse request");
        assert!(
            accept_request(&pool, community, &event, &parsed)
                .await
                .expect("insert")
                .was_inserted
        );
        assert!(
            !accept_request(&pool, community, &event, &parsed)
                .await
                .expect("replay")
                .was_inserted
        );
        let indexed_job_id: Option<String> =
            sqlx::query_scalar("SELECT d_tag FROM events WHERE community_id=$1 AND id=$2")
                .bind(community.as_uuid())
                .bind(event.id.as_bytes().as_slice())
                .fetch_one(&pool)
                .await
                .expect("query indexed job id");
        assert_eq!(indexed_job_id.as_deref(), Some(job.to_string().as_str()));

        let conflict = request_event(&requester, &target, job, channel, "different assignment");
        let conflict_parsed = parse_job_request(&conflict).expect("parse conflict");
        assert!(matches!(
            accept_request(&pool, community, &conflict, &conflict_parsed).await,
            Err(JobWriteError::Rejected(_))
        ));
        cleanup(&pool, community).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn target_authority_replay_and_restart_reconstruction_are_durable() {
        let (pool, community, channel) = fixture().await;
        let requester = Keys::generate();
        let target = Keys::generate();
        let attacker = Keys::generate();
        let job = Uuid::new_v4();
        let request = request_event(&requester, &target, job, channel, "bounded assignment");
        accept_request(
            &pool,
            community,
            &request,
            &parse_job_request(&request).expect("parse request"),
        )
        .await
        .expect("request");

        let forged = lifecycle_event(
            &attacker,
            buzz_core::kind::KIND_JOB_ACCEPTED,
            job,
            &request,
            &request,
            channel,
        );
        assert!(matches!(
            accept_lifecycle(
                &pool,
                community,
                &forged,
                &parse_job_lifecycle(&forged).expect("parse forged")
            )
            .await,
            Err(JobWriteError::Rejected(_))
        ));

        let accepted = lifecycle_event(
            &target,
            buzz_core::kind::KIND_JOB_ACCEPTED,
            job,
            &request,
            &request,
            channel,
        );
        let parsed = parse_job_lifecycle(&accepted).expect("parse accepted");
        assert!(
            accept_lifecycle(&pool, community, &accepted, &parsed)
                .await
                .expect("accept")
                .was_inserted
        );
        assert!(
            !accept_lifecycle(&pool, community, &accepted, &parsed)
                .await
                .expect("replay")
                .was_inserted
        );
        drop(crate::Db::from_pool(pool.clone()));
        let record = get_job(&pool, community, job)
            .await
            .expect("restart query")
            .expect("job");
        assert_eq!(record.state, JobState::Accepted);
        assert_eq!(
            list_jobs_for_agent(&pool, community, &target.public_key().to_bytes(), true)
                .await
                .expect("accepted list")
                .len(),
            1
        );
        cleanup(&pool, community).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn conflicting_terminal_race_commits_exactly_one_transition() {
        let (pool, community, channel) = fixture().await;
        let requester = Keys::generate();
        let target = Keys::generate();
        let job = Uuid::new_v4();
        let request = request_event(&requester, &target, job, channel, "race assignment");
        accept_request(
            &pool,
            community,
            &request,
            &parse_job_request(&request).expect("parse request"),
        )
        .await
        .expect("request");
        let accepted = lifecycle_event(
            &target,
            buzz_core::kind::KIND_JOB_ACCEPTED,
            job,
            &request,
            &request,
            channel,
        );
        accept_lifecycle(
            &pool,
            community,
            &accepted,
            &parse_job_lifecycle(&accepted).expect("parse accepted"),
        )
        .await
        .expect("accept");
        let completed = lifecycle_event(
            &target,
            buzz_core::kind::KIND_JOB_COMPLETED,
            job,
            &request,
            &accepted,
            channel,
        );
        let blocked = lifecycle_event(
            &target,
            buzz_core::kind::KIND_JOB_BLOCKED,
            job,
            &request,
            &accepted,
            channel,
        );
        let completed_parsed = parse_job_lifecycle(&completed).expect("complete parse");
        let blocked_parsed = parse_job_lifecycle(&blocked).expect("blocked parse");
        let (left, right) = tokio::join!(
            accept_lifecycle(&pool, community, &completed, &completed_parsed),
            accept_lifecycle(&pool, community, &blocked, &blocked_parsed)
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert!(matches!(
            get_job(&pool, community, job)
                .await
                .expect("query")
                .expect("job")
                .state,
            JobState::Completed | JobState::Blocked
        ));
        cleanup(&pool, community).await;
    }
}
