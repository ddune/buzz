//! Transactional execution-attempt state subordinate to delegated jobs.

use buzz_core::delegated_job::JobState;
use buzz_core::execution_attempt::{AttemptAction, ExecutionAttemptEvent};
use buzz_core::CommunityId;
use chrono::{DateTime, Utc};
use nostr::Event;
use sqlx::{PgPool, Row};

use crate::job::{self, JobWriteError, JobWriteOutcome};

/// Atomically validate and persist one attempt control event.
pub async fn accept(
    pool: &PgPool,
    community: CommunityId,
    event: &Event,
    attempt: &ExecutionAttemptEvent,
) -> Result<JobWriteOutcome, JobWriteError> {
    let mut tx = pool.begin().await?;
    job::lock_job(&mut tx, community, attempt.job_id).await?;

    if job::event_exists(&mut tx, community, event).await? {
        tx.rollback().await?;
        return Ok(job::replay_outcome(event, attempt.channel_id));
    }

    let obligation = job::load_job_for_update(&mut tx, community, attempt.job_id)
        .await?
        .ok_or_else(|| JobWriteError::Rejected("unknown job ID".into()))?;
    if obligation.state != JobState::Accepted {
        return Err(JobWriteError::Rejected(
            "execution attempts require an accepted non-terminal job".into(),
        ));
    }
    if obligation.request_event_id != hex::decode(&attempt.request_event_id).unwrap_or_default()
        || obligation.channel_id != attempt.channel_id
        || obligation.target_agent != event.pubkey.to_bytes().as_slice()
        || attempt.target_agent != event.pubkey.to_hex()
    {
        return Err(JobWriteError::Rejected(
            "attempt changed immutable job request, target, or channel".into(),
        ));
    }

    let created_at = job::event_timestamp(event)?;
    match attempt.action {
        AttemptAction::Runnable => {
            accept_runnable(&mut tx, community, event, attempt, created_at).await?
        }
        AttemptAction::Claim => accept_claim(&mut tx, community, event, attempt).await?,
        AttemptAction::Finish => accept_finish(&mut tx, community, event, attempt).await?,
    }
    job::insert_event(&mut tx, community, event, attempt.channel_id, created_at).await?;
    tx.commit().await?;
    Ok(job::inserted_outcome(event, attempt.channel_id))
}

async fn accept_runnable(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    community: CommunityId,
    event: &Event,
    attempt: &ExecutionAttemptEvent,
    created_at: DateTime<Utc>,
) -> Result<(), JobWriteError> {
    let latest = sqlx::query(
        "SELECT generation,status,lease_expires_at FROM job_execution_attempts \
         WHERE community_id=$1 AND job_id=$2 ORDER BY generation DESC LIMIT 1 FOR UPDATE",
    )
    .bind(community.as_uuid())
    .bind(attempt.job_id)
    .fetch_optional(&mut **tx)
    .await?;

    let expected_generation = match latest {
        None => 1,
        Some(row) => {
            let generation: i64 = row.try_get("generation")?;
            let status: String = row.try_get("status")?;
            let lease: Option<DateTime<Utc>> = row.try_get("lease_expires_at")?;
            match status.as_str() {
                "ended" | "suppressed" => generation + 1,
                "active" if lease.is_some_and(|until| until <= Utc::now()) => {
                    sqlx::query(
                        "UPDATE job_execution_attempts SET status='ended', \
                         runtime_outcome='lease_expired', updated_at=now() \
                         WHERE community_id=$1 AND job_id=$2 AND generation=$3",
                    )
                    .bind(community.as_uuid())
                    .bind(attempt.job_id)
                    .bind(generation)
                    .execute(&mut **tx)
                    .await?;
                    generation + 1
                }
                _ => {
                    return Err(JobWriteError::Rejected(
                        "a valid runnable or active attempt already exists".into(),
                    ))
                }
            }
        }
    };
    if attempt.generation != expected_generation {
        return Err(JobWriteError::Rejected(format!(
            "expected continuation generation {expected_generation}"
        )));
    }
    sqlx::query(
        "INSERT INTO job_execution_attempts \
         (community_id,job_id,generation,attempt_id,request_event_id,target_agent,channel_id,status,runnable_event_id,created_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,'runnable',$8,$9)",
    )
    .bind(community.as_uuid())
    .bind(attempt.job_id)
    .bind(attempt.generation)
    .bind(attempt.attempt_id)
    .bind(hex::decode(&attempt.request_event_id).unwrap_or_default())
    .bind(event.pubkey.to_bytes().as_slice())
    .bind(attempt.channel_id)
    .bind(event.id.as_bytes().as_slice())
    .bind(created_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn accept_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    community: CommunityId,
    event: &Event,
    attempt: &ExecutionAttemptEvent,
) -> Result<(), JobWriteError> {
    let parent =
        hex::decode(attempt.parent_event_id.as_deref().unwrap_or_default()).unwrap_or_default();
    let lease = attempt
        .lease_until
        .and_then(|value| DateTime::from_timestamp(value, 0))
        .ok_or_else(|| JobWriteError::Rejected("invalid claim lease".into()))?;
    if lease <= Utc::now() {
        return Err(JobWriteError::Rejected(
            "claim lease must be in the future".into(),
        ));
    }
    let result = sqlx::query(
        "UPDATE job_execution_attempts SET status='active',claim_event_id=$6,turn_id=$7,session_id=$8,lease_expires_at=$9,updated_at=now() \
         WHERE community_id=$1 AND job_id=$2 AND generation=$3 AND attempt_id=$4 \
         AND status='runnable' AND runnable_event_id=$5",
    )
    .bind(community.as_uuid())
    .bind(attempt.job_id)
    .bind(attempt.generation)
    .bind(attempt.attempt_id)
    .bind(parent)
    .bind(event.id.as_bytes().as_slice())
    .bind(attempt.turn_id.as_deref())
    .bind(attempt.session_id.as_deref())
    .bind(lease)
    .execute(&mut **tx)
    .await?;
    require_one(result.rows_affected(), "stale or already claimed attempt")
}

async fn accept_finish(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    community: CommunityId,
    event: &Event,
    attempt: &ExecutionAttemptEvent,
) -> Result<(), JobWriteError> {
    let parent =
        hex::decode(attempt.parent_event_id.as_deref().unwrap_or_default()).unwrap_or_default();
    let result = sqlx::query(
        "UPDATE job_execution_attempts SET status='ended',outcome_event_id=$6,runtime_outcome=$7,outcome_detail=$8,updated_at=now() \
         WHERE community_id=$1 AND job_id=$2 AND generation=$3 AND attempt_id=$4 \
         AND status='active' AND claim_event_id=$5 AND turn_id=$9",
    )
    .bind(community.as_uuid())
    .bind(attempt.job_id)
    .bind(attempt.generation)
    .bind(attempt.attempt_id)
    .bind(parent)
    .bind(event.id.as_bytes().as_slice())
    .bind(attempt.outcome.as_deref())
    .bind(&attempt.detail)
    .bind(attempt.turn_id.as_deref())
    .execute(&mut **tx)
    .await?;
    require_one(result.rows_affected(), "stale attempt completion")
}

fn require_one(rows: u64, message: &str) -> Result<(), JobWriteError> {
    if rows == 1 {
        Ok(())
    } else {
        Err(JobWriteError::Rejected(message.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::delegated_job::{parse_job_lifecycle, parse_job_request};
    use buzz_core::execution_attempt::parse_execution_attempt;
    use buzz_core::kind::{
        KIND_JOB_ACCEPTED, KIND_JOB_COMPLETED, KIND_JOB_EXECUTION_ATTEMPT, KIND_JOB_REQUEST,
    };
    use nostr::{EventBuilder, Keys, Kind, Tag};
    use uuid::Uuid;

    const TEST_DB_URL: &str = "postgres://buzz:buzz_dev@localhost:5432/buzz";

    async fn fixture() -> (PgPool, CommunityId, Uuid, Keys, Keys, Uuid, Event, Event) {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| TEST_DB_URL.into());
        let pool = PgPool::connect(&url).await.expect("connect test DB");
        if std::env::var_os("BUZZ_TEST_SCHEMA_READY").is_none() {
            crate::Db::from_pool(pool.clone())
                .migrate()
                .await
                .expect("migrate test DB");
        }
        let community_uuid = Uuid::new_v4();
        let community = CommunityId::from_uuid(community_uuid);
        let channel = Uuid::new_v4();
        let requester = Keys::generate();
        let target = Keys::generate();
        sqlx::query("INSERT INTO communities (id,host) VALUES ($1,$2)")
            .bind(community_uuid)
            .bind(format!("attempt-test-{}.example", community_uuid.simple()))
            .execute(&pool)
            .await
            .expect("community");
        sqlx::query(
            "INSERT INTO channels (community_id,id,name,created_by) VALUES ($1,$2,'attempts',$3)",
        )
        .bind(community_uuid)
        .bind(channel)
        .bind(requester.public_key().to_bytes().as_slice())
        .execute(&pool)
        .await
        .expect("channel");
        let job_id = Uuid::new_v4();
        let request = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), "do the work")
            .tags([
                Tag::parse(["d", &job_id.to_string()]).expect("tag"),
                Tag::parse(["job-target", &target.public_key().to_hex()]).expect("tag"),
                Tag::parse(["p", &target.public_key().to_hex()]).expect("tag"),
                Tag::parse(["h", &channel.to_string()]).expect("tag"),
            ])
            .sign_with_keys(&requester)
            .expect("request");
        job::accept_request(
            &pool,
            community,
            &request,
            &parse_job_request(&request).expect("parse request"),
        )
        .await
        .expect("store request");
        let acceptance = EventBuilder::new(Kind::Custom(KIND_JOB_ACCEPTED as u16), "")
            .tags([
                Tag::parse(["d", &job_id.to_string()]).expect("tag"),
                Tag::parse(["job-request", &request.id.to_hex()]).expect("tag"),
                Tag::parse(["job-parent", &request.id.to_hex()]).expect("tag"),
                Tag::parse(["h", &channel.to_string()]).expect("tag"),
            ])
            .sign_with_keys(&target)
            .expect("acceptance");
        (
            pool, community, channel, requester, target, job_id, request, acceptance,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn attempt_event(
        target: &Keys,
        job_id: Uuid,
        request: &Event,
        channel: Uuid,
        attempt_id: Uuid,
        generation: i64,
        action: &str,
        parent: Option<&str>,
        turn: Option<&str>,
        outcome: Option<&str>,
    ) -> Event {
        let mut tags = vec![
            Tag::parse(["d", &job_id.to_string()]).expect("tag"),
            Tag::parse(["job-request", &request.id.to_hex()]).expect("tag"),
            Tag::parse(["attempt", &attempt_id.to_string()]).expect("tag"),
            Tag::parse(["generation", &generation.to_string()]).expect("tag"),
            Tag::parse(["job-target", &target.public_key().to_hex()]).expect("tag"),
            Tag::parse(["h", &channel.to_string()]).expect("tag"),
            Tag::parse(["attempt-action", action]).expect("tag"),
        ];
        if let Some(parent) = parent {
            tags.push(Tag::parse(["attempt-parent", parent]).expect("tag"));
        }
        if let Some(turn) = turn {
            tags.push(Tag::parse(["turn-id", turn]).expect("tag"));
        }
        if action == "claim" {
            tags.push(
                Tag::parse(["lease-until", &(Utc::now().timestamp() + 3600).to_string()])
                    .expect("tag"),
            );
        }
        if let Some(outcome) = outcome {
            tags.push(Tag::parse(["attempt-outcome", outcome]).expect("tag"));
        }
        EventBuilder::new(Kind::Custom(KIND_JOB_EXECUTION_ATTEMPT as u16), "")
            .tags(tags)
            .sign_with_keys(target)
            .expect("attempt event")
    }

    async fn cleanup(pool: &PgPool, community: CommunityId) {
        sqlx::query("DELETE FROM events WHERE community_id=$1")
            .bind(community.as_uuid())
            .execute(pool)
            .await
            .expect("events");
        sqlx::query("DELETE FROM channels WHERE community_id=$1")
            .bind(community.as_uuid())
            .execute(pool)
            .await
            .expect("channels");
        sqlx::query("DELETE FROM communities WHERE id=$1")
            .bind(community.as_uuid())
            .execute(pool)
            .await
            .expect("community");
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn concurrent_reconcilers_commit_exactly_one_runnable_generation() {
        let (pool, community, channel, _, target, job_id, request, acceptance) = fixture().await;
        job::accept_lifecycle(
            &pool,
            community,
            &acceptance,
            &parse_job_lifecycle(&acceptance).expect("parse acceptance"),
        )
        .await
        .expect("accept job");
        let first = attempt_event(
            &target,
            job_id,
            &request,
            channel,
            Uuid::new_v4(),
            1,
            "runnable",
            None,
            None,
            None,
        );
        let second = attempt_event(
            &target,
            job_id,
            &request,
            channel,
            Uuid::new_v4(),
            1,
            "runnable",
            None,
            None,
            None,
        );
        let first_envelope = parse_execution_attempt(&first).expect("parse");
        let second_envelope = parse_execution_attempt(&second).expect("parse");
        let (left, right) = tokio::join!(
            accept(&pool, community, &first, &first_envelope),
            accept(&pool, community, &second, &second_envelope),
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM job_execution_attempts WHERE community_id=$1 AND job_id=$2 AND generation=1",
        )
        .bind(community.as_uuid())
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(count, 1);
        cleanup(&pool, community).await;
    }

    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn end_turn_opens_next_generation_and_terminal_race_leaves_none_runnable() {
        let (pool, community, channel, _, target, job_id, request, acceptance) = fixture().await;
        job::accept_lifecycle(
            &pool,
            community,
            &acceptance,
            &parse_job_lifecycle(&acceptance).expect("parse"),
        )
        .await
        .expect("accept job");
        let attempt_id = Uuid::new_v4();
        let runnable = attempt_event(
            &target, job_id, &request, channel, attempt_id, 1, "runnable", None, None, None,
        );
        accept(
            &pool,
            community,
            &runnable,
            &parse_execution_attempt(&runnable).expect("parse"),
        )
        .await
        .expect("runnable");
        let claim = attempt_event(
            &target,
            job_id,
            &request,
            channel,
            attempt_id,
            1,
            "claim",
            Some(&runnable.id.to_hex()),
            Some("turn-1"),
            None,
        );
        accept(
            &pool,
            community,
            &claim,
            &parse_execution_attempt(&claim).expect("parse"),
        )
        .await
        .expect("claim");
        let finish = attempt_event(
            &target,
            job_id,
            &request,
            channel,
            attempt_id,
            1,
            "finish",
            Some(&claim.id.to_hex()),
            Some("turn-1"),
            Some("end_turn"),
        );
        accept(
            &pool,
            community,
            &finish,
            &parse_execution_attempt(&finish).expect("parse"),
        )
        .await
        .expect("finish");

        let stale_finish = attempt_event(
            &target,
            job_id,
            &request,
            channel,
            attempt_id,
            1,
            "finish",
            Some(&claim.id.to_hex()),
            Some("turn-1"),
            Some("provider_error"),
        );
        let stale_envelope = parse_execution_attempt(&stale_finish).expect("parse");
        assert!(
            accept(&pool, community, &stale_finish, &stale_envelope)
                .await
                .is_err(),
            "a second/stale completion cannot mutate an ended generation"
        );

        let next = attempt_event(
            &target,
            job_id,
            &request,
            channel,
            Uuid::new_v4(),
            2,
            "runnable",
            None,
            None,
            None,
        );
        let completion = EventBuilder::new(Kind::Custom(KIND_JOB_COMPLETED as u16), "done")
            .tags([
                Tag::parse(["d", &job_id.to_string()]).expect("tag"),
                Tag::parse(["job-request", &request.id.to_hex()]).expect("tag"),
                Tag::parse(["job-parent", &acceptance.id.to_hex()]).expect("tag"),
                Tag::parse(["h", &channel.to_string()]).expect("tag"),
            ])
            .sign_with_keys(&target)
            .expect("completion");
        let next_envelope = parse_execution_attempt(&next).expect("parse");
        let completion_envelope = parse_job_lifecycle(&completion).expect("parse");
        let (next_result, terminal_result) = tokio::join!(
            accept(&pool, community, &next, &next_envelope),
            job::accept_lifecycle(&pool, community, &completion, &completion_envelope),
        );
        assert!(terminal_result.is_ok());
        if next_result.is_ok() {
            let status: String = sqlx::query_scalar(
                "SELECT status FROM job_execution_attempts WHERE community_id=$1 AND job_id=$2 AND generation=2",
            )
            .bind(community.as_uuid())
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("status");
            assert_eq!(status, "suppressed");
        }
        let runnable_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM job_execution_attempts WHERE community_id=$1 AND job_id=$2 AND status IN ('runnable','active')",
        )
        .bind(community.as_uuid())
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(runnable_count, 0);
        cleanup(&pool, community).await;
    }
}
