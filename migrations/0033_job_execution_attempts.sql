-- Runtime attempts are subordinate metadata for accepted delegated jobs.
CREATE TABLE job_execution_attempts (
    community_id UUID NOT NULL,
    job_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    attempt_id UUID NOT NULL,
    request_event_id BYTEA NOT NULL CHECK (octet_length(request_event_id) = 32),
    target_agent BYTEA NOT NULL CHECK (octet_length(target_agent) = 32),
    channel_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('runnable','active','ended','suppressed')),
    runnable_event_id BYTEA NOT NULL CHECK (octet_length(runnable_event_id) = 32),
    claim_event_id BYTEA CHECK (claim_event_id IS NULL OR octet_length(claim_event_id) = 32),
    outcome_event_id BYTEA CHECK (outcome_event_id IS NULL OR octet_length(outcome_event_id) = 32),
    turn_id TEXT,
    session_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    runtime_outcome TEXT,
    outcome_detail TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, job_id, generation),
    UNIQUE (community_id, attempt_id),
    FOREIGN KEY (community_id, job_id)
        REFERENCES delegated_jobs (community_id, job_id) ON DELETE CASCADE,
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_job_execution_attempts_target_status
    ON job_execution_attempts (community_id, target_agent, status, updated_at);

SELECT attach_community_write_fence('job_execution_attempts');
