-- Materialized current state for the append-only delegated-job event chain.
CREATE TABLE delegated_jobs (
    community_id UUID NOT NULL REFERENCES communities(id) ON DELETE CASCADE,
    job_id UUID NOT NULL,
    request_event_id BYTEA NOT NULL,
    requester BYTEA NOT NULL,
    target_agent BYTEA NOT NULL,
    channel_id UUID NOT NULL,
    assignment_hash BYTEA NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'requested', 'accepted', 'rejected', 'completed', 'blocked', 'delegated/transferred'
    )),
    acceptance_event_id BYTEA,
    terminal_event_id BYTEA,
    successor_agent BYTEA,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, job_id),
    UNIQUE (community_id, request_event_id),
    CHECK (octet_length(request_event_id) = 32),
    CHECK (octet_length(requester) = 32),
    CHECK (octet_length(target_agent) = 32),
    CHECK (octet_length(assignment_hash) = 32),
    CHECK (acceptance_event_id IS NULL OR octet_length(acceptance_event_id) = 32),
    CHECK (terminal_event_id IS NULL OR octet_length(terminal_event_id) = 32),
    CHECK (successor_agent IS NULL OR octet_length(successor_agent) = 32),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_delegated_jobs_target_state
    ON delegated_jobs (community_id, target_agent, state, created_at DESC);

SELECT attach_community_write_fence('delegated_jobs');
