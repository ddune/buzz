-- Backfill the #p discovery index for delegated-job requests written before
-- transactional mention indexing was attached to the delegated-job event path.
-- Reconciliation queries requests by target through event_mentions, so a
-- missing row makes an accepted job impossible to reconstruct from relay
-- history even though its request and lifecycle rows are durable.
INSERT INTO event_mentions
    (community_id, pubkey_hex, event_id, event_created_at, channel_id, event_kind)
SELECT
    e.community_id,
    lower(tag.value ->> 1),
    e.id,
    e.created_at,
    e.channel_id,
    e.kind
FROM events AS e
INNER JOIN communities AS c ON c.id = e.community_id
CROSS JOIN LATERAL jsonb_array_elements(e.tags) AS tag(value)
WHERE e.deleted_at IS NULL
  -- Community deletion fences reject new scoped writes by design. Historical
  -- obligations in a non-active community are not runnable, so do not bypass
  -- that fence merely to rebuild a discovery index.
  AND c.deletion_state = 'active'
  AND e.kind = 43001
  AND jsonb_typeof(tag.value) = 'array'
  AND jsonb_array_length(tag.value) >= 2
  AND tag.value ->> 0 = 'p'
  AND tag.value ->> 1 ~ '^[0-9A-Fa-f]{64}$'
ON CONFLICT DO NOTHING;
