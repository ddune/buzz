//! Constrained producer for the delegated-job protocol.

use nostr::{EventBuilder, Kind, Tag};
use std::collections::HashMap;
use uuid::Uuid;

use crate::client::{normalize_write_response, BuzzClient};
use crate::error::CliError;
use crate::validate::{parse_uuid, validate_hex64};
use crate::{JobLifecycleArgs, JobsCmd};

use buzz_core::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_BLOCKED, KIND_JOB_COMPLETED, KIND_JOB_DELEGATED, KIND_JOB_REJECTED,
    KIND_JOB_REQUEST,
};

pub(crate) async fn dispatch(command: JobsCmd, client: &BuzzClient) -> Result<(), CliError> {
    match command {
        JobsCmd::Create {
            target,
            channel,
            assignment,
            job,
        } => create(client, &target, &channel, &assignment, job.as_deref()).await,
        JobsCmd::Accept(args) => lifecycle(client, KIND_JOB_ACCEPTED, &args, None).await,
        JobsCmd::Reject(args) => lifecycle(client, KIND_JOB_REJECTED, &args, None).await,
        JobsCmd::Complete(args) => lifecycle(client, KIND_JOB_COMPLETED, &args, None).await,
        JobsCmd::Blocked(args) => lifecycle(client, KIND_JOB_BLOCKED, &args, None).await,
        JobsCmd::Delegate { job, successor } => {
            validate_hex64(&successor)?;
            lifecycle(client, KIND_JOB_DELEGATED, &job, Some(&successor)).await
        }
        JobsCmd::Get { job } => get(client, &job).await,
        JobsCmd::List { target } => list_accepted(client, &target).await,
    }
}

async fn create(
    client: &BuzzClient,
    target: &str,
    channel: &str,
    assignment: &str,
    job: Option<&str>,
) -> Result<(), CliError> {
    validate_hex64(target)?;
    let channel = parse_uuid(channel)?;
    if assignment.trim().is_empty() {
        return Err(CliError::Usage("assignment must not be empty".into()));
    }
    let job_id = match job {
        Some(value) => parse_uuid(value)?,
        None => Uuid::new_v4(),
    };
    let mut tags = job_tags(job_id, channel, Some(("job-target", target)))?;
    tags.push(parse_tag(["p", target])?);
    let event = client.sign_event(
        EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), assignment).tags(tags),
    )?;
    let event_id = event.id.to_hex();
    let response = client.submit_event(event).await?;
    let normalized = normalize_write_response(&response);
    let mut value: serde_json::Value = serde_json::from_str(&normalized)
        .map_err(|error| CliError::Other(format!("invalid relay response: {error}")))?;
    value["job_id"] = serde_json::json!(job_id);
    value["event_id"] = serde_json::json!(event_id);
    println!("{value}");
    Ok(())
}

async fn lifecycle(
    client: &BuzzClient,
    kind: u32,
    args: &JobLifecycleArgs,
    successor: Option<&str>,
) -> Result<(), CliError> {
    let job_id = parse_uuid(&args.job)?;
    let channel = parse_uuid(&args.channel)?;
    validate_hex64(&args.request)?;
    let parent = if matches!(kind, KIND_JOB_ACCEPTED | KIND_JOB_REJECTED) {
        args.request.as_str()
    } else {
        args.parent
            .as_deref()
            .ok_or_else(|| CliError::Usage("--parent acceptance-event-id is required".into()))?
    };
    validate_hex64(parent)?;
    let mut tags = job_tags(
        job_id,
        channel,
        Some(("job-request", args.request.as_str())),
    )?;
    tags.push(parse_tag(["job-parent", parent])?);
    if let Some(successor) = successor {
        tags.push(parse_tag(["job-successor", successor])?);
    }
    let event = client
        .sign_event(EventBuilder::new(Kind::Custom(kind as u16), &args.content).tags(tags))?;
    let response = client.submit_event(event).await?;
    println!("{}", normalize_write_response(&response));
    Ok(())
}

async fn get(client: &BuzzClient, job: &str) -> Result<(), CliError> {
    let job_id = parse_uuid(job)?;
    let filter = serde_json::json!({
        "kinds": [KIND_JOB_REQUEST, KIND_JOB_ACCEPTED, KIND_JOB_REJECTED,
                  KIND_JOB_COMPLETED, KIND_JOB_BLOCKED, KIND_JOB_DELEGATED],
        "#job": [job_id.to_string()],
        "limit": 16
    });
    println!("{}", client.query(&filter).await?);
    Ok(())
}

async fn list_accepted(client: &BuzzClient, target: &str) -> Result<(), CliError> {
    validate_hex64(target)?;
    let request_filter = serde_json::json!({"kinds": [KIND_JOB_REQUEST], "#p": [target]});
    let mut jobs = HashMap::new();
    for value in client.query_all(request_filter).await? {
        let event: nostr::Event = serde_json::from_value(value)
            .map_err(|error| CliError::Other(format!("invalid job event: {error}")))?;
        if let Ok(request) = buzz_core::delegated_job::parse_job_request(&event) {
            if request.target_agent == target.to_ascii_lowercase() {
                jobs.insert(
                    request.job_id,
                    (request, buzz_core::delegated_job::JobState::Requested),
                );
            }
        }
    }
    if jobs.is_empty() {
        println!("[]");
        return Ok(());
    }
    let ids: Vec<String> = jobs.keys().map(Uuid::to_string).collect();
    let lifecycle_filter = serde_json::json!({
        "kinds": [KIND_JOB_ACCEPTED, KIND_JOB_REJECTED, KIND_JOB_COMPLETED,
                  KIND_JOB_BLOCKED, KIND_JOB_DELEGATED],
        "#job": ids,
    });
    let mut events: Vec<nostr::Event> = client
        .query_all(lifecycle_filter)
        .await?
        .into_iter()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|error| CliError::Other(format!("invalid job event: {error}")))
        })
        .collect::<Result<_, _>>()?;
    let mut heads: HashMap<Uuid, String> = jobs
        .iter()
        .map(|(id, (request, _))| (*id, request.request_event_id.clone()))
        .collect();
    loop {
        let mut advanced = false;
        events.retain(|event| {
            let Ok(lifecycle) = buzz_core::delegated_job::parse_job_lifecycle(event) else {
                return false;
            };
            let Some((request, state)) = jobs.get_mut(&lifecycle.job_id) else {
                return false;
            };
            if lifecycle.request_event_id == request.request_event_id
                && lifecycle.channel_id == request.channel_id
                && lifecycle.author == request.target_agent
                && heads.get(&lifecycle.job_id) == Some(&lifecycle.parent_event_id)
            {
                if let Ok(next) = state.apply(lifecycle.action) {
                    *state = next;
                    heads.insert(lifecycle.job_id, event.id.to_hex());
                    advanced = true;
                    return false;
                }
            }
            true
        });
        if !advanced {
            break;
        }
    }
    let mut accepted: Vec<serde_json::Value> = jobs
        .into_values()
        .filter_map(|(request, state)| {
            (state == buzz_core::delegated_job::JobState::Accepted).then(|| {
                serde_json::json!({
                    "job_id": request.job_id,
                    "request_event_id": request.request_event_id,
                    "channel_id": request.channel_id,
                    "requester": request.requester,
                    "target": request.target_agent,
                    "assignment": request.assignment,
                    "state": state.as_str(),
                })
            })
        })
        .collect();
    accepted.sort_by_key(|job| job["job_id"].as_str().unwrap_or_default().to_owned());
    println!(
        "{}",
        serde_json::to_string(&accepted)
            .map_err(|error| CliError::Other(format!("output serialization failed: {error}")))?
    );
    Ok(())
}

fn job_tags(
    job_id: Uuid,
    channel: Uuid,
    extra: Option<(&str, &str)>,
) -> Result<Vec<Tag>, CliError> {
    let mut tags = vec![
        parse_tag(["job", &job_id.to_string()])?,
        parse_tag(["h", &channel.to_string()])?,
    ];
    if let Some((name, value)) = extra {
        tags.push(parse_tag([name, value])?);
    }
    Ok(tags)
}

fn parse_tag<const N: usize>(parts: [&str; N]) -> Result<Tag, CliError> {
    Tag::parse(parts).map_err(|error| CliError::Other(format!("tag error: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_builder_has_one_job_target_and_channel() {
        let target = "ab".repeat(32);
        let mut tags =
            job_tags(Uuid::nil(), Uuid::nil(), Some(("job-target", &target))).expect("tags");
        tags.push(parse_tag(["p", &target]).expect("routing tag"));
        for name in ["job", "h", "job-target", "p"] {
            assert_eq!(
                tags.iter().filter(|tag| tag.as_slice()[0] == name).count(),
                1
            );
        }
    }
}
