#![cfg_attr(not(windows), forbid(unsafe_code))]
#![cfg_attr(windows, deny(unsafe_code))]
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::stdio,
    ErrorData, ServerHandler, ServiceExt,
};
use std::path::Path;
use std::sync::Arc;

mod paths;
mod read_file;
mod rg;
mod shell;
mod shim;
mod str_replace;
mod todo;
mod tree;
mod view_image;

#[derive(Clone)]
struct DevMcp {
    state: Arc<shell::SharedState>,
    todos: Arc<todo::TodoState>,
    tool_router: ToolRouter<DevMcp>,
}

#[tool_router]
impl DevMcp {
    fn new(state: Arc<shell::SharedState>) -> Self {
        Self {
            state,
            todos: Arc::new(todo::TodoState::new()),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "shell",
        description = "Run a shell command (bash by default; set `BUZZ_SHELL` to use cmd, PowerShell, or another shell). Ephemeral process per call. Output tail-truncated to ~8KB for the LLM; full output (first 10MB) saved to artifact file. timeout_ms defaults to 120000 (2 min) if omitted; capped at 600000 (10 min). For long-running commands (git push with hooks, cargo build, test suites), use 300000+. On PATH: rg (prefer over grep; flags: -n -i -l -g <glob> -C <n> --files), tree (flags: -d <depth>; shows line counts), and buzz (Buzz relay CLI — run buzz --help for commands)."
    )]
    async fn shell(
        &self,
        Parameters(p): Parameters<shell::ShellParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        if job_followup_readonly() && !followup_shell_command_allowed(&p.command) {
            return Ok(CallToolResult::error(vec![rmcp::model::Content::text(
                "Shell and process execution are disabled while an accepted job owns the workspace; the durable claimed continuation is the sole mutation-capable authority.",
            )]));
        }
        if evaluation_only() && !evaluation_shell_command_allowed(&p.command) {
            return Ok(CallToolResult::error(vec![rmcp::model::Content::text(
                "Delegated-job evaluation permits only `buzz jobs accept` or `buzz jobs reject`; executable repository work begins in the claimed continuation.",
            )]));
        }
        shell::run(&self.state, p, context.ct).await
    }

    #[tool(
        name = "read_file",
        description = "Read a text file and return its contents with line numbers. Returns lines in `{number}:{content}` format. Use `offset` (0-based) and `limit` (default 2000) to window into large files. Path resolved relative to workdir (defaults to server cwd). Prefer over cat/head/tail."
    )]
    async fn read_file(
        &self,
        Parameters(p): Parameters<read_file::ReadFileParams>,
    ) -> Result<String, ErrorData> {
        read_file::run(&self.state, p)
    }

    #[tool(
        name = "view_image",
        description = "Load an image from a file path, http(s) URL, or data: URL and return it as an MCP image content block that multimodal LLMs (Anthropic, OpenAI-compatible, etc.) can see. Resizes to a longest-edge of 1568px by default (override with `max_dim`, range 64..=2048). Pass-through for already-small PNG/JPEG; transcodes oversize input to PNG (if alpha) or JPEG q85. Animated GIF/WebP rejected — provide a still frame. Hard cap 20 MiB source, ~4 MiB on the wire. Relative paths resolve under `workdir` (defaults to server cwd) and may not escape it."
    )]
    async fn view_image(
        &self,
        Parameters(p): Parameters<view_image::ViewImageParams>,
    ) -> Result<CallToolResult, ErrorData> {
        view_image::run(&self.state, p).await
    }

    #[tool(
        name = "str_replace",
        description = "Atomic find-and-replace in a file. old_str must occur exactly once unless replace_all is true, in which case all occurrences are replaced. Returns a unified diff. Path resolved relative to workdir (defaults to server cwd). Prefer over sed/awk."
    )]
    async fn str_replace(
        &self,
        Parameters(p): Parameters<str_replace::StrReplaceParams>,
    ) -> Result<String, ErrorData> {
        if !file_mutation_allowed(evaluation_only(), job_followup_readonly()) {
            return Err(ErrorData::invalid_request(
                "file mutation is disabled in this restricted delegated-job session",
                None,
            ));
        }
        str_replace::run(&self.state, p)
    }

    #[tool(
        name = "todo",
        description = "Session checklist only for work that must continue across turns or survive context compaction. Do not use for work you can finish in the current turn. Omit `todos` to read; provide the full {text, done} list to replace it. Open items let the _Stop hook advise against ending."
    )]
    async fn todo(
        &self,
        Parameters(p): Parameters<todo::TodoParams>,
    ) -> Result<CallToolResult, ErrorData> {
        match self.todos.handle_todo(p) {
            Ok(text) => todo::text_result(text),
            Err(e) => todo::error_result(format!("Error: {e}")),
        }
    }

    /// Hook: called by the agent before honoring end_turn. Returns
    /// non-empty objection text iff items remain open.
    #[tool(
        name = "_Stop",
        description = "Returns open todo items if any exist. Used by the agent's _Stop lifecycle hook to advise against ending with incomplete work."
    )]
    async fn stop_hook(
        &self,
        Parameters(_): Parameters<todo::HookParams>,
    ) -> Result<CallToolResult, ErrorData> {
        todo::text_result(self.todos.stop_objection())
    }

    /// Hook: called by the agent after context compaction/handoff so the
    /// todo list survives history truncation.
    #[tool(
        name = "_PostCompact",
        description = "Internal hook. Agent invokes after handoff; returns todo state for re-injection."
    )]
    async fn post_compact_hook(
        &self,
        Parameters(_): Parameters<todo::HookParams>,
    ) -> Result<CallToolResult, ErrorData> {
        todo::text_result(self.todos.post_compact())
    }
}

fn evaluation_only() -> bool {
    std::env::var_os("BUZZ_JOB_EVALUATION_ONLY").is_some()
}

fn job_followup_readonly() -> bool {
    std::env::var_os("BUZZ_JOB_FOLLOWUP_READONLY").is_some()
}

fn followup_shell_command_allowed(command: &str) -> bool {
    followup_shell_command_allowed_for(command)
}

fn followup_shell_command_allowed_for(command: &str) -> bool {
    let Some(words) = restricted_shell_words(command) else {
        return false;
    };
    if words
        .get(..3)
        .map(|words| words.iter().map(String::as_str).collect::<Vec<_>>())
        != Some(vec!["buzz", "messages", "send"])
    {
        return false;
    }
    if words.len() != 9 {
        return false;
    }
    let mut options = std::collections::HashMap::<&str, &str>::new();
    for pair in words[3..].chunks_exact(2) {
        if !matches!(pair[0].as_str(), "--channel" | "--content" | "--reply-to")
            || options.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return false;
        }
    }
    options
        .get("--channel")
        .is_some_and(|channel| uuid::Uuid::parse_str(channel).is_ok())
        && options
            .get("--content")
            .is_some_and(|content| !content.is_empty() && *content != "-")
        && options.get("--reply-to").is_some_and(|event_id| {
            event_id.len() == 64 && event_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

/// Parse a single shell command while rejecting operators and expansion. Shell
/// metacharacters are harmless inside single quotes, which lets ordinary
/// Markdown status text contain backticks without opening command execution.
fn restricted_shell_words(command: &str) -> Option<Vec<String>> {
    #[derive(Clone, Copy)]
    enum Quote {
        None,
        Single,
        Double,
    }
    let mut quote = Quote::None;
    let mut escaped = false;
    let mut word = String::new();
    let mut words = Vec::new();
    for ch in command.chars() {
        if matches!(ch, '\n' | '\r') {
            return None;
        }
        if escaped {
            word.push(ch);
            escaped = false;
            continue;
        }
        match quote {
            Quote::Single => {
                if ch == '\'' {
                    quote = Quote::None;
                } else {
                    word.push(ch);
                }
            }
            Quote::Double => match ch {
                '"' => quote = Quote::None,
                '\\' => escaped = true,
                '$' | '`' => return None,
                _ => word.push(ch),
            },
            Quote::None => match ch {
                '\'' => quote = Quote::Single,
                '"' => quote = Quote::Double,
                '\\' => escaped = true,
                // Reject every unquoted shell expansion/operator character,
                // not just command separators. The validated argv is later
                // executed by bash, so brace/glob/tilde/history expansion
                // would otherwise be able to manufacture extra CLI options
                // after this check (for example `{ok,--file=/etc/passwd}`).
                ';' | '|' | '&' | '>' | '<' | '`' | '$' | '#' | '{' | '}' | '*' | '?' | '['
                | ']' | '~' | '(' | ')' | '!' => return None,
                ch if ch.is_whitespace() => {
                    if !word.is_empty() {
                        words.push(std::mem::take(&mut word));
                    }
                }
                _ => word.push(ch),
            },
        }
    }
    if escaped || !matches!(quote, Quote::None) {
        return None;
    }
    if !word.is_empty() {
        words.push(word);
    }
    Some(words)
}

fn file_mutation_allowed(evaluation: bool, followup_readonly: bool) -> bool {
    !evaluation && !followup_readonly
}

fn evaluation_shell_command_allowed(command: &str) -> bool {
    if command
        .chars()
        .any(|ch| matches!(ch, ';' | '|' | '&' | '>' | '<' | '`' | '$' | '\n' | '\r'))
    {
        return false;
    }
    let mut words = command.split_whitespace();
    words.next() == Some("buzz")
        && words.next() == Some("jobs")
        && matches!(words.next(), Some("accept" | "reject"))
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for DevMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(rmcp::model::Implementation::new(
                "buzz-dev-mcp",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(self.state.bootstrap_instructions.clone())
    }
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let argv0 = std::env::args().next().unwrap_or_default();
    let cmd = Path::new(&argv0)
        .file_stem()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    // Multicall dispatch — sync personalities exit before any runtime is built.
    // No tracing, no tokio, no allocations beyond argv parsing.
    match cmd.as_str() {
        "rg" => std::process::exit(rg::run(std::env::args().skip(1).collect())),
        "tree" => std::process::exit(tree::run(std::env::args().skip(1).collect())),
        "git-credential-nostr" => std::process::exit(git_credential_nostr::run()),
        "git-sign-nostr" => std::process::exit(git_sign_nostr::run()),
        _ => {}
    }

    // Async personalities and MCP server mode — build the runtime.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async_main(cmd))
}

async fn async_main(cmd: String) -> Result<(), Box<dyn std::error::Error>> {
    // HTTPS clients invoked through this MCP process need a Rustls provider;
    // repeated installation is harmless.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // buzz CLI needs tokio (async HTTP client).
    if cmd == "buzz" {
        std::process::exit(buzz_cli::run_from_args(std::env::args()).await);
    }

    // MCP server mode — safe to init tracing now.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let cwd = std::env::current_dir()?;
    let shim = shim::Shim::install()?;
    let state = Arc::new(shell::SharedState::new(cwd, shim)?);

    let service = DevMcp::new(state).serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

/// Suppress the console window that Windows otherwise allocates for every
/// console-subsystem child process spawned from a non-console parent.
/// No-op on non-Windows platforms.
pub(crate) fn configure_no_window(cmd: &mut std::process::Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    let _ = cmd;
}

/// Suppress the console window for async (`tokio::process::Command`) spawns.
/// Equivalent to `configure_no_window` but accepts a tokio command.
/// No-op on non-Windows platforms.
pub(crate) fn configure_no_window_async(cmd: &mut tokio::process::Command) {
    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    let _ = cmd;
}

#[cfg(test)]
mod evaluation_tests {
    use super::{
        evaluation_shell_command_allowed, file_mutation_allowed, followup_shell_command_allowed,
        followup_shell_command_allowed_for,
    };

    #[test]
    fn evaluation_shell_allows_only_constrained_job_decisions() {
        assert!(evaluation_shell_command_allowed(
            "buzz jobs accept --job 00000000-0000-0000-0000-000000000000 --request aa --channel 00000000-0000-0000-0000-000000000000"
        ));
        assert!(evaluation_shell_command_allowed(
            "buzz jobs reject --job id --request event --channel channel --content 'out of scope'"
        ));
        for command in [
            "git status",
            "bash -lc 'touch marker'",
            "python -c 'open(\"marker\", \"w\").close()'",
            "buzz jobs complete --job id",
            "buzz jobs blocked --job id",
            "buzz jobs delegate --job id",
            "buzz jobs accept --job id; touch marker",
            "buzz jobs accept --job id && cargo test",
            "buzz jobs accept --content $(touch marker)",
            "buzz jobs reject --content `touch marker`",
        ] {
            assert!(
                !evaluation_shell_command_allowed(command),
                "unexpected evaluation command admission: {command}"
            );
        }
    }

    #[test]
    fn accepted_job_followup_denies_every_shell_mutation_path() {
        // The follow-up handler rejects shell before parsing the command. Pin
        // representative file, Git, process, lifecycle, and MCP escape paths.
        for command in [
            "touch marker-b",
            "git add .",
            "git commit -m continued",
            "cargo test",
            "python -c 'open(\"marker-b\", \"w\").close()'",
            "buzz jobs complete --job id",
            "buzz jobs blocked --job id",
            "buzz jobs delegate --job id",
        ] {
            assert!(!followup_shell_command_allowed(command));
        }
        assert!(!file_mutation_allowed(false, true));
        assert!(!file_mutation_allowed(true, false));
        assert!(file_mutation_allowed(false, false));
    }

    #[test]
    fn accepted_job_followup_allows_only_the_threaded_reply_command_shape() {
        let channel = "00000000-0000-0000-0000-000000000042";
        let event = "ab".repeat(32);
        assert!(followup_shell_command_allowed_for(
            &format!(
                "buzz messages send --channel {channel} --content 'Work on `marker-a.txt` is checkpointed.' --reply-to {event}"
            )
        ));
        for command in [
            format!("buzz messages send --channel {channel} --content - --reply-to {event}"),
            format!("buzz messages send --channel {channel} --content ok"),
            format!("buzz messages send --channel {channel} --content ok --reply-to {event} --file secret"),
            format!("buzz messages send --channel {channel} --content ok --reply-to {event} --file=secret"),
            format!("buzz messages send --channel {channel} --content ok --reply-to {event} --kind=9"),
            format!("buzz messages send --channel {channel} --content ok --reply-to not-an-event"),
            format!("buzz messages send --channel {channel} --content ok # --reply-to {event}"),
            format!("buzz messages send --channel {channel} --content {{ok,--file=/etc/hostname}} --reply-to {event}"),
            format!("buzz messages send --channel {channel} --content * --reply-to {event}"),
            format!("buzz messages send --channel {channel} --content ok --reply-to {event} --reply-to {event}"),
            format!("buzz messages send --channel not-a-uuid --content ok --reply-to {event}"),
            format!("buzz messages send --channel {channel} --content 'ok'; touch marker --reply-to {event}"),
            "buzz jobs complete --job id".into(),
        ] {
            assert!(
                !followup_shell_command_allowed_for(&command),
                "admitted: {command}"
            );
        }
    }
}
