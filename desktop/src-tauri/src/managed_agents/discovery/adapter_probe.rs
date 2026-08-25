use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::managed_agents::AcpAvailabilityStatus;

use super::{probe_codex_acp_version, MIN_CODEX_ACP_VERSION};

type ProbeResult = Arc<OnceLock<AcpAvailabilityStatus>>;

/// Per-binary version-probe results for the current discovery generation.
///
/// Managed agents start concurrently, and each readiness check asks about the
/// same adapter. Without single-flight caching, a cold desktop launch can run
/// many Node-backed `codex-acp --version` processes at once. Those processes
/// contend with each other, exceed the bounded probe deadline, and make a
/// supported adapter look outdated. `OnceLock` lets one caller probe while
/// peers for the same resolved path wait for and reuse that exact result.
///
/// The cache is cleared with the command-resolution cache after discovery or
/// installation, so replacing the adapter causes the new binary to be probed.
fn cache() -> &'static Mutex<HashMap<PathBuf, ProbeResult>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, ProbeResult>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn clear_cache() {
    if let Ok(mut guard) = cache().lock() {
        guard.clear();
    }
}

/// Classifies a resolved codex-acp binary, sharing one bounded version probe
/// among concurrent readiness callers for the same path.
pub(crate) fn codex_adapter_availability(path: &Path) -> AcpAvailabilityStatus {
    let result = match cache().lock() {
        Ok(mut cache) => Arc::clone(
            cache
                .entry(path.to_path_buf())
                .or_insert_with(|| Arc::new(OnceLock::new())),
        ),
        // A poisoned optimization cache must not prevent readiness evaluation.
        Err(_) => return probe(path),
    };

    result.get_or_init(|| probe(path)).clone()
}

fn probe(path: &Path) -> AcpAvailabilityStatus {
    match probe_codex_acp_version(path) {
        Some(version) if version >= MIN_CODEX_ACP_VERSION => AcpAvailabilityStatus::Available,
        _ => AcpAvailabilityStatus::AdapterOutdated,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Barrier};

    use super::codex_adapter_availability;
    use crate::managed_agents::{clear_resolve_cache, AcpAvailabilityStatus};

    /// Concurrent managed-agent readiness checks for the same adapter must
    /// share one version probe. A cold-start herd can otherwise push every
    /// subprocess past the bounded deadline.
    #[test]
    fn coalesces_concurrent_probes_for_one_path() {
        let _guard = crate::managed_agents::lock_path_mutex();
        clear_resolve_cache();

        let dir = tempfile::tempdir().expect("temp dir");
        let bin = dir.path().join("codex-acp");
        std::fs::write(
            &bin,
            "#!/bin/sh\nprintf 'probe\\n' >> \"$(dirname \"$0\")/probe-count\"\nsleep 1\necho '@agentclientprotocol/codex-acp 1.1.7'\nexit 0\n",
        )
        .expect("write script");
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755))
            .expect("chmod script");

        let bin = Arc::new(bin);
        let barrier = Arc::new(Barrier::new(6));
        let handles: Vec<_> = (0..6)
            .map(|_| {
                let bin = Arc::clone(&bin);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    codex_adapter_availability(bin.as_path())
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(
                handle.join().expect("readiness thread"),
                AcpAvailabilityStatus::Available
            );
        }

        let probe_count = std::fs::read_to_string(dir.path().join("probe-count"))
            .expect("read probe count")
            .lines()
            .count();
        assert_eq!(
            probe_count, 1,
            "concurrent readiness checks must execute one adapter probe"
        );
    }

    /// A result for one resolved adapter path must never mask a different
    /// binary.
    #[test]
    fn keeps_distinct_paths_independent() {
        let _guard = crate::managed_agents::lock_path_mutex();
        clear_resolve_cache();

        let dir = tempfile::tempdir().expect("temp dir");
        let supported = dir.path().join("codex-acp-supported");
        let outdated = dir.path().join("codex-acp-outdated");
        std::fs::write(
            &supported,
            "#!/bin/sh\necho '@agentclientprotocol/codex-acp 1.1.7'\nexit 0\n",
        )
        .expect("write supported script");
        std::fs::write(
            &outdated,
            "#!/bin/sh\necho '@agentclientprotocol/codex-acp 1.1.5'\nexit 0\n",
        )
        .expect("write outdated script");
        std::fs::set_permissions(&supported, std::fs::Permissions::from_mode(0o755))
            .expect("chmod supported script");
        std::fs::set_permissions(&outdated, std::fs::Permissions::from_mode(0o755))
            .expect("chmod outdated script");

        assert_eq!(
            codex_adapter_availability(&supported),
            AcpAvailabilityStatus::Available
        );
        assert_eq!(
            codex_adapter_availability(&outdated),
            AcpAvailabilityStatus::AdapterOutdated
        );
    }
}
