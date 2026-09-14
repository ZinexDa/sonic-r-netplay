use std::path::{Path, PathBuf};
use tokio::process::{Child, Command};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutableSource {
    Bundled,
    EnvVar,
    GameDir,
    Workspace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedExecutable {
    pub path: PathBuf,
    pub source: ExecutableSource,
}

impl ResolvedExecutable {
    pub fn display_label(&self) -> String {
        match self.source {
            ExecutableSource::Bundled => "Using bundled sonicr.exe".to_string(),
            ExecutableSource::EnvVar => format!("Env: {}", self.path.display()),
            ExecutableSource::GameDir => "Game dir sonicr.exe".to_string(),
            ExecutableSource::Workspace => "Workspace dev build".to_string(),
        }
    }
}

/// Validates whether a directory is a valid Sonic R game data folder.
/// Checks for the existence of game asset directories or core files (e.g. `GENERAL`, `GENERAL/SONICR.BIT`).
pub fn validate_game_dir(dir: &Path) -> bool {
    if !dir.is_dir() {
        return false;
    }

    let has_general_dir = dir.join("GENERAL").is_dir() || dir.join("general").is_dir();
    let has_bit_file = dir.join("GENERAL").join("SONICR.BIT").exists()
        || dir.join("general").join("sonicr.bit").exists()
        || dir.join("GENERAL").join("sonicr.bit").exists()
        || dir.join("general").join("SONICR.BIT").exists();
    let has_island = dir.join("ISLAND").is_dir() || dir.join("island").is_dir();
    let has_music = dir.join("MUSIC").is_dir() || dir.join("music").is_dir();
    let has_exe = dir.join("sonicr.exe").is_file() || dir.join("SONICR.EXE").is_file();

    has_bit_file || has_general_dir || (has_exe && (has_island || has_music))
}

/// Resolves the Sonic R executable `sonicr.exe` according to priority:
/// 1. Priority 1 (Bundled): Check in the SAME directory as the running launcher executable.
/// 2. Priority 2 (Explicit env): Check `SONICR_PATH` environment variable if defined.
/// 3. Priority 3 (Inside Game Directory): Check if `sonicr.exe` exists inside user-selected `game_dir`.
/// 4. Priority 4 (Dev / Workspace fallback): Fallback to workspace build path (`sonic-r-main/source/sdl/build-release/sonicr.exe`).
pub fn resolve_sonicr_executable(game_dir: Option<&Path>) -> Option<ResolvedExecutable> {
    resolve_sonicr_executable_with_launcher_dir(game_dir, current_launcher_dir().as_deref())
}

/// Internal helper allowing test overrides of the launcher directory.
pub fn resolve_sonicr_executable_with_launcher_dir(
    game_dir: Option<&Path>,
    launcher_dir: Option<&Path>,
) -> Option<ResolvedExecutable> {
    // 1. Priority 1 (Bundled): Same directory as launcher executable
    if let Some(dir) = launcher_dir {
        let bundled = dir.join("sonicr.exe");
        if bundled.is_file() {
            return Some(ResolvedExecutable {
                path: bundled,
                source: ExecutableSource::Bundled,
            });
        }
        let bundled_upper = dir.join("SONICR.EXE");
        if bundled_upper.is_file() {
            return Some(ResolvedExecutable {
                path: bundled_upper,
                source: ExecutableSource::Bundled,
            });
        }
    }

    // 2. Priority 2 (Explicit env): SONICR_PATH
    if let Ok(path_str) = std::env::var("SONICR_PATH") {
        let p = PathBuf::from(path_str);
        if p.is_file() {
            return Some(ResolvedExecutable {
                path: p,
                source: ExecutableSource::EnvVar,
            });
        }
    }

    // 3. Priority 3 (Inside Game Directory): user-selected game_dir
    if let Some(dir) = game_dir {
        let in_dir = dir.join("sonicr.exe");
        if in_dir.is_file() {
            return Some(ResolvedExecutable {
                path: in_dir,
                source: ExecutableSource::GameDir,
            });
        }
        let in_dir_upper = dir.join("SONICR.EXE");
        if in_dir_upper.is_file() {
            return Some(ResolvedExecutable {
                path: in_dir_upper,
                source: ExecutableSource::GameDir,
            });
        }
    }

    // 4. Priority 4 (Dev / Workspace fallback)
    let candidates = [
        "sonic-r-main/source/sdl/build-release/sonicr.exe",
        "../sonic-r-main/source/sdl/build-release/sonicr.exe",
        "../../sonic-r-main/source/sdl/build-release/sonicr.exe",
        "sonicr.exe",
    ];

    for candidate in &candidates {
        let p = PathBuf::from(candidate);
        if p.is_file() {
            let resolved = std::fs::canonicalize(&p).unwrap_or(p);
            return Some(ResolvedExecutable {
                path: resolved,
                source: ExecutableSource::Workspace,
            });
        }
    }

    None
}

/// Helper to get the parent directory of current launcher executable.
fn current_launcher_dir() -> Option<PathBuf> {
    std::env::current_exe().ok().and_then(|p| p.parent().map(|d| d.to_path_buf()))
}

/// Spawns the Sonic R executable with netplay arguments:
/// - Host: `sonicr.exe --data <game_dir> --port <port> --autohost [--username <name>]`
/// - Join: `sonicr.exe --data <game_dir> --port <port> --host <host_ip> --autojoin [--username <name>]`
///
/// Sets the working directory to `game_dir` and passes `--data <game_dir>` so that
/// assets (GENERAL, ISLAND, MUSIC) resolve correctly regardless of executable location.
pub fn launch_game(
    exe_path: &Path,
    game_dir: &Path,
    is_host: bool,
    port: u16,
    host_ip: Option<&str>,
    player_name: Option<&str>,
) -> std::io::Result<Child> {
    let mut cmd = Command::new(exe_path);
    cmd.current_dir(game_dir);
    cmd.arg("--data").arg(game_dir);
    cmd.arg("--port").arg(port.to_string());

    if is_host {
        cmd.arg("--autohost");
    } else {
        let ip = host_ip.unwrap_or("127.0.0.1");
        cmd.arg("--host").arg(ip);
        cmd.arg("--autojoin");
    }

    if let Some(name) = player_name {
        let trimmed = name.trim();
        if !trimmed.is_empty() {
            cmd.arg("--username").arg(trimmed);
        }
    }

    tracing::info!(
        exe = %exe_path.display(),
        working_dir = %game_dir.display(),
        is_host,
        port,
        host_ip,
        player_name,
        "Launching Sonic R executable"
    );

    cmd.spawn()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_game_dir_empty() {
        let temp = std::env::temp_dir().join(format!("sonicr_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp).unwrap();
        assert!(!validate_game_dir(&temp));
        let _ = std::fs::remove_dir_all(&temp);
    }

    #[test]
    fn test_validate_game_dir_with_general() {
        let temp = std::env::temp_dir().join(format!("sonicr_test_{}", uuid::Uuid::new_v4()));
        let general = temp.join("GENERAL");
        std::fs::create_dir_all(&general).unwrap();
        std::fs::write(general.join("SONICR.BIT"), b"dummy").unwrap();

        assert!(validate_game_dir(&temp));
        let _ = std::fs::remove_dir_all(&temp);
    }

    #[test]
    fn test_priority_bundled_over_game_dir() {
        let base_temp = std::env::temp_dir().join(format!("sonicr_priority_test_{}", uuid::Uuid::new_v4()));
        let launcher_dir = base_temp.join("launcher_dir");
        let game_dir = base_temp.join("game_dir");

        std::fs::create_dir_all(&launcher_dir).unwrap();
        std::fs::create_dir_all(&game_dir).unwrap();

        let bundled_exe = launcher_dir.join("sonicr.exe");
        let game_dir_exe = game_dir.join("sonicr.exe");

        std::fs::write(&bundled_exe, b"bundled").unwrap();
        std::fs::write(&game_dir_exe, b"gamedir").unwrap();

        let res = resolve_sonicr_executable_with_launcher_dir(Some(&game_dir), Some(&launcher_dir)).unwrap();
        assert_eq!(res.source, ExecutableSource::Bundled);
        assert_eq!(res.path, bundled_exe);

        let _ = std::fs::remove_dir_all(&base_temp);
    }

    #[test]
    fn test_priority_game_dir_when_not_bundled() {
        let base_temp = std::env::temp_dir().join(format!("sonicr_gamedir_test_{}", uuid::Uuid::new_v4()));
        let launcher_dir = base_temp.join("launcher_dir_empty");
        let game_dir = base_temp.join("game_dir");

        std::fs::create_dir_all(&launcher_dir).unwrap();
        std::fs::create_dir_all(&game_dir).unwrap();

        let game_dir_exe = game_dir.join("sonicr.exe");
        std::fs::write(&game_dir_exe, b"gamedir").unwrap();

        let res = resolve_sonicr_executable_with_launcher_dir(Some(&game_dir), Some(&launcher_dir)).unwrap();
        assert_eq!(res.source, ExecutableSource::GameDir);
        assert_eq!(res.path, game_dir_exe);

        let _ = std::fs::remove_dir_all(&base_temp);
    }
}
