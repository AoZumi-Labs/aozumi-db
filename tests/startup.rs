//! Starting the server binary: without a secret it runs only when dev mode
//! is asked for, so a secret that never reached the process cannot leave
//! every database open.

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(10);

/// The server binary with only `env` set, listening on a free port.
fn server(env: &[(&str, &str)], data: &tempfile::TempDir) -> Child {
    Command::new(env!("CARGO_BIN_EXE_aozumi-db"))
        .env_clear()
        .env("AOZUMI_DB_LISTEN", "127.0.0.1:0")
        .env("AOZUMI_DB_DATA_DIR", data.path())
        .envs(env.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Starts the server and returns what it printed once it exits. Fails if it
/// is still running after a few seconds.
fn refused(env: &[(&str, &str)]) -> String {
    let data = tempfile::tempdir().unwrap();
    let mut child = server(env, &data);
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > WAIT {
            stop(&mut child);
            panic!("the server started with {env:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = String::new();
    child.stderr.as_mut().unwrap().read_to_string(&mut stderr).unwrap();
    assert!(!status.success(), "{stderr}");
    stderr
}

/// Starts the server, waits until it is listening, stops it and returns what
/// it printed by then.
fn listening(env: &[(&str, &str)]) -> String {
    let data = tempfile::tempdir().unwrap();
    let mut child = server(env, &data);
    let (lines, printed) = mpsc::channel();
    let stderr = BufReader::new(child.stderr.take().unwrap());
    std::thread::spawn(move || stderr.lines().map_while(Result::ok).for_each(|line| drop(lines.send(line))));
    let mut seen = Vec::new();
    while let Ok(line) = printed.recv_timeout(WAIT) {
        seen.push(line);
        if seen.last().is_some_and(|line| line.contains("listening on")) {
            stop(&mut child);
            return seen.join("\n");
        }
    }
    stop(&mut child);
    panic!("the server did not start with {env:?}:\n{}", seen.join("\n"));
}

fn stop(child: &mut Child) {
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn refuses_to_start_without_a_secret() {
    let printed = refused(&[]);
    assert!(printed.contains("AOZUMI_DB_SECRET") && printed.contains("AOZUMI_DB_DEV=1"), "{printed}");
}

#[test]
fn an_empty_secret_is_no_secret() {
    let printed = refused(&[("AOZUMI_DB_SECRET", "")]);
    assert!(printed.contains("AOZUMI_DB_SECRET"), "{printed}");
}

#[test]
fn dev_mode_runs_only_when_asked_for() {
    let printed = listening(&[("AOZUMI_DB_DEV", "1")]);
    assert!(printed.contains("dev mode"), "{printed}");
    refused(&[("AOZUMI_DB_DEV", "yes")]);
}

#[test]
fn a_secret_starts_the_server_with_auth() {
    let printed = listening(&[("AOZUMI_DB_SECRET", "startup-test-secret")]);
    assert!(!printed.contains("dev mode"), "{printed}");
}

/// Runs a one-off command of the binary and returns its exit code, stdout and stderr.
fn command(args: &[&str]) -> (i32, String, String) {
    let data = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_aozumi-db"))
        .env_clear()
        .env("AOZUMI_DB_SECRET", "startup-test-secret")
        .env("AOZUMI_DB_DATA_DIR", data.path())
        .args(args)
        .output()
        .unwrap();
    (out.status.code().unwrap_or(-1), String::from_utf8_lossy(&out.stdout).into(), String::from_utf8_lossy(&out.stderr).into())
}

#[test]
fn help_prints_the_usage_and_succeeds() {
    for flag in ["--help", "-h", "help"] {
        let (code, stdout, _) = command(&[flag]);
        assert_eq!(code, 0, "{flag}");
        assert!(stdout.contains("aozumi-db token <database>"), "{flag}: {stdout}");
    }
    assert_eq!(command(&["nonsense"]).0, 2);
}

#[test]
fn a_bad_ttl_is_explained_and_a_huge_one_does_not_overflow() {
    let (code, _, stderr) = command(&["token", "notes", "1h"]);
    assert_ne!(code, 0);
    assert!(stderr.contains("ttl must be a whole number of seconds") && stderr.contains("\"1h\""), "{stderr}");
    let (code, stdout, stderr) = command(&["token", "notes", &u64::MAX.to_string()]);
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.starts_with("adb1."), "{stdout}");
}
