use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aozumi_db::{mint_token, token_generation, BackupConfig, BackupTarget, Claims, Config, Limits, Synchronous};

const USAGE: &str = "usage:
  aozumi-db                         serve (configured by AOZUMI_DB_* env vars)
  aozumi-db token <database> [ttl] [--read-only]
                                    print a token for <database>, valid for ttl seconds,
                                    at the token generation kept in AOZUMI_DB_DATA_DIR";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let read_only = args.iter().any(|a| a == "--read-only");
    args.retain(|a| a != "--read-only");
    let secret = match std::env::var("AOZUMI_DB_SECRET") {
        Ok(secret) => (!secret.is_empty()).then(|| secret.into_bytes()),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("AOZUMI_DB_SECRET must be UTF-8"),
    };

    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        [] => serve(secret).await,
        ["token", db, rest @ ..] if rest.len() <= 1 => {
            let secret = secret.ok_or_else(|| anyhow::anyhow!("set AOZUMI_DB_SECRET to mint tokens"))?;
            let exp = match rest.first() {
                Some(ttl) => Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + ttl.parse::<u64>()?),
                None => None,
            };
            // The server's own record of the generation, so the token is not one it already revoked.
            let data_dir = PathBuf::from(std::env::var("AOZUMI_DB_DATA_DIR").unwrap_or_else(|_| "./data".into()));
            let generation = token_generation(&data_dir, db)?;
            println!("{}", mint_token(&secret, &Claims { db: db.to_string(), generation, exp, read_only }));
            Ok(())
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}

async fn serve(secret: Option<Vec<u8>>) -> anyhow::Result<()> {
    let listen = std::env::var("AOZUMI_DB_LISTEN").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let data_dir = PathBuf::from(std::env::var("AOZUMI_DB_DATA_DIR").unwrap_or_else(|_| "./data".into()));
    if secret.is_none() {
        // Without a secret nothing is checked, so a secret that never reached
        // the process must stop the server, not open every database.
        if std::env::var("AOZUMI_DB_DEV").as_deref() != Ok("1") {
            anyhow::bail!(
                "AOZUMI_DB_SECRET is unset or empty. Set it (for example to `openssl rand -hex 32`), \
                 or set AOZUMI_DB_DEV=1 for dev mode: no auth, one database named `default`"
            );
        }
        eprintln!("aozumi-db: AOZUMI_DB_DEV=1 and no AOZUMI_DB_SECRET, running in dev mode: no auth, one database named `default`");
    }

    let mut config = Config::new(data_dir.clone(), secret);
    config.backup = backup_config()?;
    config.default_limits = default_limits()?;
    config.synchronous = match std::env::var("AOZUMI_DB_SYNCHRONOUS").as_deref() {
        Err(_) | Ok("full") => Synchronous::Full,
        Ok("normal") => {
            eprintln!("aozumi-db: synchronous = normal: commits reach the disk at checkpoints; a power loss can lose the last few");
            Synchronous::Normal
        }
        Ok(other) => anyhow::bail!("AOZUMI_DB_SYNCHRONOUS must be full or normal, got {other:?}"),
    };
    if config.backup.is_none() {
        eprintln!("aozumi-db: AOZUMI_DB_BACKUP_URL is unset, so nothing is backed up");
    }

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    eprintln!("aozumi-db: listening on http://{} (data in {})", listener.local_addr()?, data_dir.display());
    tokio::select! {
        result = aozumi_db::serve(listener, config) => result,
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

/// `AOZUMI_DB_BACKUP_URL` turns backups on; `AOZUMI_DB_BACKUP_EVERY` (seconds,
/// 0 = only on request), `AOZUMI_DB_BACKUP_KEEP` and `AOZUMI_DB_BACKUP_WAL_MS`
/// (point-in-time restore; 0 = off) tune them.
fn backup_config() -> anyhow::Result<Option<BackupConfig>> {
    let Some(url) = std::env::var("AOZUMI_DB_BACKUP_URL").ok().filter(|u| !u.is_empty()) else {
        return Ok(None);
    };
    let number = |name: &str, default: u64| -> anyhow::Result<u64> {
        match std::env::var(name) {
            Ok(value) => value.parse().map_err(|_| anyhow::anyhow!("{name} must be a whole number, got {value:?}")),
            Err(_) => Ok(default),
        }
    };
    let every = number("AOZUMI_DB_BACKUP_EVERY", 300)?;
    let keep = number("AOZUMI_DB_BACKUP_KEEP", 24)?;
    let wal_ms = number("AOZUMI_DB_BACKUP_WAL_MS", 1000)?;
    eprintln!("aozumi-db: backing up to {url}, every {every}s when changed, keeping {keep} per database");
    if wal_ms > 0 {
        eprintln!("aozumi-db: shipping the WAL every {wal_ms}ms for point-in-time restore");
    }
    Ok(Some(BackupConfig {
        target: BackupTarget::parse(&url)?,
        keep: keep.max(1) as usize,
        every: (every > 0).then(|| Duration::from_secs(every)),
        wal_every: (wal_ms > 0).then(|| Duration::from_millis(wal_ms)),
    }))
}

/// Limits for new databases. A 10 s statement limit by default, so one
/// runaway query cannot hold a thread forever; `0` lifts a limit.
fn default_limits() -> anyhow::Result<Limits> {
    let limit = |name: &str, default: Option<u64>| -> anyhow::Result<Option<u64>> {
        match std::env::var(name) {
            Ok(value) => match value.parse::<u64>() {
                Ok(0) => Ok(None),
                Ok(n) => Ok(Some(n)),
                Err(_) => anyhow::bail!("{name} must be a whole number, got {value:?}"),
            },
            Err(_) => Ok(default),
        }
    };
    Ok(Limits {
        max_size_bytes: limit("AOZUMI_DB_MAX_SIZE_BYTES", None)?,
        max_statement_ms: limit("AOZUMI_DB_MAX_STATEMENT_MS", Some(10_000))?,
        max_requests_per_second: limit("AOZUMI_DB_MAX_REQUESTS_PER_SECOND", None)?.map(|n| n as f64),
    })
}
