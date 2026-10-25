use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aozumi_db::{mint_token, BackupConfig, BackupTarget, Claims, Config};

const USAGE: &str = "usage:
  aozumi-db                         serve (configured by AOZUMI_DB_* env vars)
  aozumi-db token <database> [ttl]  print a token for <database>, valid for ttl seconds";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let secret = std::env::var("AOZUMI_DB_SECRET").ok().filter(|s| !s.is_empty()).map(String::into_bytes);

    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        [] => serve(secret).await,
        ["token", db, rest @ ..] if rest.len() <= 1 => {
            let secret = secret.ok_or_else(|| anyhow::anyhow!("set AOZUMI_DB_SECRET to mint tokens"))?;
            let exp = match rest.first() {
                Some(ttl) => Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + ttl.parse::<u64>()?),
                None => None,
            };
            println!("{}", mint_token(&secret, &Claims { db: db.to_string(), exp, read_only: false }));
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
        eprintln!("aozumi-db: AOZUMI_DB_SECRET is unset, running in dev mode: no auth, one database named `default`");
    }

    let mut config = Config::new(data_dir.clone(), secret);
    config.backup = backup_config()?;
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
/// 0 = only on request) and `AOZUMI_DB_BACKUP_KEEP` tune them.
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
    eprintln!("aozumi-db: backing up to {url}, every {every}s when changed, keeping {keep} per database");
    Ok(Some(BackupConfig {
        target: BackupTarget::parse(&url)?,
        keep: keep.max(1) as usize,
        every: (every > 0).then(|| Duration::from_secs(every)),
    }))
}
