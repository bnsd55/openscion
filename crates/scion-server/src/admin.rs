//! The admin commands that make a repo and a ref without a Git import:
//! the same one-way, safe-to-repeat spirit as the import, through the
//! store's own transactions, with the audit's evidence before the change.
//! Both may run while the server runs: every write is one transaction the
//! server's own writes order on.

use crate::Backend;
use crate::error::{Error, Result};
use crate::meta::{Meta as _, MetaTx as _};
use crate::tree;
use crate::{Audit, Event, names};
use scion_core::{Changeset, ChangesetId, IdentityTime};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr as _;

/// The identity a made repo's root changeset carries: the command, the
/// machine, the moment, at UTC. Nothing here is a person.
fn root_identity(now_secs: i64) -> Result<IdentityTime> {
    IdentityTime::new(
        b"scion-server".to_vec(),
        b"scion-server@localhost".to_vec(),
        now_secs,
        0,
    )
    .map_err(|e| Error::Internal(format!("the root changeset's identity does not fit: {e}")))
}

/// The root changeset of a made repo: no parents, no changes — `apply` on
/// the empty base stores the empty tree, as the import's first commit
/// would, minus its files.
fn root_changeset(name: &str, now_secs: i64) -> Result<Changeset> {
    Changeset::new(
        Vec::new(),
        root_identity(now_secs)?,
        None,
        format!("create repo {name}").into_bytes(),
        BTreeMap::new(),
    )
    .map_err(|e| Error::Internal(format!("the root changeset does not fit the format: {e}")))
}

/// Parses the hex form of a changeset ID: 64 hex digits.
fn parse_changeset(text: &str) -> Result<ChangesetId> {
    ChangesetId::from_str(text)
        .map_err(|_| Error::InvalidArgument(format!("{text:?} is not a changeset ID")))
}

/// Creates `name` with one root changeset (no parents, the empty tree) and
/// the trunk ref at it, so a client can land on the ref at once. Refuses a
/// repo that already exists: a second root would be an orphan history.
///
/// The audit carries one intent line before the change and one result line
/// after it, as the import's does.
///
/// # Errors
///
/// [`Error::InvalidArgument`] for a name that breaks the name rule,
/// [`Error::FailedPrecondition`] for a repo that exists or grew a changeset
/// between the check and the write, and the store's own errors otherwise.
pub async fn create_repo(audit: Audit, store: Backend, name: String, trunk: String) -> Result<()> {
    // The attempt is evidence before anything else: a refused run owes
    // its intent the one failed line, as the import's does.
    let what = serde_json::json!({ "what": "create-repo", "repo": name, "ref": trunk });
    audit
        .record(Event::new("admin", "intent", what.clone()))
        .await
        .map_err(|why| {
            tracing::error!(reason = %why, "cannot record the create-repo intent");
            Error::Io(why)
        })?;
    match create_repo_run(&audit, store, &name, &trunk).await {
        Ok(()) => Ok(()),
        Err(e) => {
            record_failed(&audit, "create-repo", &name, &e).await;
            Err(e)
        }
    }
}

/// The run behind [`create_repo`], after its intent: the checks, the one
/// write transaction, and the result's audit line.
async fn create_repo_run(audit: &Audit, store: Backend, name: &str, trunk: &str) -> Result<()> {
    names::check("repo", name)?;
    names::check("ref", trunk)?;
    // A cheap read first, so a repo that exists reads no write transaction.
    // The write transaction checks again: two racing runs order on the
    // write, and the loser sees the winner's changeset and rolls back.
    if store.read(|c| c.repo_id(name)).is_ok() {
        return Err(Error::FailedPrecondition(format!(
            "repo {name:?} already exists"
        )));
    }
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Internal(format!("the clock is before the epoch: {e}")))?
        .as_secs();
    let now = i64::try_from(secs)
        .map_err(|_| Error::Internal("the clock is past what a changeset can stamp".to_owned()))?;
    let run_store = store.clone();
    let (run_name, run_trunk) = (name.to_owned(), trunk.to_owned());
    let run = move || {
        let store = run_store;
        let cs = root_changeset(&run_name, now)?;
        store.write(|c| {
            let repo_id = c.create_repo(&run_name)?;
            if !c.repo_changeset_ids(repo_id)?.is_empty() {
                return Err(Error::FailedPrecondition(format!(
                    "repo {run_name:?} already has changesets"
                )));
            }
            let (id, _) =
                tree::store_changeset(c, repo_id, &cs, &mut tree::lens_from(&HashMap::new()))?;
            // The trunk ref at version 1.
            if !c.cas_ref(repo_id, &run_trunk, None, id)? {
                return Err(Error::FailedPrecondition(format!(
                    "ref {run_trunk:?} already exists"
                )));
            }
            Ok(id)
        })
    };
    match tokio::task::spawn_blocking(run).await {
        Ok(Ok(id)) => {
            store.flush()?;
            let fields = serde_json::json!({
                "what": "create-repo",
                "repo": name,
                "ref": trunk,
                "changeset": id.to_string(),
            });
            audit
                .record(Event::new("admin", "ok", fields))
                .await
                .map_err(|why| {
                    tracing::error!(reason = %why, "cannot record the create-repo result");
                    Error::Io(why)
                })?;
            println!("created repo {name}, ref {trunk} at {id}");
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(e) => Err(Error::Internal(format!("the create-repo task failed: {e}"))),
    }
}

/// Creates the ref `name` in `repo` at `target`, a changeset the repo
/// already holds, named by its hex ID. Refuses a ref that exists; moving a
/// ref stays a land's compare-and-set. Safe to repeat only in the sense
/// that a refused repeat changes nothing.
///
/// The audit carries one intent line before the change and one result line
/// after it, as the import's does.
///
/// # Errors
///
/// [`Error::InvalidArgument`] for a name or an ID that breaks its rule,
/// [`Error::NotFound`] for a repo or a changeset it does not hold,
/// [`Error::FailedPrecondition`] for a ref that exists, and the store's own
/// errors otherwise.
pub async fn create_ref(
    audit: Audit,
    store: Backend,
    repo: String,
    name: String,
    target_hex: String,
) -> Result<()> {
    // The attempt is evidence before anything else: a refused run owes
    // its intent the one failed line, as the import's does. A target
    // that is not an ID names itself in the intent the failed line
    // answers; the fields stay the refusal's own.
    let what = serde_json::json!({
        "what": "create-ref",
        "repo": repo,
        "ref": name,
        "target": target_hex,
    });
    audit
        .record(Event::new("admin", "intent", what.clone()))
        .await
        .map_err(|why| {
            tracing::error!(reason = %why, "cannot record the create-ref intent");
            Error::Io(why)
        })?;
    match create_ref_run(&audit, store, &repo, &name, &target_hex).await {
        Ok(()) => Ok(()),
        Err(e) => {
            record_failed(&audit, "create-ref", &repo, &e).await;
            Err(e)
        }
    }
}

/// The run behind [`create_ref`], after its intent.
async fn create_ref_run(
    audit: &Audit,
    store: Backend,
    repo: &str,
    name: &str,
    target_hex: &str,
) -> Result<()> {
    names::check("repo", repo)?;
    names::check("ref", name)?;
    let target = parse_changeset(target_hex)?;
    let run_store = store.clone();
    let (run_repo, run_name) = (repo.to_owned(), name.to_owned());
    let run = move || {
        let store = run_store;
        store.write(|c| {
            let repo_id = c.repo_id(&run_repo)?;
            if !c.has_changeset(repo_id, target)? {
                return Err(Error::NotFound(format!(
                    "repo {run_repo:?} does not hold changeset {target}"
                )));
            }
            // `cas_ref` is the one check: its compare-and-set holds the
            // ref's absence as its precondition, against the state the
            // write transaction sees, so a ref another create-ref (or a
            // racing run) inserts between this run's statements is still
            // refused here, and its current target names the refusal.
            if !c.cas_ref(repo_id, &run_name, None, target)? {
                let at = match c.get_ref(repo_id, &run_name)? {
                    Some(state) => state.target,
                    // cas_ref refused against the state it saw, so the
                    // ref is unreadable now: the command's own damage,
                    // not the store's. Internal, and the audit keeps
                    // its failed line.
                    None => {
                        return Err(Error::Internal(format!(
                            "ref {run_name:?} was refused but is unreadable"
                        )));
                    }
                };
                return Err(Error::FailedPrecondition(format!(
                    "ref {run_name:?} already exists at {at}"
                )));
            }
            Ok(target)
        })
    };
    match tokio::task::spawn_blocking(run).await {
        Ok(Ok(id)) => {
            store.flush()?;
            let fields = serde_json::json!({
                "what": "create-ref",
                "repo": repo,
                "ref": name,
                "target": id.to_string(),
                "version": 1,
            });
            audit
                .record(Event::new("admin", "ok", fields))
                .await
                .map_err(|why| {
                    tracing::error!(reason = %why, "cannot record the create-ref result");
                    Error::Io(why)
                })?;
            println!("created ref {name} in {repo} at {id}");
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(e) => Err(Error::Internal(format!("the create-ref task failed: {e}"))),
    }
}

/// The one failed line a refused run owes its intent: the why is the
/// error's class, the one word the import's failures carry too.
async fn record_failed(audit: &Audit, what: &str, repo: &str, e: &Error) {
    let class = match e {
        Error::Storage(storage) => match storage.class() {
            crate::StorageClass::Busy => "busy",
            crate::StorageClass::DiskFull => "disk_full",
            _ => "failed",
        },
        _ => "failed",
    };
    let fields = serde_json::json!({ "what": what, "repo": repo, "why": class });
    if let Err(why) = audit.record(Event::new("admin", "failed", fields)).await {
        tracing::error!(reason = %why, "an admin failure line was not made durable");
    }
}
