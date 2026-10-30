//! `commit`.

use anyhow::{Context, Result, bail, ensure};
use clap::builder::{OsStringValueParser, TypedValueParser as _};
use scion_core::{Changeset, FileState, IdentityTime, RepoPath};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use crate::client::{Ctx, now_unix_secs};
use crate::outln;

/// Arguments of `scn commit`.
#[derive(clap::Args)]
pub struct CommitArgs {
    /// Base: ref name or changeset ID.
    #[arg(long, default_value = "main")]
    base: String,
    /// Commit message.
    #[arg(short, long)]
    message: String,
    /// `PATH=FILE`: put the content of local FILE at PATH. Repeatable.
    #[arg(
        long = "put",
        value_name = "PATH=FILE",
        value_parser = OsStringValueParser::new().try_map(|spec| parse_put(&spec))
    )]
    puts: Vec<(String, PathBuf)>,
    /// Mark the put files executable.
    #[arg(long)]
    executable: bool,
    /// PATH to delete. Repeatable.
    #[arg(long = "delete", value_name = "PATH")]
    deletes: Vec<String>,
    /// Author as `Name <email>`.
    #[arg(long, env = "SCION_AUTHOR", default_value = "scn <scn@localhost>")]
    author: String,
    /// Take the changes from the overlay of a `scn mount` on DIR, instead
    /// of --put and --delete. A build without the
    /// `unstable-writable-mount` feature refuses it before it connects.
    #[arg(long = "from-mount", value_name = "DIR", conflicts_with_all = ["puts", "deletes", "executable"])]
    from_mount: Option<PathBuf>,
}

impl CommitArgs {
    /// Whether this commit takes its changes from a mount's overlay,
    /// which only a build with the `unstable-writable-mount` feature
    /// serves.
    pub fn from_mount(&self) -> bool {
        self.from_mount.is_some()
    }
}

/// Splits `PATH=FILE` at its first `=`. FILE is a local file name, so its
/// bytes are taken as they are, as `checkout --into` takes a directory's;
/// PATH is a repo path, UTF-8 as on the rest of the command line.
fn parse_put(spec: &OsStr) -> Result<(String, PathBuf), String> {
    let bytes = spec.as_bytes();
    let shown = spec.to_string_lossy();
    let at = bytes
        .iter()
        .position(|&b| b == b'=')
        .ok_or_else(|| format!("{shown:?}: expected PATH=FILE"))?;
    let path =
        std::str::from_utf8(&bytes[..at]).map_err(|_| format!("{shown:?}: PATH is not UTF-8"))?;
    Ok((
        path.to_owned(),
        PathBuf::from(OsStr::from_bytes(&bytes[at + 1..])),
    ))
}

/// What to commit for one path: a file, a symlink, or a deletion.
enum Edit {
    /// The local file replaces the path, or adds it.
    Put { file: PathBuf, exec: bool },
    /// The symlink names these bytes.
    Link(Vec<u8>),
    /// The path of the base is deleted.
    Delete,
}

/// Uploads the files, creates the changeset, and prints its ID.
pub async fn commit(ctx: &mut Ctx, args: CommitArgs) -> Result<()> {
    // A build without the writable-mount feature refused the mount
    // argument in `main`, before anything connected; the write path it
    // guards is compiled out of that build.
    let (name, email) = parse_author(&args.author)?;
    let edits = match &args.from_mount {
        None => arg_edits(&args)?,
        Some(dir) => {
            let record = crate::cmd::mount::session_for_dir(dir)
                .with_context(|| format!("cannot commit from the mount on {}", dir.display()))?;
            // A read-only mount holds no changes: its writes are refused.
            // The flag is the same one the mount took its writes with, so
            // the error names it.
            anyhow::ensure!(
                record.writable,
                "the mount on {} is read-only; it takes `scn mount \
                 --unstable-writable`",
                dir.display()
            );
            let mut edits = BTreeMap::new();
            for (path, change) in crate::cmd::mount::overlay_changes(&record)? {
                let edit = match change {
                    crate::vfs::Change::Put { exec } => {
                        // The raw bytes, never a lossy name: a name that is
                        // not UTF-8 must name its own file.
                        let file = record
                            .overlay
                            .join(crate::vfs::CHANGES)
                            .join(OsStr::from_bytes(path.as_bytes()));
                        Edit::Put { file, exec }
                    }
                    crate::vfs::Change::PutLink(target) => Edit::Link(target),
                    crate::vfs::Change::Delete => Edit::Delete,
                };
                ensure!(!edits.contains_key(&path), "{path:?} changes twice");
                edits.insert(path, edit);
            }
            edits
        }
    };

    let parent = ctx.resolve(&args.base).await?;
    let mut changes = BTreeMap::new();
    for (path, edit) in edits {
        let state = match edit {
            Edit::Delete => None,
            Edit::Link(target) => {
                // SHORTCUT: reads each target whole, but a target is one
                // path, so its length is PATH_MAX (4096 bytes on Linux,
                // 1024 on macOS). Upgrade: none short of the protocol
                // renaming symlink targets to blob ids.
                let content = ctx
                    .put_blob(target)
                    .await
                    .context("cannot upload a symlink target")?;
                Some(FileState::Symlink { target: content })
            }
            Edit::Put { file, exec } => {
                check_put(&file)?;
                let content = ctx
                    .put_blob_file(&file)
                    .await
                    .with_context(|| format!("cannot upload {}", file.display()))?;
                Some(FileState::Regular {
                    content,
                    executable: exec,
                })
            }
        };
        changes.insert(path, state);
    }
    // SHORTCUT: always UTC, since std cannot read the local time zone.
    // Upgrade: a time zone crate.
    let author = IdentityTime::new(name, email, now_unix_secs()?, 0)
        .context("the author identity does not fit a changeset field")?;
    let changeset = Changeset::new(
        vec![parent],
        author,
        None,
        args.message.into_bytes(),
        changes,
    )
    .context("the changeset fields do not fit the format")?;
    let id = ctx.create_changeset(&changeset).await?;
    if ctx.json {
        outln!("{}", serde_json::json!({ "changeset": id.to_string() }));
    } else {
        outln!("{id}");
    }
    Ok(())
}

/// The edits of the command line: --put and --delete.
fn arg_edits(args: &CommitArgs) -> Result<BTreeMap<RepoPath, Edit>> {
    let mut edits = BTreeMap::new();
    let puts = args.puts.iter().map(|(path, file)| (path, Some(file)));
    for (path, file) in puts.chain(args.deletes.iter().map(|path| (path, None))) {
        ensure!(
            !path.starts_with('/'),
            "bad path {path:?}: repo paths are relative to the repository root, not to this machine"
        );
        let path = RepoPath::new(path.as_str()).with_context(|| format!("bad path {path:?}"))?;
        ensure!(
            !edits.contains_key(&path),
            "{path:?} is given more than once"
        );
        let edit = match file {
            None => Edit::Delete,
            Some(file) => Edit::Put {
                file: file.clone(),
                exec: args.executable,
            },
        };
        edits.insert(path, edit);
    }
    Ok(edits)
}

/// Refuses anything that is not a regular file, before any byte is
/// read: a device or FIFO never sees an end, so a stream of one would
/// grow until the OOM killer.
fn check_put(file: &Path) -> Result<()> {
    let meta =
        std::fs::metadata(file).with_context(|| format!("cannot read {}", file.display()))?;
    if !meta.is_file() {
        bail!(
            "{}: not a regular file; --put takes the content of a file",
            file.display()
        );
    }
    Ok(())
}

/// Splits `Name <email>` into name and email bytes.
fn parse_author(author: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let well_formed = |part: &str| !part.is_empty() && !part.contains(['<', '>', '\n']);
    match author
        .strip_suffix('>')
        .and_then(|rest| rest.split_once(" <"))
    {
        Some((name, email)) if well_formed(name) && well_formed(email) => {
            Ok((name.as_bytes().to_vec(), email.as_bytes().to_vec()))
        }
        _ => bail!("--author {author:?}: expected \"Name <email>\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn author_splits_name_and_email() {
        let (name, email) = parse_author("Ada Lovelace <ada@example.com>").unwrap();
        assert_eq!(name, b"Ada Lovelace");
        assert_eq!(email, b"ada@example.com");
    }

    /// An absolute repo path names a file on this machine, not in the
    /// repo: the refusal says repo paths are relative, and names the
    /// path, instead of `RepoPath`'s "empty part".
    #[test]
    fn an_absolute_repo_path_says_repo_paths_are_relative() {
        let args = CommitArgs {
            base: "main".into(),
            message: "m".into(),
            puts: vec![("/tmp/hello.txt".into(), "hello.txt".into())],
            executable: false,
            deletes: vec![],
            author: "scn <scn@localhost>".into(),
            from_mount: None,
        };
        let err = match arg_edits(&args) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("an absolute repo path was accepted"),
        };
        assert!(
            err.contains("repo paths are relative") && err.contains("/tmp/hello.txt"),
            "{err}"
        );
    }

    /// A local file name is raw bytes, as `checkout --into` takes it: a
    /// `--put` FILE that is not UTF-8 names its own file.
    #[test]
    fn a_put_file_whose_name_is_not_utf8_is_taken() {
        use clap::Parser as _;
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            commit: CommitArgs,
        }
        let put = OsString::from_vec(b"x=caf\xe9.txt".to_vec());
        let cli = Cli::try_parse_from([
            OsString::from("scn-commit"),
            "-m".into(),
            "m".into(),
            "--put".into(),
            put,
        ])
        .unwrap_or_else(|e| panic!("a non-UTF-8 FILE was refused: {e}"));
        assert_eq!(cli.commit.puts.len(), 1);
        assert_eq!(cli.commit.puts[0].0, "x");
        assert_eq!(cli.commit.puts[0].1.as_os_str().as_bytes(), b"caf\xe9.txt");
        // PATH stays UTF-8, and a spec with no `=` is refused as before.
        for (spec, why) in [
            (b"caf\xe9=x".as_slice(), "PATH is not UTF-8"),
            (b"x", "expected PATH=FILE"),
        ] {
            let err = Cli::try_parse_from([
                OsString::from("scn-commit"),
                "-m".into(),
                "m".into(),
                "--put".into(),
                OsString::from_vec(spec.to_vec()),
            ])
            .err()
            .unwrap_or_else(|| panic!("{spec:?} was accepted"));
            assert!(err.to_string().contains(why), "{err}");
        }
    }

    #[test]
    fn malformed_author_is_rejected() {
        for bad in [
            "Ada",
            "ada@example.com",
            "<ada@example.com>",
            "Ada <>",
            "Ada <ada@example.com",
            "Ada<ada@example.com>",
            "Ada <ada@example.com> x",
            "Ada <a <b>",
            "Ada\nEve <ada@example.com>",
        ] {
            assert!(parse_author(bad).is_err(), "{bad:?} was accepted");
        }
    }

    /// A put refuses what never sees an end, at once: a read of a FIFO
    /// or of /dev/zero would otherwise grow until the OOM killer.
    #[test]
    fn a_put_refuses_what_never_sees_an_end() {
        // mkfifo forks, and a fork carries every lock the process holds
        // to the child until it execs: it runs under the crate's gate,
        // like every test here that forks.
        let _serial = crate::fork_gate::serial();
        let refused = |file: std::path::PathBuf| {
            let (sent, done) = std::sync::mpsc::channel();
            std::thread::spawn(move || sent.send(check_put(&file).is_err()).unwrap());
            // The refusal needs no read: one that hangs fails the test.
            done.recv_timeout(Duration::from_secs(2))
                .unwrap_or_else(|_| panic!("the read was not refused at once"))
        };
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("pipe");
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
            .then_some(())
            .expect("mkfifo runs");
        assert!(refused(fifo), "a FIFO was accepted");
        assert!(
            refused(std::path::PathBuf::from("/dev/zero")),
            "/dev/zero was accepted"
        );
    }
}
