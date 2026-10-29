A new `--out` directory of `scion-server export` was made 0777 less
the umask (0755 under Ubuntu's 002, 0755 again under 022), and the
section files inside at 0644, so an archive that holds every blob,
changeset and ref of the store was group- or world-readable wherever
the operator pointed the flag. The archive root is now 0700 and the
section files 0600, whatever the umask, the way the data directory is.
