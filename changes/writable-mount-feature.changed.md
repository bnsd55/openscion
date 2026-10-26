The writable mount is a build feature, off by default: a build without
`--features unstable-writable-mount` has no write path at all, and
`scn mount` without the feature refuses the flag before it connects.
The read-only mount every build has is unchanged, and the vfs suite's
symlink tests run in the feature build while the default build's tests
hold the no-writable-mount surface. The write path stays out of
release builds until a rewrite that opens each path step by handle
passes the hostile tests.
