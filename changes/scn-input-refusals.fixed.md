`scn` answered three broken or absurd inputs with a panic, and now
answers each with an error that says what to fix:

- `scn commit` and `scn bench` built the changeset's author identity
  and the changeset itself with an unchecked assert whose only failure
  is a field longer than the encoder can write (over 4 GiB); the
  results are now errors that name the field.
- `scn mount` registered its stop-signal handlers with an unchecked
  result; a system that refuses them now hears why at startup.
- The mount's secret came from `/dev/urandom` with an unchecked open,
  so a system without the device panicked mid-mount; the mount now
  reports the error with its cause.
