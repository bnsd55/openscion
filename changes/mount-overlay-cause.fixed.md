A mount whose overlay write or read failed answered `I/O error` and
printed nothing: the disk-full, the permission refusal, or the errno
that caused it was dropped, so an operator whose mount lost its overlay
had nothing to read. The failure now warns once per operation and file,
with the cause and its OS error number, and answers the same reply as
before.
