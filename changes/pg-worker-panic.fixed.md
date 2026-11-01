A database worker that panics no longer dies in silence: the log names
the panic, and the caller's "the database worker stopped" error says the
log names why. A store_uuid row that is not 16 bytes refuses the open
with the row's length, instead of panicking it.
