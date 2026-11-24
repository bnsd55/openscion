`scion_stalled_reply_bytes` renders at 0 from the start: an operator
who greps the export on a quiet server reads the zero as "no reply is
stalled", not a missing series - the same shape
`scion_busy_timeouts_total` already took. metrics_v1.golden is
re-pinned for the now-conditional gauge's name and type (#1409).
