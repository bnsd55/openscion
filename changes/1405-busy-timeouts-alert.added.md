The `BusyTimeoutsRising` alert on `scion_busy_timeouts_total{limit}`: it
fires when a shared limit's busy timeouts have climbed by more than six
in ten minutes for 15 minutes, and its runbook is docs/operations.md's
"Shared limits busy" - the audit log and the per-principal request
counts name who holds the limit, and the per-principal caps are the
knob (#1405).
