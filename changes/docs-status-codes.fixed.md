The status-code table of docs/protocol-v1.md no longer says that only
`GetObjects` answers `RESOURCE_EXHAUSTED`: the row now names every kind
of server limit that answers it (a per-principal quota, a memory
budget, the unary-reply budget, a reply's size cap, a full disk), as
the rest of the document already did. The README's partial-reader
section says that `GetObjects` without a ticket answers
`PERMISSION_DENIED`, as for every caller, not `UNIMPLEMENTED`.
