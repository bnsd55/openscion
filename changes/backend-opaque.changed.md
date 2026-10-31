The server's Backend is an opaque struct with one private engine
behind it, opened by the engine-agnostic constructors. A caller
matches behavior through the methods, never the engine, so a new
engine (or a major bump of one) is a private change of the server
crate, not a break of its API. The tests that build a backend from a
store they prepared, or read the data directory's own store out of
one, find both in test_support::backend (from_store), where the test
seams live since the facade became the API.
