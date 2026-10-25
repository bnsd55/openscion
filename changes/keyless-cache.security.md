A cache started without `--ticket-public-key` served objects to any
ticket its callers presented: the empty key list sent the cache down the
daemon's relay path, which parses the ticket without its signature and
skips the proof walk, so any caller that reached the cache could read
any object it holds, past repo and folder rules. The relay is now the
daemon's explicit constructor, a cache refuses to start without the key
(`Tier::new` refuses an empty list), and the call path chooses by the
stored mode, never by emptiness
(`scion-tier/tests/keyless_cache.rs::a_cache_without_a_public_key_does_not_start`).
What an operator must do: start every cache with `--ticket-public-key`
(the hex that `scion-server ticket-key` printed); an existing cache
started without it must be restarted with the key, and any caller that
reached it in that state is treated as having read its store.
