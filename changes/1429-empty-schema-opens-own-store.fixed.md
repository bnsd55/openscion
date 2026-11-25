An empty schema opens as its own store: the open's version probe and
identity reads are qualified to the schema the URL names, so a later
`search_path` member (the `public` the scripts append) is never read.
A fresh schema migrates itself; a store in a later member is never
adopted, refused, or written through.

`migrate --check` on a URL whose search_path names no existing schema
now exits 1 with the refusal to create the schema first, where it used
to report version 0 with every step pending - it must predict what
`migrate` does, and migrate on a missing schema fails.
