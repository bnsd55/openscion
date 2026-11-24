A mount lists a directory only if every entry's name is valid: the
listing insert checks every name before it adds any node, so a tree
with a refused name changes nothing and the directory answers an error
naming the directory and the refused name. A walk that added as it went
left its own orphans in the node map, and the next listing dropped
every name before the refusal for good.
