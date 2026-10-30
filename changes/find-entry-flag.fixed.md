A lookup of one name in a stored tree checks the executable flag of
every entry it reads past, as the full decode does: a tree whose
earlier entry carries a flag byte other than 0 or 1 is refused as
corrupt instead of answering the later name.
