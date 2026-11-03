-- A rejected land carries the reason it can never succeed, as one
-- word, beside the sentence `detail` holds. One row per land, so the
-- value lands with the state it belongs to. Existing rows and every
-- non-rejected state read 'unspecified'.
ALTER TABLE lands ADD COLUMN reject_reason TEXT NOT NULL DEFAULT 'unspecified'
    CHECK (reject_reason IN ('unspecified', 'needs_one_parent', 'missing_ref',
                             'base_not_on_history', 'abandoned'));
