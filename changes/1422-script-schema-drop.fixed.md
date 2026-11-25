The scripts that start a real server drop the Postgres schema they
made when they exit: the schema's life is its data directory's, and
both go in the same cleanup. A run with SCION_KEEP_LOGS set keeps its
schema and writes its name next to the kept logs; a data directory
outside the run's work directory (a caller's own) keeps its schema.
