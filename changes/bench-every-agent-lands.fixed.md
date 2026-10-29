`scn bench --agents N` runs N agents that each land, as its report
says. It gave every agent the rounded-up share of `--lands`, so the
lands ran out before the last agents got any: `--agents 300 --lands
1000` ran 250 agents and reported 300, and its throughput and latency
described a lower load. The lands now spread as evenly as they divide,
and more agents than lands run one agent per land.
