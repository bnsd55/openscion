import json, glob, re, sys
from collections import defaultdict

def key_of(mutant):
    # union key: file + function + replacement + start column (line numbers shift between pre/post-merge code states)
    fn = (mutant.get('function') or {}).get('function_name') or ''
    col = mutant['span']['start']['column']
    return f"{mutant.get('file')}|{fn}|{mutant.get('replacement')}|col{col}|{mutant.get('genre')}"

def bucket(summary):
    s = summary or ''
    if 'Timeout' in s: return 'timeout'
    if 'Caught' in s: return 'caught'
    if 'Missed' in s: return 'missed'
    if 'Unviable' in s: return 'unviable'
    return 'other:' + s

runs = {}
suites = defaultdict(set)
summaries = defaultdict(int)
for line in open('ids.txt'):
    name, rid = line.split()
    d = json.load(open(f'art-{name}/mutants.out/outcomes.json'))
    cases = {}
    for o in d['outcomes']:
        if o.get('scenario') == 'Baseline':
            continue
        m = o['scenario']['Mutant']
        k = key_of(m)
        b = bucket(o.get('summary'))
        summaries[b] += 1
        # best outcome per run per case
        rank = {'caught':0,'timeout':1,'missed':2,'unviable':3}
        if k not in cases or rank[b] < rank.get(cases[k], 99):
            cases[k] = b
        if b == 'other':
            print(f"  !! {name}: unknown summary {o.get('summary')} for {k}", file=sys.stderr)
    runs[name] = cases
    # executed test binaries from the baseline log (the run's actual suite set)
    for ln in open(f'art-{name}/mutants.out/log/baseline.log', errors='replace'):
        mo = re.search(r'Running `[^`]*deps/([a-z_0-9]+)-[0-9a-f]{16}`', ln)
        if mo: suites[name].add(mo.group(1))

# distinct suite sets
set_repr = defaultdict(list)
for name, ss in suites.items():
    set_repr[frozenset(ss)].append(name)
print("== suite sets per run ==")
for ss, names in set_repr.items():
    print(f"{sorted(names)}: {len(ss)} suites: {sorted(ss)}")

print("\n== per-run outcomes (cases by best outcome) ==")
tot = defaultdict(int)
for name, cases in runs.items():
    c = defaultdict(int)
    for b in cases.values(): c[b] += 1
    print(f"{name}: cases={len(cases)} caught={c['caught']} timeout={c['timeout']} missed={c['missed']} unviable={c['unviable']} other={sum(v for k,v in c.items() if k.startswith('other'))}")
    for b, n in c.items(): tot[b] += n

# union: best outcome per case across runs (caught > timeout > missed > unviable > other)
rank = {'caught':0,'timeout':1,'missed':2,'unviable':3}
union = {}
contrib = defaultdict(int)
for name, cases in runs.items():
    for k, b in cases.items():
        if k not in union or rank[b] < rank.get(union[k][0], 99):
            if k not in union: contrib[name] += 1
            union[k] = (b, name)

uc = defaultdict(int)
for b, _ in union.values(): uc[b] += 1
print(f"\n== UNION over {len(runs)} b-shards ==")
print(f"distinct cases covered: {len(union)} of 844")
print(f"caught={uc['caught']} timeout={uc['timeout']} missed={uc['missed']} unviable={uc['unviable']} other={sum(v for k,v in uc.items() if k.startswith('other'))}")

print("\n== the missed list (best=missed) ==")
for k, (b, name) in sorted(union.items()):
    if b == 'missed':
        print(f"{k}  [missed in {name}]")

with open('union.json','w') as f:
    json.dump({k: {'best': b, 'first_witness': n} for k,(b,n) in union.items()}, f, indent=1)
