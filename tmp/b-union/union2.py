import json, re, sys
from collections import defaultdict

def key_of(mutant):
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

PG_B = ['b8','b9','b10','b11','b20','b21','b22','b23','b30','b31']
runs, suites = {}, {}
for name in PG_B:
    d = json.load(open(f'art-{name}/mutants.out/outcomes.json'))
    cases = {}
    for o in d['outcomes']:
        if o.get('scenario') == 'Baseline': continue
        m = o['scenario']['Mutant']
        if not m['file'].endswith('pg.rs'): continue
        b = bucket(o.get('summary'))
        k = key_of(m)
        rank = {'caught':0,'timeout':1,'missed':2,'unviable':3}
        if k not in cases or rank[b] < rank.get(cases[k], 99): cases[k] = b
    runs[name] = cases
    ex = set()
    for ln in open(f'art-{name}/mutants.out/log/baseline.log', errors='replace'):
        # executed test binaries: "Running `<...>/deps/<name>-<hash>`" (not rustc compile lines)
        mo = re.search(r'Running\x1b\[0m `[^`]*deps/([A-Za-z_0-9]+)-[0-9a-f]{16}`', ln)
        if mo: ex.add(mo.group(1))
    suites[name] = ex

set_repr = defaultdict(list)
for name, ss in suites.items(): set_repr[frozenset(ss)].append(name)
print("== each pg.rs b-run's executed suite set ==")
for ss, names in sorted(set_repr.items(), key=lambda kv: -len(kv[1])):
    print(f"{sorted(names)} -> {len(ss)} suites: {sorted(ss)}")

print("\n== per-run (pg.rs cases only) ==")
for name, cases in runs.items():
    c = defaultdict(int)
    for b in cases.values(): c[b] += 1
    print(f"{name}: cases={len(cases)} caught={c['caught']} timeout={c['timeout']} missed={c['missed']} unviable={c['unviable']}")

rank = {'caught':0,'timeout':1,'missed':2,'unviable':3}
union = {}
for name, cases in runs.items():
    for k, b in cases.items():
        if k not in union or rank[b] < rank.get(union[k], 99): union[k] = b
uc = defaultdict(int)
for b in union.values(): uc[b] += 1
print(f"\n== UNION over {len(PG_B)} pg.rs b-shards ==")
print(f"distinct pg.rs cases covered: {len(union)} of 844")
print(f"caught={uc['caught']} timeout={uc['timeout']} missed={uc['missed']} unviable={uc['unviable']}")

print("\n== the missed list ==")
for k in sorted(union):
    if union[k] == 'missed':
        print(k)
json.dump(union, open('pg-union.json','w'), indent=1)
