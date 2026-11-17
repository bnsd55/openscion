import json, re
from collections import defaultdict

def bucket(summary):
    s = summary or ''
    if 'Timeout' in s: return 'timeout'
    if 'Caught' in s: return 'caught'
    if 'Missed' in s: return 'missed'
    if 'Unviable' in s: return 'unviable'
    return 'other:' + s

PG_B = ['b8','b9','b10','b11','b20','b21','b22','b23','b30','b31']
rank = {'caught':0,'timeout':1,'missed':2,'unviable':3}
runs = {}
for name in PG_B:
    d = json.load(open(f'art-{name}/mutants.out/outcomes.json'))
    cases = {}
    for o in d['outcomes']:
        if o.get('scenario') == 'Baseline': continue
        m = o['scenario']['Mutant']
        if not m['file'].endswith('pg.rs'): continue
        b = bucket(o.get('summary')); k = m['name']
        if k not in cases or rank[b] < rank.get(cases[k], 99): cases[k] = b
    runs[name] = cases

union = {}
for name, cases in runs.items():
    for k, b in cases.items():
        if k not in union or rank[b] < rank.get(union[k], 99): union[k] = b
uc = defaultdict(int)
for b in union.values(): uc[b] += 1
print(f"UNION over {len(PG_B)} pg.rs b-shards (exact mutant names)")
print(f"distinct cases covered: {len(union)} of 844")
print(f"caught={uc['caught']} timeout={uc['timeout']} missed={uc['missed']} unviable={uc['unviable']}")
print("\nMISSED LIST:")
for k in sorted(union):
    if union[k] == 'missed': print(f"  {k}")
json.dump(union, open('pg-union-exact.json','w'), indent=1)
