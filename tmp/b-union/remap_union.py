import json, re
from collections import defaultdict

# 1. old->new line map from the real diff (context lines keep their number)
def build_map(diff_path):
    m = {}
    old = new = None
    for ln in open(diff_path, errors='replace'):
        if ln.startswith('@@'):
            mo = re.match(r'@@ -(\d+),\d+ \+(\d+),\d+ @@', ln)
            old, new = int(mo.group(1)), int(mo.group(2))
        elif old is None:
            continue
        elif ln.startswith('+++') or ln.startswith('---'):
            continue
        elif ln.startswith('+'):
            new += 1
        elif ln.startswith('-'):
            old += 1
        else:  # context
            m[old] = new
            old += 1; new += 1
    return m

LM = build_map('hooks.diff')
def remap(name):
    mo = re.match(r'crates/scion-server/src/pg\.rs:(\d+):(\d+): (.*)', name)
    if not mo: return name
    old = int(mo.group(1))
    new = LM.get(old, old)
    if new is None:
        raise SystemExit(f"no mapping for old line {old} in {name}")
    return f"crates/scion-server/src/pg.rs:{new}:{mo.group(2)}: {mo.group(3)}"

def bucket(s):
    s = s or ''
    for k in ('Timeout','Caught','Missed','Unviable'):
        if k in s: return k.lower()
    return 'other:' + s

rank = {'caught':0,'timeout':1,'missed':2,'unviable':3}
PRE = {'b8','b9','b10','b11','b20','b21','b22','b23'}
POST = {'b30','b31','b32'}
runs = {}
for line in open('ids.txt'):
    name, rid = line.split()
    if name not in PRE and name not in POST: continue   # b0-b7 = import.rs, out of scope
    d = json.load(open(f'art-{name}/mutants.out/outcomes.json'))
    cases = {}
    for o in d['outcomes']:
        if o.get('scenario') == 'Baseline': continue
        m = o['scenario']['Mutant']
        if not m['file'].endswith('pg.rs'): continue
        nm = remap(m['name']) if name in PRE else m['name']
        b = bucket(o.get('summary'))
        if b.startswith('other'): print(f"!! {name}: {o.get('summary')} {nm}")
        if nm not in cases or rank[b] < rank.get(cases[nm], 99): cases[nm] = b
    runs[name] = cases

union = {}
for name, cases in runs.items():
    for k, b in cases.items():
        if k not in union or rank[b] < rank.get(union[k], 99): union[k] = b
uc = defaultdict(int)
for b in union.values(): uc[b] += 1
print(f"REMAPPED UNION over {len(runs)} pg.rs b-runs (8 PRE remapped by 4d6cd405's hunks + 3 post)")
print(f"distinct cases (current names): {len(union)} of 844")
print(f"caught={uc['caught']} timeout={uc['timeout']} missed={uc['missed']} unviable={uc['unviable']}")
with open('pg-union-current.json','w') as f:
    json.dump(union, f, indent=1)
print("wrote pg-union-current.json")
# sanity: the 6539-family names under current numbering
for k in sorted(union):
    if 'put_history_node' in k or 'set_land_state' in k or 'imported_commits' in k or 'put_git_imports' in k:
        if union[k] in ('missed','unviable'): print(f"  {union[k]:9} {k}")
