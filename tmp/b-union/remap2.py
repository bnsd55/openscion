import json, re
from collections import defaultdict

# segment map: (old_start, old_end, offset) per gap, plus exact context maps per hunk
def build_segments(diff_path):
    segs = []   # (old_from, old_to_inclusive, offset) for gaps between hunks
    hunks = []  # (old_start, {old_line: new_line}, old_end, new_end)
    old = new = None
    cur = None
    for ln in open(diff_path, errors='replace'):
        if ln.startswith('@@'):
            mo = re.match(r'@@ -(\d+),(\d+) \+(\d+),(\d+) @@', ln)
            o_start, o_cnt, n_start, n_cnt = map(int, mo.groups())
            if cur is not None:
                hunks.append(cur)
            if hunks:
                segs.append((hunks[-1][3] + 1, o_start - 1, n_start - o_start))
            else:
                segs.append((1, o_start - 1, 0))
            cur = (o_start, {}, o_start + o_cnt - 1, n_start + n_cnt - 1)
            old, new = o_start, n_start
        elif old is None or ln.startswith('+++') or ln.startswith('---'):
            continue
        elif ln.startswith('+'):
            new += 1
        elif ln.startswith('-'):
            old += 1
        else:
            cur[1][old] = new
            old += 1; new += 1
    if cur is not None:
        hunks.append(cur)
        segs.append((hunks[-1][3] + 1, 10**9, new - old))
    return segs, hunks

SEGS, HUNKS = build_segments('hooks.diff')
def remap_line(old):
    for (o_start, cmap, o_end, n_end) in HUNKS:
        if o_start <= old <= o_end:
            return cmap.get(old)  # None => line was deleted (none here)
    for (a, b, off) in SEGS:
        if a <= old <= b:
            return old + off
    return old

def remap(name):
    mo = re.match(r'crates/scion-server/src/pg\.rs:(\d+):(\d+): (.*)', name)
    if not mo: return name
    old = int(mo.group(1))
    new = remap_line(old)
    if new is None: raise SystemExit(f"deleted line {old} in {name}")
    return f"crates/scion-server/src/pg.rs:{new}:{mo.group(2)}: {mo.group(3)}"

def bucket(s):
    s = s or ''
    for k in ('Timeout','Caught','Missed','Unviable'):
        if k in s: return k.lower()
    return 'other:' + s

rank = {'caught':0,'timeout':1,'missed':2,'unviable':3}
PRE = {'b8','b9','b10','b11','b20','b21','b22','b23'}
runs = {}
for line in open('ids.txt'):
    name, rid = line.split()
    if name not in PRE: continue
    d = json.load(open(f'art-{name}/mutants.out/outcomes.json'))
    cases = {}
    for o in d['outcomes']:
        if o.get('scenario') == 'Baseline': continue
        m = o['scenario']['Mutant']
        if not m['file'].endswith('pg.rs'): continue
        nm = remap(m['name'])
        b = bucket(o.get('summary'))
        if b.startswith('other'): print(f"!! {name}: {o.get('summary')} {nm}")
        if nm not in cases or rank[b] < rank.get(cases[nm], 99): cases[nm] = b
    runs[name] = cases

# merge with the post runs' current names from the earlier union
post = {k: v for k, v in json.load(open('pg-union-current.json')).items()}
union = dict(post)
for name, cases in runs.items():
    for k, b in cases.items():
        if k not in union or rank[b] < rank.get(union[k], 99): union[k] = b
uc = defaultdict(int)
for b in union.values(): uc[b] += 1
print(f"FINAL UNION: {len(union)} of 844 | caught={uc['caught']} timeout={uc['timeout']} missed={uc['missed']} unviable={uc['unviable']}")
print("flush_deferred + migrate_under_lock rows now:")
for k in sorted(union):
    if 'flush_deferred' in k or 'migrate_under_lock' in k:
        print(f"  {union[k]:9} {k}")
json.dump(union, open('pg-union-current.json','w'), indent=1)
print("wrote pg-union-current.json (current names)")
