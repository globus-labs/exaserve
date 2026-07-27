import ijson, sys, json
R = "/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/sc26workshop/full"
FILES = {
    1:   f"{R}/proxycmp_direct/run0/n1/results/result0.json",
    64:  f"{R}/proxycmp_direct/run0/n64/results/result0.json",
    128: f"{R}/proxycmp_direct_scale/run0/n128/results/result0.json",
    256: f"{R}/proxycmp_direct_scale/run0/n256/results/result0.json",
}
def pct(a, q):
    a = sorted(a); k = (len(a)-1)*q; f = int(k)
    return a[f] + (a[min(f+1,len(a)-1)]-a[f])*(k-f)
out = {}
for N, path in FILES.items():
    runs = {}
    with open(path, "rb") as f:
        for r in ijson.items(f, "requests.item"):
            ri = r["run_index"]
            st = float(r["first_token_at"]) - float(r["ttft_s"])
            lat = float(r["latency"])
            d = runs.setdefault(ri, {"starts": [], "ends": [], "lats": [], "tbt": []})
            d["starts"].append(st); d["ends"].append(st+lat); d["lats"].append(lat)
            v = r.get("tbt_p50_s");
            (v is not None) and d["tbt"].append(float(v))
    res = {}
    for ri, d in sorted(runs.items()):
        s0, s1 = min(d["starts"]), max(d["starts"])
        e1 = max(d["ends"]); n = len(d["lats"])
        T = s1 - s0; makespan = e1 - s0; drain = e1 - s1
        after = sum(1 for e in d["ends"] if e > s1)
        res[ri] = dict(n=n, T=round(T,1), makespan=round(makespan,1), drain=round(drain,1),
                       frac_after_send=round(after/n,4),
                       lat_p50=round(pct(d["lats"],.5),2), lat_p99=round(pct(d["lats"],.99),2),
                       lat_max=round(max(d["lats"]),1),
                       tbt_p50_med=round(pct(d["tbt"],.5),4),
                       rps_measured=round(n/makespan,1), rps_sendwin=round(n/T,1),
                       model_TT=round(T/makespan,4))
    out[N] = res
    print(f"=== N={N} ===")
    for ri, v in res.items():
        print(f" run{ri}: {v}")
    sys.stdout.flush()
json.dump(out, open("/tmp/claude-37221/-home-wenyiw-exaserve/6a6522df-8a62-46be-a395-b323be56023e/scratchpad/tail_analysis.json","w"), indent=1)
print("DONE")
