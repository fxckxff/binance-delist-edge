import httpx, time, json
URL="https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
VAR={
 "ps1":      {"type":1,"pageNo":1,"pageSize":1,"catalogId":161},
 "ps5":      {"type":1,"pageNo":1,"pageSize":5,"catalogId":161},
 "ps10":     {"type":1,"pageNo":1,"pageSize":10,"catalogId":161},
 "ps20":     {"type":1,"pageNo":1,"pageSize":20,"catalogId":161},
 "ps50":     {"type":1,"pageNo":1,"pageSize":50,"catalogId":161},
 "type2":    {"type":2,"pageNo":1,"pageSize":10,"catalogId":161},
}
out={}
c=httpx.Client(timeout=15.0, headers={"User-Agent":"newsbot-probe/1.0"})
t0=time.time(); N=70; GAP=5
rows={k:[] for k in VAR}
for i in range(N):
    for k,p in VAR.items():
        try:
            r=c.get(URL, params=p)
            rows[k].append({"t":round(time.time()-t0,1),"status":r.status_code,
                            "cache":r.headers.get("x-cache","?").split(" ")[0],
                            "age":r.headers.get("age")})
        except Exception as e:
            rows[k].append({"t":round(time.time()-t0,1),"err":type(e).__name__})
    time.sleep(GAP)
    if (i+1)%6==0: print(f"  {i+1}/{N} циклов, {time.time()-t0:.0f}s", flush=True)
json.dump(rows, open("data/ttl_variants.json","w"), indent=1)
for k,v in rows.items():
    ages=[x.get("age") for x in v if x.get("age")]
    resets=[]
    prev=None
    for x in v:
        a=x.get("age")
        if a is None: continue
        a=int(a)
        if prev is not None and a<prev: resets.append(x["t"])
        prev=a
    ttl=[b-a for a,b in zip(resets,resets[1:])]
    print(f"{k:8s} | запросов {len(v)} | age видели {len(ages)} | сбросов {len(resets)} | TTL≈{ttl}")
