import httpx, time, json
URL="https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
P={"type":1,"pageNo":1,"pageSize":1,"catalogId":161}
rows=[]; c=httpx.Client(timeout=10.0, headers={"User-Agent":"newsbot-probe/1.0"})
t0=time.time()
for i in range(60):                       # 10 минут по 10 с
    try:
        r=c.get(URL, params=P)
        rows.append({"t":round(time.time()-t0,1),"status":r.status_code,
                     "cache":r.headers.get("x-cache","?").split(" ")[0],
                     "etag":r.headers.get("etag","")})
    except Exception as e:
        rows.append({"t":round(time.time()-t0,1),"error":type(e).__name__})
    time.sleep(10)
json.dump(rows, open("data/ttl_probe.json","w"), indent=1)
print("готово")
