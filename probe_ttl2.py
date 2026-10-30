import httpx, time, json
URL="https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
rows=[]; c=httpx.Client(timeout=10.0, headers={"User-Agent":"newsbot-probe/1.0"})
t0=time.time()
for i in range(30):                       # 5 минут по 10 с, pageSize=10 (как у бота)
    P={"type":1,"pageNo":1,"pageSize":10,"catalogId":161}
    try:
        r=c.get(URL, params=P)
        rows.append({"t":round(time.time()-t0,1),"status":r.status_code,
                     "cache":r.headers.get("x-cache","?").split(" ")[0],
                     "etag":r.headers.get("etag","")[:20],"len":len(r.content)})
    except Exception as e:
        rows.append({"t":round(time.time()-t0,1),"error":type(e).__name__})
    json.dump(rows, open("data/ttl_probe_ps10.json","w"), indent=1)   # пишем инкрементально
    time.sleep(10)
print("готово")
