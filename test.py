import requests

# 1) 조회하고 싶은 날짜 (YYYY-MM-DD)
date = "2025-05-08"

# 2) 헤더 (User-Agent는 SEC 규칙상 꼭 필요)
headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    }

# 3) Elasticsearch DSL 형태의 바디
body = {
    "query": {
        "bool": {
            "must": [
                # Form Type이 8-K인 것
                { "term": { "formType.keyword": "8-K" } },
                # Filed 날짜가 date인 것
                { "range": {
                    "filedAt": {
                        "gte": date,
                        "lte": date
                    }
                } }
            ]
        }
    },
    "from": 0,
    "size": 500,     # 하루에 500건 넘지 않으니 넉넉히
    "sort": [
        { "filedAt": { "order": "asc" } }
    ]
}

# 4) API 호출
resp = requests.post(
    "https://efts.sec.gov/LATEST/search-index",
    headers=headers,
    json=body
)
resp.raise_for_status()
data = resp.json()

# 5) hits에서 CIK나 티커만 추출
hits = data["hits"]["hits"]
ciks   = [ h["_source"]["cik"]    for h in hits ]
tickers= [ h["_source"].get("ticker") for h in hits ]

print("CIKs  :", ciks)
print("Tickers:", tickers)
