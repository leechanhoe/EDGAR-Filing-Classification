import requests
import pandas as pd
import time
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta

mongo_uri    = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
mongo_client = MongoClient(mongo_uri)
mongo_db     = mongo_client[MONGO_DB]
raw_col      = mongo_db[RAW_COLL]
split_col    = mongo_db[SPLIT_COLL]

def get_company_tickers():
    """
    SEC의 company_tickers.json 파일에서 기업 정보를 가져옵니다.

    Returns:
        pandas.DataFrame: CIK, 티커, 종목명 정보를 담은 데이터프레임
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        "User-Agent": "Chan Lee (klavier0823@gmail.com)",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # JSON 데이터를 데이터프레임으로 변환
        df = pd.DataFrame.from_dict(data, orient="index")

        # 컬럼명 변경
        df.columns = ["cik", "ticker", "title"]

        # CIK를 10자리 문자열로 변환 (앞에 0을 채움)
        df["cik"] = df["cik"].astype(str).str.zfill(10)

        return df

    except Exception as e:
        print(f"데이터 가져오기 실패: {str(e)}")
        return None


def get_recent_8k_filings(cik, ticker, end_date):
    """
    특정 기업의 당일 8-K 공시 url들을 가져옵니다.

    Args:
        cik (str): 기업의 CIK 번호
        ticker (str): 기업의 티커 심볼
        end_date (datetime): 기준 날짜

    Returns:
        list: (티커, url, 공시일자) 튜플의 리스트
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
        "User-Agent": "Chan Lee (klavier0823@gmail.com)",
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }

    try:
        time.sleep(0.1)  # SEC API 요청 제한 준수
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        filing_urls = []
        recent_filings = data["filings"]["recent"]

        # form 타입, accessionNumber, primaryDocument, filingDate를 매칭
        for form_type, acc_num, primary_doc, filing_date, items in zip(
            recent_filings["form"],
            recent_filings["accessionNumber"],
            recent_filings["primaryDocument"],
            recent_filings["filingDate"],
            recent_filings["items"],
        ):
            if form_type == "8-K":
                filing_date = datetime.strptime(filing_date, "%Y-%m-%d")
                if filing_date.date() == end_date.date():  # 당일 공시만 필터링
                    filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_num.replace('-','')}/{primary_doc}"
                    filing_urls.append(
                        (ticker, acc_num, filing_url, filing_date.strftime("%Y-%m-%d"), items)
                    )

        return filing_urls

    except Exception as e:
        print(f"{ticker}의 공시 url 가져오기 실패: {str(e)}")
        return []


def get_recent_8k_ciks(end_date=None):
    """
    SEC 웹사이트에서 최근 8-K 공시를 한 기업들의 CIK를 수집합니다.

    Args:
        end_date (datetime, optional): 기준 날짜. 기본값은 현재 날짜입니다.

    Returns:
        set: CIK 번호들의 집합
    """
    if end_date is None:
        end_date = datetime.now()

    date_str = end_date.strftime("%Y-%m-%d")
    base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    headers = {
        "User-Agent": "Chan Lee (klavier0823@gmail.com)",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }

    ciks = set()
    start = 0
    count = 100

    while True:
        try:
            # url 구성
            params = {
                "action": "getcurrent",
                "type": "8-K",
                "owner": "include",
                "count": count,
                "start": start,
            }

            time.sleep(0.1)  # SEC API 요청 제한 준수
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()

            # HTML 파싱
            soup = BeautifulSoup(response.text, "html.parser")

            # 현재 페이지의 날짜 확인
            date_cells = soup.find_all(
                "td", string=lambda x: x and x.strip().startswith(date_str)
            )
            if not date_cells:
                break

            # CIK 추출
            for a_tag in soup.find_all("a", href=lambda x: x and "CIK=" in x):
                cik_match = re.search(r"CIK=(\d{10})", a_tag["href"])
                if cik_match:
                    ciks.add(cik_match.group(1))

            start += count

        except Exception as e:
            print(f"CIK 수집 중 오류 발생: {str(e)}")
            break

    print(f"\n총 {len(ciks)}개의 고유한 CIK가 수집되었습니다.")

    return ciks


def collect_all_recent_8k_filings(end_date=None):
    """
    전 종목의 당일 8-K 공시 url을 수집합니다.

    Args:
        end_date (datetime, optional): 기준 날짜. 기본값은 현재 날짜입니다.

    Returns:
        list: (티커, accession_nunber, url, 공시일자) 튜플의 리스트
    """
    # 기준 날짜 설정
    if end_date is None:
        end_date = datetime.now()

    # 기업 정보 가져오기
    companies_df = get_company_tickers()
    if companies_df is None:
        return []

    # 최근 8-K 공시를 한 기업들의 CIK 수집
    recent_ciks = get_recent_8k_ciks(end_date)

    # 최근 8-K 공시를 한 기업들만 필터링
    companies_df = companies_df[companies_df["cik"].isin(recent_ciks)]
    print(f"\n최근 8-K 공시를 한 기업 수: {len(companies_df)}")

    # 결과를 저장할 리스트
    all_filings = []

    # 각 기업별로 8-K 공시 수집
    total_companies = len(companies_df)
    for idx, (_, company) in enumerate(companies_df.iterrows(), 1):
        print(f"처리 중: {company['ticker']} ({idx}/{total_companies})")
        filings = get_recent_8k_filings(company["cik"], company["ticker"], end_date)
        all_filings.extend(filings)

    # 중복 검사 및 제거
    accnum_dict = {}
    duplicates = []
    unique_filings = []

    for filing in all_filings:
        ticker, acc_num, url, filing_date, items = filing
        if acc_num in accnum_dict:
            duplicates.append((ticker, acc_num, url, filing_date, items))
            print(f"중복 발견: {ticker} - {acc_num}")
            print(f"  기존: {accnum_dict[acc_num]}")
            print(f"  중복: {url}")
        else:
            accnum_dict[acc_num] = url
            unique_filings.append(filing)

    if duplicates:
        print(f"\n총 {len(duplicates)}개의 중복된 accession_nunber가 발견되었습니다.")
        print("중복된 항목은 제외하고 저장됩니다.")

    print(f"\n총 {len(unique_filings)}개의 8-K 공시가 수집되었습니다.")
    return unique_filings


def extract_filing_content(url):
    """
    8-K 공시 url에서 Item 유형과 본문을 추출합니다.

    Args:
        url (str): 공시 url

    Returns:
        tuple: (Item 유형 리스트, 본문 텍스트)
    """
    headers = {
        "User-Agent": "Chan Lee (klavier0823@gmail.com)",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }

    try:
        time.sleep(0.1)  # SEC API 요청 제한 준수
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # windows-1252 인코딩으로 설정
        response.encoding = "windows-1252"

        # HTML 파싱
        soup = BeautifulSoup(response.text, "html.parser", from_encoding="windows-1252")

        # 불필요한 태그 제거
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()

        # 텍스트 추출 및 정제
        paragraphs = []
        for tag in soup.stripped_strings:
            # 특수 문자 처리를 위한 정규식
            cleaned_text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", tag)
            # 연속된 공백을 하나로 변환
            # cleaned_text = re.sub(r'[ \t]+', ' ', cleaned_text)
            if cleaned_text.strip():
                paragraphs.append(cleaned_text.strip())

        # 줄바꿈을 유지하면서 텍스트 결합
        text = "\n\n".join(paragraphs)

        # --- 추가: TOC 후 첫 signatures 이후로 필터링 ---
        toc = re.search(r"(?i)table\s+of\s+contents", text)
        if toc:
            after_toc = text[toc.end():]
            sig_iter = list(re.finditer(r"(?i)s\s*i\s*g\s*n\s*a\s*t\s*u\s*r\s*e\s*s?", after_toc))
            if len(sig_iter) >= 2:
                # 첫 signatures 매치의 끝 위치 이후를 새로운 text로
                first_sig = sig_iter[0]
                text = after_toc[first_sig.end():]

        # 본문 추출
        # 첫 번째 Item이 나오는 위치 찾기 - 대소문자 구분 없이
        first_item_match = re.search(r"(?i)i\s*t\s*e\s*m\s*\.?\s*(\d+\.\d+)", text)
        if first_item_match:
            start_pos = first_item_match.start()

            # 모든 'SIGNATURES' 위치 찾기 - 대소문자 구분 없이
            signature_positions = [
                m.start() for m in re.finditer(r"(?i)s\s*i\s*g\s*n\s*a\s*t\s*u\s*r\s*e\s*s?", text[start_pos:])
            ]

            if signature_positions:
                # 마지막 'SIGNATURES' 위치 사용
                end_pos = start_pos + signature_positions[-1]
                content = text[start_pos:end_pos].strip()
            else:
                content = text[start_pos:].strip()

            content = re.sub(r"\s*\n\s*", "\n", content)  # 줄바꿈 주변 공백 제거
            content = clean_non_ascii_newlines(content)
        else:
            content = ""

        return content

    except Exception as e:
        print(f"공시 내용 추출 실패 ({url}): {str(e)}")
        return [], ""


def clean_non_ascii_newlines(text):
    """
    아스키코드가 아닌 값들은 가끔 앞or뒤에 줄바꿈이 있는 경우가 있어 줄바꿈 제거
    """
    # 앞뒤 줄바꿈이 하나도 없어도, 하나만 있어도, 여러 개 있어도 모두 매칭
    return re.sub(r"\n*([^\x00-\x7F])\n*", r"\1", text)


# 1) 분할 전용 매핑: 첫 단어만, 소문자, s optional
item_type_mapping_for_split = {
    "1.01": r"Entry",     # Entry or Entrys
    "1.02": r"Termination",
    "1.03": r"Bankruptcy",
    "1.04": r"Mine",
    "1.05": r"Material",
    "2.01": r"Completion",
    "2.02": r"Result",
    "2.03": r"Creation",
    "2.04": r"Triggering",
    "2.05": r"(?:Cost|item)s?",
    "2.06": r"Material",
    "3.01": r"Notice",
    "3.02": r"(?:Unregistered|Sale)s?",
    "3.03": r"Material",
    "4.01": r"Change",    # Changes? (원본엔 Change이지만 Changes 허용)
    "4.02": r"(?:Non-Reliance|NonReliance)s?",
    "5.01": r"Change",
    "5.02": r"(?:Departure|Resignation)s?",
    "5.03": r"Amendment",
    "5.04": r"Temporary",
    "5.05": r"Amendment",
    "5.06": r"Change",
    "5.07": r"Submission",
    "5.08": r"Shareholder",
    "6.01": r"ABS",
    "6.02": r"Change",     # Change or Changes
    "6.03": r"Change",
    "6.04": r"Failure",
    "6.05": r"Securitie",
    "7.01": r"Regulation",
    "8.01": r"Other",
    "9.01": r"(?:exhibit|financial|\(d\))s?",   # Exhibits 또는 Exhibit
}

def split_by_items_whitespace_agnostic(content_full, item_numbers, mapping):
    """
    공백/줄바꿈/비영숫자 제거 + 
    'itemX.XX' + optional '.' + 매핑 앞글자(s?) 를 기준으로 분리.
    Returns: dict of { item_code: item_content_str }
    """
    # 1) 원본 → 압축문자 매핑
    compressed_chars = []
    orig_to_comp = []
    for idx, ch in enumerate(content_full):
        if ch.isspace():
            continue
        cl = ch.lower()
        if re.match(r"[a-z0-9\(\)]", cl):
            compressed_chars.append(cl)
            orig_to_comp.append(idx)
    compressed = "".join(compressed_chars)
    item_ranges = {}

    for item in item_numbers:
        # 1) 매핑값(정규식 그룹) 그대로 읽어와 소문자
        fw_raw = mapping.get(item, "")
        if not fw_raw:
            continue
        # 이미 소문자, [a-z0-9()]와 '|' 와 '?' 만 남아 있다고 가정
        fw_pattern = fw_raw.lower()

        # 2) item 번호 정제 (숫자만)
        num = re.sub(r"[^0-9]", "", item)   # '9.01' -> '901'

        # 3) 최종 패턴 조합
        #    - 'item' + optional '.' + 번호 + optional '.' + (exhibit|financial)s?
        pat = rf"item\.?{num}\.?{fw_pattern}"

        # 4) 검색
        m = re.search(pat, compressed)
        if m:
            item_ranges[item] = [m.start(), None]

    # 3) 끝 위치 결정
    sorted_items = sorted(item_ranges.items(), key=lambda kv: kv[1][0])
    for (it, (st, _)), (nxt, (nst, _)) in zip(sorted_items, sorted_items[1:]):
        item_ranges[it][1] = nst
    if sorted_items:
        last = sorted_items[-1][0]
        item_ranges[last][1] = len(compressed)

    # 4) 압축→원본 매핑 후 자르기
    result = {}
    for it, (cstart, cend) in item_ranges.items():
        orig_start = orig_to_comp[cstart]
        orig_end   = orig_to_comp[cend-1] + 1
        result[it] = content_full[orig_start:orig_end].strip()

    return result


def process_filings(filings):
    """
    filings: list of tuples
      (ticker, accession_nunber, url, filing_date, item_number)
    1) raw upsert -> RAW_COLL
    2) split  upsert -> SPLIT_COLL
    """
    # 1) 원본 DataFrame
    df = pd.DataFrame(filings,
         columns=["ticker", "accession_nunber", "url", "filing_date", "item_number"])
    df["content"] = ""

    # 2) URL별 content 추출
    total = len(df)
    for idx, row in df.iterrows():
        print(f"Extracting content {idx+1}/{total}")
        df.at[idx, "content"] = extract_filing_content(row["url"])

    # 3) 중간 결과 raw upsert
    ops = []
    for rec in df.to_dict(orient="records"):
        ops.append(
            UpdateOne(
                {"accession_nunber": rec["accession_nunber"]},
                {"$set": rec},
                upsert=True
            )
        )
    if ops:
        res = raw_col.bulk_write(ops)
        print(f"Raw upsert: matched={res.matched_count}, upserted={len(res.upserted_ids)}")

    # 4) split
    split_ops = []
    for _, row in df.iterrows():
        items = [i.strip() for i in str(row["item_number"]).split(",") if i.strip()]
        content_full = row["content"]
        item_contents = split_by_items_whitespace_agnostic(
            content_full, items, item_type_mapping_for_split
        )
        # fallback 계산
        remainder = content_full
        for c in item_contents.values():
            if c:
                remainder = remainder.replace(c, "")
        fallback = remainder.strip()

        for it in items:
            cnt = item_contents.get(it, "")
            final_content = cnt if cnt else fallback
            doc = {
                "ticker": row["ticker"],
                "accession_nunber": row["accession_nunber"],
                "url": row["url"],
                "filing_date": row["filing_date"],
                "item_number": it,
                "content": final_content
            }
            split_ops.append(
                UpdateOne(
                    {"accession_nunber": doc["accession_nunber"],
                     "item_number":    doc["item_number"]},
                    {"$set": doc},
                    upsert=True
                )
            )

    if split_ops:
        res2 = split_col.bulk_write(split_ops)
        print(f"Split upsert: matched={res2.matched_count}, upserted={len(res2.upserted_ids)}")

    return df


if __name__ == "__main__":
    start = datetime.now()
    print("Start:", start)

    end_date = datetime.now() - timedelta(days=1)
    filings  = collect_all_recent_8k_filings(end_date)
    if filings:
        process_filings(filings)
    else:
        print("No filings to process")

    end = datetime.now()
    print("End:", end, "Duration:", end - start)