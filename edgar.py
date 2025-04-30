import requests
import pandas as pd
import json
import time
from bs4 import BeautifulSoup
import re
import csv
import openai
import os
from openai import OpenAI

os.environ["OPENAI_API_KEY"] = "sk-svcacct-w9oJFmpeEWwDL0gqsDnZT3BlbkFJXIUfetQoq5xJZDzADdEY"

def get_company_tickers():
    """
    SEC의 company_tickers.json 파일에서 기업 정보를 가져옵니다.
    
    Returns:
        pandas.DataFrame: CIK, 티커, 종목명 정보를 담은 데이터프레임
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',  # 실제 이름과 이메일로 변경
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # JSON 데이터를 데이터프레임으로 변환
        df = pd.DataFrame.from_dict(data, orient='index')
        
        # 컬럼명 변경
        df.columns = ['cik', 'ticker', 'title']
        
        # CIK를 10자리 문자열로 변환 (앞에 0을 채움)
        df['cik'] = df['cik'].astype(str).str.zfill(10)
        
        return df
    
    except Exception as e:
        print(f"데이터 가져오기 실패: {str(e)}")
        return None

def get_filing_urls(ticker):
    """
    특정 종목의 티커를 입력받아 해당 기업의 공시 URL들을 가져옵니다.
    
    Args:
        ticker (str): 종목 티커 심볼
        
    Returns:
        list: 공시 URL 리스트
    """
    # 먼저 티커에 해당하는 CIK 값을 찾습니다
    df = get_company_tickers()
    if df is None:
        return None
        
    company_info = df[df['ticker'] == ticker.upper()]
    if company_info.empty:
        print(f"티커 {ticker}에 해당하는 기업을 찾을 수 없습니다.")
        return None
        
    cik = company_info['cik'].iloc[0]
    
    # SEC API에서 공시 메타 정보를 가져옵니다
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'data.sec.gov'
    }
    
    try:
        # API 요청 전 딜레이
        time.sleep(0.1)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # 공시 URL 리스트 생성
        filing_urls = []
        recent_filings = data['filings']['recent']
        
        # accessionNumber와 primaryDocument를 매칭하여 URL 생성
        for acc_num, primary_doc in zip(recent_filings['accessionNumber'], recent_filings['primaryDocument']):
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_num.replace('-','')}/{primary_doc}"
            filing_urls.append(filing_url)
            
        return filing_urls
        
    except Exception as e:
        print(f"공시 URL 가져오기 실패: {str(e)}")
        return None

def get_filing_urls_by_type(ticker, filing_type, limit=10):
    """
    특정 종목의 티커와 공시 유형을 입력받아 해당하는 공시 URL들을 가져옵니다.
    
    Args:
        ticker (str): 종목 티커 심볼
        filing_type (str): 공시 유형 (예: '8-K')
        limit (int): 가져올 URL의 최대 개수 (기본값: 10)
        
    Returns:
        list: 해당 유형의 공시 URL 리스트
    """
    # 먼저 티커에 해당하는 CIK 값을 찾습니다
    df = get_company_tickers()
    if df is None:
        return None
        
    company_info = df[df['ticker'] == ticker.upper()]
    if company_info.empty:
        print(f"티커 {ticker}에 해당하는 기업을 찾을 수 없습니다.")
        return None
        
    cik = company_info['cik'].iloc[0]
    
    # SEC API에서 공시 메타 정보를 가져옵니다
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'data.sec.gov'
    }
    
    try:
        # API 요청 전 딜레이
        time.sleep(0.1)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # 공시 URL 리스트 생성
        filing_urls = []
        recent_filings = data['filings']['recent']
        
        # form 타입과 accessionNumber, primaryDocument를 매칭하여 URL 생성
        for form_type, acc_num, primary_doc in zip(
            recent_filings['form'],
            recent_filings['accessionNumber'],
            recent_filings['primaryDocument']
        ):
            if form_type == filing_type:
                filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_num.replace('-','')}/{primary_doc}"
                filing_urls.append(filing_url)
                if len(filing_urls) >= limit:
                    break
            
        return filing_urls
        
    except Exception as e:
        print(f"공시 URL 가져오기 실패: {str(e)}")
        return None

def extract_filing_text(url):
    """
    공시 URL에서 텍스트 내용을 추출합니다.
    'Item'으로 시작하는 부분부터 본문으로 간주하여 추출합니다.
    
    Args:
        url (str): 공시 URL
        
    Returns:
        str: 추출된 텍스트 내용
    """
    headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    }
    
    try:
        # API 요청 전 딜레이
        time.sleep(0.1)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 모든 텍스트 추출
        text = soup.get_text()
        
        # 'Item'으로 시작하는 부분 찾기
        item_index = text.find('Item')
        if item_index != -1:
            # 'Item' 이후의 텍스트만 추출
            text = text[item_index:]
        
        # 문장 단위로 분리하고 띄어쓰기 추가
        # 마침표, 물음표, 느낌표 뒤에 공백 추가
        text = re.sub(r'([.!?])([A-Z])', r'\1 \2', text)
        
        # 여러 줄의 공백을 하나의 공백으로 변환
        text = re.sub(r'\s+', ' ', text)
        
        # 문장 시작 부분의 공백 제거
        text = re.sub(r'^\s+', '', text)
        
        # 문장 끝 부분의 공백 제거
        text = re.sub(r'\s+$', '', text)
        
        # 특수문자 처리
        text = re.sub(r'([,;:])([A-Za-z])', r'\1 \2', text)
        
        # 괄호 주변에 공백 추가
        text = re.sub(r'([(])([A-Za-z])', r'\1 \2', text)
        text = re.sub(r'([A-Za-z])([)])', r'\1 \2', text)
        
        # 숫자와 단위 사이에 공백 추가
        text = re.sub(r'(\d)([A-Za-z])', r'\1 \2', text)
        
        return text
        
    except Exception as e:
        print(f"텍스트 추출 실패: {str(e)}")
        return None

def load_form_types():
    """
    form_type.xlsx 파일에서 공시 유형 정보를 로드합니다.
    
    Returns:
        dict: 공시 유형 정보를 담은 딕셔너리
    """
    try:
        df = pd.read_excel('form_type.xlsx')
        form_types = df.to_dict('records')
        return form_types
    except Exception as e:
        print(f"공시 유형 파일 로드 실패: {str(e)}")
        return None

def classify_filing_with_gpt(text, form_types):
    """
    GPT를 사용하여 공시 텍스트를 분류합니다.
    
    Args:
        text (str): 분류할 공시 텍스트
        form_types (list): 공시 유형 정보 리스트
        
    Returns:
        tuple: (분류된 report_item 값, 분류 이유)
    """
    # OpenAI 클라이언트 초기화
    client = OpenAI()
    
    # 시스템 메시지 생성
    system_message = f"""
You are an expert at classifying SEC filings into predefined categories.
Below is the list of possible report_item codes and their descriptions:
{json.dumps(form_types, indent=2, ensure_ascii=False)}

Analyze the given filing text and return two things:
1. The report_item code (e.g. "1.2.1") that best matches the content
2. A brief explanation (1-2 sentences) of why you chose this classification

Format your response exactly like this:
CODE: [report_item code]
REASON: [your brief explanation]

Discard any irrelevant boilerplate and focus on the core event.
"""

    try:
        # GPT API 호출
        response = client.chat.completions.create(
            model="gpt-4.1-mini-2025-04-14",
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": text}
            ],
            temperature=0.3,  # 더 결정적인 응답을 위해 낮은 temperature 사용
            max_tokens=150    # 이유 설명을 위해 토큰 수 증가
        )
        
        # 응답에서 report_item 값과 이유 추출
        response_text = response.choices[0].message.content.strip()
        code_match = re.search(r'CODE:\s*([\d.]+)', response_text)
        reason_match = re.search(r'REASON:\s*(.+?)(?=\n|$)', response_text)
        
        if code_match and reason_match:
            classification = code_match.group(1)
            reason = reason_match.group(1).strip()
            return classification, reason
        else:
            print("GPT 응답 형식이 올바르지 않습니다.")
            return None, None
        
    except Exception as e:
        print(f"GPT 분류 실패: {str(e)}")
        return None, None

def save_filings_to_csv(ticker, filing_type="8-K", limit=10):
    """
    특정 종목의 공시 내용을 CSV 파일로 저장합니다.
    
    Args:
        ticker (str): 종목 티커 심볼
        filing_type (str): 공시 유형 (기본값: '8-K')
        limit (int): 저장할 공시의 최대 개수 (기본값: 10)
    """
    # 공시 URL 가져오기
    filing_urls = get_filing_urls_by_type(ticker, filing_type, limit)
    
    if not filing_urls:
        print(f"{ticker}의 {filing_type} 공시를 찾을 수 없습니다.")
        return
    
    # 공시 유형 정보 로드
    form_types = load_form_types()
    if not form_types:
        print("공시 유형 정보를 로드할 수 없습니다.")
        return
    
    print(form_types)
    # CSV 파일 생성
    csv_filename = f'{ticker}_{filing_type}_filings.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'URL', 'Text', 'Classification', 'Reason'])
        
        # 각 공시에 대해 텍스트 추출 및 분류
        for url in filing_urls:
            print(f"처리 중: {url}")
            text = extract_filing_text(url)
            if text:
                # GPT로 분류
                classification, reason = classify_filing_with_gpt(text, form_types)
                writer.writerow([ticker, url, text, classification, reason])
                print(url)
                print(f"분류: {classification}")
                print(f"이유: {reason}")
                print()
    
    print(f"\n{ticker}의 {filing_type} 공시 내용이 {csv_filename} 파일로 저장되었습니다.")

if __name__ == "__main__":
    # 데이터 가져오기
    df = get_company_tickers()

    if df is not None:
        # 데이터 확인
        print("데이터 샘플:")
        print(df.head())
        print("\n데이터 정보:")
        print(df.info())
        
        # Parquet 파일로 저장
        df.to_parquet('cik_tickers.parquet', index=False, engine='pyarrow')
        print("\n데이터가 cik_tickers.parquet 파일로 저장되었습니다.")
        
        # CSV 파일로 저장
        df.to_csv('cik_tickers.csv', index=False, encoding='utf-8')
        print("데이터가 cik_tickers.csv 파일로 저장되었습니다.")
        
        # 특정 종목의 8-K 공시 내용을 CSV로 저장
        ticker = "NVDA"  # 예시로 Apple의 티커 사용
        save_filings_to_csv(ticker, "8-K") 