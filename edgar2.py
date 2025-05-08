import requests
import pandas as pd
import json
import time
from bs4 import BeautifulSoup
import re
import csv
from datetime import datetime, timedelta
import os

def get_company_tickers():
    """
    SEC의 company_tickers.json 파일에서 기업 정보를 가져옵니다.
    
    Returns:
        pandas.DataFrame: CIK, 티커, 종목명 정보를 담은 데이터프레임
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',
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

def get_recent_8k_filings(cik, ticker, one_month_ago):
    """
    특정 기업의 최근 한달간의 8-K 공시 URL들을 가져옵니다.
    
    Args:
        cik (str): 기업의 CIK 번호
        ticker (str): 기업의 티커 심볼
        one_month_ago (datetime): 한달 전 날짜
        
    Returns:
        list: (티커, URL, 공시일자) 튜플의 리스트
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'data.sec.gov'
    }
    
    try:
        time.sleep(0.1)  # SEC API 요청 제한 준수
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        filing_urls = []
        recent_filings = data['filings']['recent']
        
        # form 타입, accessionNumber, primaryDocument, filingDate를 매칭
        for form_type, acc_num, primary_doc, filing_date in zip(
            recent_filings['form'],
            recent_filings['accessionNumber'],
            recent_filings['primaryDocument'],
            recent_filings['filingDate']
        ):
            if form_type == '8-K':
                filing_date = datetime.strptime(filing_date, '%Y-%m-%d')
                if filing_date >= one_month_ago:
                    filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_num.replace('-','')}/{primary_doc}"
                    filing_urls.append((ticker, acc_num, filing_url, filing_date.strftime('%Y-%m-%d')))
        
        return filing_urls
        
    except Exception as e:
        print(f"{ticker}의 공시 URL 가져오기 실패: {str(e)}")
        return []

def collect_all_recent_8k_filings(end_date=None):
    """
    전 종목의 최근 한달간의 8-K 공시 URL을 수집합니다.
    
    Args:
        end_date (datetime, optional): 기준 날짜. 기본값은 현재 날짜입니다.
        
    Returns:
        list: (티커, Accession Number, URL, 공시일자) 튜플의 리스트
    """
    # 기준 날짜 설정
    if end_date is None:
        end_date = datetime.now()
    
    # 한달 전 날짜 계산
    one_month_ago = end_date - timedelta(days=30)
    
    # 기업 정보 가져오기
    companies_df = get_company_tickers()
    if companies_df is None:
        return []
    
    # 결과를 저장할 리스트
    all_filings = []
    
    # 각 기업별로 8-K 공시 수집
    total_companies = len(companies_df)
    for idx, (_, company) in enumerate(companies_df.iterrows(), 1):
        print(f"처리 중: {company['ticker']} ({idx}/{total_companies})")
        filings = get_recent_8k_filings(company['cik'], company['ticker'], one_month_ago)
        all_filings.extend(filings)
    
    # 중복 검사 및 제거
    accnum_dict = {}
    duplicates = []
    unique_filings = []
    
    for filing in all_filings:
        ticker, acc_num, url, filing_date = filing
        if acc_num in accnum_dict:
            duplicates.append((ticker, acc_num, url, filing_date))
            print(f"중복 발견: {ticker} - {acc_num}")
            print(f"  기존: {accnum_dict[acc_num]}")
            print(f"  중복: {url}")
        else:
            accnum_dict[acc_num] = url
            unique_filings.append(filing)
    
    if duplicates:
        print(f"\n총 {len(duplicates)}개의 중복된 Accession Number가 발견되었습니다.")
        print("중복된 항목은 제외하고 저장됩니다.")
    
    print(f"\n총 {len(unique_filings)}개의 8-K 공시가 수집되었습니다.")
    return unique_filings

def extract_filing_content(url):
    """
    8-K 공시 URL에서 Item 유형과 본문을 추출합니다.
    
    Args:
        url (str): 공시 URL
        
    Returns:
        tuple: (Item 유형 리스트, 본문 텍스트)
    """
    headers = {
        'User-Agent': 'Chan Lee (klavier0823@gmail.com)',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    }
    
    try:
        time.sleep(0.1)  # SEC API 요청 제한 준수
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # windows-1252 인코딩으로 설정
        response.encoding = 'windows-1252'
            
        # HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser', from_encoding='windows-1252')
        
        # 불필요한 태그 제거
        for tag in soup.find_all(['script', 'style']):
            tag.decompose()
        
        # 텍스트 추출 및 정제
        paragraphs = []
        for tag in soup.stripped_strings:
            # 특수 문자 처리를 위한 정규식
            cleaned_text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', tag)
            # 연속된 공백을 하나로 변환
            # cleaned_text = re.sub(r'[ \t]+', ' ', cleaned_text)
            if cleaned_text.strip():
                paragraphs.append(cleaned_text.strip())
        
        # 줄바꿈을 유지하면서 텍스트 결합
        text = '\n\n'.join(paragraphs)
        
        # Item 패턴 찾기 (예: Item 1.01, Item 8.01 등) - 대소문자 구분 없이
        items = re.findall(r'(?i)item\s+(\d+\.\d+)', text)
        items = list(set(items))  # 중복 제거
        
        # 본문 추출
        # 첫 번째 Item이 나오는 위치 찾기 - 대소문자 구분 없이
        first_item_match = re.search(r'(?i)item\s+\d+\.\d+', text)
        if first_item_match:
            start_pos = first_item_match.start()
            
            # 모든 'SIGNATURES' 위치 찾기 - 대소문자 구분 없이
            signature_positions = [m.start() for m in re.finditer(r'(?i)signatures?', text[start_pos:])]
            
            if signature_positions:
                # 마지막 'SIGNATURES' 위치 사용
                end_pos = start_pos + signature_positions[-1]
                content = text[start_pos:end_pos].strip()
            else:
                content = text[start_pos:].strip()
                
            # 특수 문자 처리
            # content = content.encode('ascii', 'ignore').decode('ascii')
            content = re.sub(r'\s*\n\s*', '\n', content)  # 줄바꿈 주변 공백 제거
            content = clean_non_ascii_newlines(content)
        else:
            content = ""
        
        return items, content
        
    except Exception as e:
        print(f"공시 내용 추출 실패 ({url}): {str(e)}")
        return [], ""

def clean_non_ascii_newlines(text):
    """
    아스키코드가 아닌 값들은 가끔 앞or뒤에 줄바꿈이 있는 경우가 있어 줄바꿈 제거
    """
    # 앞뒤 줄바꿈이 하나도 없어도, 하나만 있어도, 여러 개 있어도 모두 매칭
    return re.sub(r'\n*([^\x00-\x7F])\n*', r'\1', text)

# Item 유형 매핑 정의
item_type_mapping_en = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.02": "Termination of a Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "1.04": "Mine Safety - Reporting of Shutdowns and Patterns of Violations",
    "1.05": "Material Cybersecurity Incidents",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement",
    "2.04": "Triggering Events That Accelerate or Increase a Direct Financial Obligation or an Obligation",
    "2.05": "Costs Associated with Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule or Standard; Transfer of Listing",
    "3.02": "Unregistered Sales of Equity Securities",
    "3.03": "Material Modification to Rights of Security Holders",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements or a Related Audit Report",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure of Directors or Certain Officers; Election of Directors; Appointment of Certain Officers",
    "5.03": "Amendments to Articles of Incorporation or Bylaws; Change in Fiscal Year",
    "5.04": "Temporary Suspension of Trading Under Registrant's Employee Benefit Plans",
    "5.05": "Amendments to the Registrant's Code of Ethics, or Waiver of a Provision of the Code of Ethics",
    "5.06": "Change in Shell Company Status",
    "5.07": "Submission of Matters to a Vote of Security Holders",
    "5.08": "Shareholder Director Nominations",
    "6.01": "ABS Informational and Computational Material",
    "6.02": "Change of Servicer or Trustee",
    "6.03": "Change in Credit Enhancement or Other External Support",
    "6.04": "Failure to Make a Required Distribution",
    "6.05": "Securities Act Updating Disclosure",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits"
}

item_type_mapping_kr = {
    "1.01": "중대한 확정 계약 체결",
    "1.02": "중대한 확정 계약 해지",
    "1.03": "파산 또는 법정관리",
    "1.04": "광산 안전 — 작업중단 및 위반 패턴 보고",
    "1.05": "중대한 사이버 보안 사고 발생",
    "2.01": "자산 인수 또는 처분 완료",
    "2.02": "영업실적 및 재무상태",
    "2.03": "직접 금융채무 또는 부외채무 발생",
    "2.04": "금융채무 또는 부외채무의 조기상환 트리거 발생",
    "2.05": "사업 철수 또는 처분 활동 관련 비용",
    "2.06": "중대한 자산손상 인식",
    "3.01": "상장폐지 통보 또는 상장유지 요건 미충족; 상장 이전 통보",
    "3.02": "미등록 주식 판매",
    "3.03": "주주 권리의 중대한 변경",
    "4.01": "감사인 변경",
    "4.02": "기존 재무제표나 감사·검토보고서에 대한 불신 선언",
    "5.01": "경영권 변경",
    "5.02": "이사 또는 주요 임원 사임, 선임, 보상계약 체결",
    "5.03": "정관 또는 내규 수정; 회계연도 변경",
    "5.04": "직원 복리후생 플랜 내 거래 일시 중단",
    "5.05": "윤리강령 수정 또는 예외 승인",
    "5.06": "페이퍼컴퍼니(Shell Company) 지위 변경",
    "5.07": "주주총회 안건 제출",
    "5.08": "주주에 의한 이사 후보 추천 관련 통지",
    "6.01": "ABS 관련 정보 및 계산자료 공시",
    "6.02": "서비스업자 또는 신탁관리인 변경",
    "6.03": "신용보강 또는 외부 지원 변경",
    "6.04": "필수 분배금 지급 실패",
    "6.05": "증권법 공시 업데이트",
    "7.01": "Regulation FD(공정공시) 공시",
    "8.01": "기타 사건 공시",
    "9.01": "재무제표 및 첨부자료 제출"
}

def process_filings(filings):
    """
    수집된 공시 데이터를 처리하여 Item 유형과 본문을 추출하고 Item별로 분리합니다.
    
    Args:
        filings (list): (티커, Accession Number, URL, 공시일자) 튜플의 리스트
        
    Returns:
        pandas.DataFrame: 처리된 데이터를 담은 DataFrame
    """
    try:
        # DataFrame 생성
        df = pd.DataFrame(filings, columns=['Ticker', 'Accession Number', 'URL', 'Filing Date'])
        total_rows = len(df)
        
        # Item 유형과 본문을 저장할 새로운 컬럼 추가
        df['Item Numbers'] = ''
        df['Item Descriptions (EN)'] = ''
        df['Item Descriptions (KR)'] = ''
        df['Content'] = ''
        
        # 각 URL에 대해 Item 유형과 본문 추출
        for idx, row in df.iterrows():
            print(f"처리 중: {idx + 1}/{total_rows}")
            items, content = extract_filing_content(row['URL'])
            
            # Item 번호와 설명을 저장
            item_numbers = ', '.join(sorted(items))
            item_descriptions_en = ', '.join(item_type_mapping_en.get(item, "Unknown") for item in sorted(items))
            item_descriptions_kr = ', '.join(item_type_mapping_kr.get(item, "알 수 없음") for item in sorted(items))
            
            df.at[idx, 'Item Numbers'] = item_numbers
            df.at[idx, 'Item Descriptions (EN)'] = item_descriptions_en
            df.at[idx, 'Item Descriptions (KR)'] = item_descriptions_kr
            df.at[idx, 'Content'] = content
        
        # Item별로 행 분리
        rows = []
        for _, row in df.iterrows():
            # 각 필드 분리 (쉼표+공백 조합도 고려)
            item_numbers = [x.strip() for x in str(row['Item Numbers']).split(',') if x.strip()]
            item_desc_en = [x.strip() for x in str(row['Item Descriptions (EN)']).split(',') if x.strip()]
            item_desc_kr = [x.strip() for x in str(row['Item Descriptions (KR)']).split(',') if x.strip()]
            content_full = str(row['Content'])
            
            # Content를 Item별로 분리
            item_contents = {}
            current_item = None
            current_content = []
            
            # Content를 줄 단위로 분리
            lines = content_full.split('\n')
            for line in lines:
                # Item 패턴 찾기 (대소문자 구분 없이)
                for item in item_numbers:
                    pattern = f'item\\s*{item}'
                    if re.search(pattern, line.lower()):
                        if current_item and current_content:
                            item_contents[current_item] = '\n'.join(current_content)
                        current_item = item
                        current_content = [line]
                        break
                else:
                    if current_item:
                        current_content.append(line)
            
            # 마지막 Item의 content 저장
            if current_item and current_content:
                item_contents[current_item] = '\n'.join(current_content)

            # 각 Item별로 행 생성
            for i, item in enumerate(item_numbers):
                desc_en = item_desc_en[i] if i < len(item_desc_en) else ''
                desc_kr = item_desc_kr[i] if i < len(item_desc_kr) else ''
                content = item_contents.get(item, '')
                
                rows.append({
                    'Ticker': row['Ticker'],
                    'URL': row['URL'],
                    'Filing Date': row['Filing Date'],
                    'Accession Number': row['Accession Number'],
                    'Item Numbers': item,
                    'Item Descriptions (EN)': desc_en,
                    'Item Descriptions (KR)': desc_kr,
                    'Content': content
                })
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        print(f"처리 중 오류 발생: {str(e)}")
        return None

if __name__ == "__main__":
    # 현재 날짜 기준으로 공시 수집
    end_date = datetime.now()
    filings = collect_all_recent_8k_filings(end_date)
    
    if filings:
        # 수집된 공시 처리
        result_df = process_filings(filings)
        
        if result_df is not None:
            # 최종 결과를 CSV 파일로 저장
            output_file = f'8k_filings_{end_date.strftime("%Y%m%d")}.csv'
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n처리된 데이터가 {output_file}에 저장되었습니다.")
    else:
        print("처리할 8-K 파일을 찾을 수 없습니다.")
