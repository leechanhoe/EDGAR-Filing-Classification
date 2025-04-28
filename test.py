import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def get_naver_stock_posts(code: str, pages: int = 1):
    """
    네이버 증권 종목토론방에서 게시물 정보를 수집합니다.
    
    Parameters:
        code (str): 종목 코드 (예: 삼성전자 '005930')
        pages (int): 크롤링할 페이지 수 (기본값: 1)
        
    Returns:
        pandas.DataFrame: 출처, 글 제목, 본문, 작성 일시(날짜), 조회수
    """
    base_url = "https://finance.naver.com/item/board.naver"
    posts = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for page in range(1, pages + 1):
        params = {'code': code, 'page': page}
        
        try:
            res = requests.get(base_url, params=params, headers=headers)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "lxml")

            # 게시글 목록이 있는 테이블 찾기
            table = soup.find('table', {'class': 'type2'})
            if not table:
                continue

            rows = table.find_all('tr')[2:]  # 첫 두 행은 헤더이므로 제외
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5:
                    continue

                # 게시글 링크 추출
                title_tag = cols[1].find('a')
                if not title_tag:
                    continue

                href = title_tag.get('href')
                post_url = f"https://finance.naver.com{href}"

                # 상세 페이지 요청
                try:
                    detail_res = requests.get(post_url, headers=headers)
                    detail_res.raise_for_status()
                    detail_soup = BeautifulSoup(detail_res.text, "lxml")

                    # 제목 추출
                    title = detail_soup.find('strong', {'class': 'c p15'})
                    title = title.get_text(strip=True) if title else "제목 없음"

                    # 본문 추출
                    content_div = detail_soup.find('div', {'id': 'body', 'class': 'view_se'})
                    if content_div:
                        # 본문에서 불필요한 요소 제거
                        for element in content_div.find_all(['script', 'style', 'iframe']):
                            element.decompose()
                        content = content_div.get_text(strip=True)
                    else:
                        content = "본문을 찾을 수 없습니다."

                    # 날짜 추출
                    date = detail_soup.find('th', {'class': 'gray03 p9 tah'})
                    date = date.get_text(strip=True) if date else "날짜 정보 없음"

                    # 조회수 추출
                    views = detail_soup.find('span', {'class': 'tah p11'})
                    views = views.get_text(strip=True) if views else "조회수 정보 없음"

                    posts.append({
                        "source": "네이버 종목토론방",
                        "title": title,
                        "content": content,
                        "date": date,
                        "views": views
                    })
                    
                    # 서버 부하를 줄이기 위해 잠시 대기
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"게시글 {post_url} 처리 중 오류 발생: {str(e)}")
                    continue

        except Exception as e:
            print(f"페이지 {page} 처리 중 오류 발생: {str(e)}")
            continue

    return pd.DataFrame(posts)

# 예시 실행
if __name__ == "__main__":
    df = get_naver_stock_posts("005930", pages=2)
    # CSV 파일로 저장
    # df.to_csv('naver_stock_posts.csv', index=False, encoding='utf-8-sig')
    df.to_parquet('naver_stock_posts.parquet', index=False, engine='pyarrow')
    print("데이터가 naver_stock_posts.csv 파일로 저장되었습니다.")
    print(df.head())