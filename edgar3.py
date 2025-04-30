import pandas as pd
import re

input_file = '8k_raw_data_sample_100.csv'
output_file = '8k_raw_data_sample_100_split.csv'

df = pd.read_csv(input_file)

rows = []

for _, row in df.iterrows():
    item_numbers = [x.strip() for x in str(row['Item Numbers']).split(',') if x.strip()]
    item_desc_en = [x.strip() for x in str(row['Item Descriptions (EN)']).split(',') if x.strip()]
    item_desc_kr = [x.strip() for x in str(row['Item Descriptions (KR)']).split(',') if x.strip()]
    content_full = str(row['Content'])

    # 각 Item별로 Content를 분리
    item_spans = []
    for item in item_numbers:
        # 패턴: ITEM X.XX (대소문자 구분 없이, 공백 허용)
        match = re.search(r'(?i)(ITEM\s*' + re.escape(item) + r')', content_full)
        if match:
            item_spans.append((item, match.start()))
        else:
            item_spans.append((item, None))

    # 시작 위치 기준으로 정렬
    item_spans = sorted([s for s in item_spans if s[1] is not None], key=lambda x: x[1])

    for i, (item, start) in enumerate(item_spans):
        desc_en = item_desc_en[i] if i < len(item_desc_en) else ''
        desc_kr = item_desc_kr[i] if i < len(item_desc_kr) else ''
        if start is not None:
            if i + 1 < len(item_spans):
                end = item_spans[i+1][1]
            else:
                end = None
            content = content_full[start:end].strip() if end else content_full[start:].strip()
        else:
            content = ''
        rows.append({
            'Ticker': row['Ticker'],
            'URL': row['URL'],
            'Filing Date': row['Filing Date'],
            'Item Numbers': item,
            'Item Descriptions (EN)': desc_en,
            'Item Descriptions (KR)': desc_kr,
            'Content': content
        })

# 새로운 DataFrame 생성 및 저장
split_df = pd.DataFrame(rows)
split_df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f'분리된 데이터가 {output_file}에 저장되었습니다.')
