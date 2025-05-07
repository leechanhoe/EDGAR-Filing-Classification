import pandas as pd
import re

input_file = 'a.csv'
output_file = 'b.csv'

df = pd.read_csv(input_file)

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

# 새로운 DataFrame 생성 및 저장
split_df = pd.DataFrame(rows)
split_df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f'분리된 데이터가 {output_file}에 저장되었습니다.')
