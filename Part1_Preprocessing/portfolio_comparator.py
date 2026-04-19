import pandas as pd
import os
import glob

def calculate_portfolio_overlap(etf_df, pdf_folder_path):
    print("\n⏳ [4단계] 선도-후발 ETF 간 구성종목 일치율 계산을 시작합니다...")
    
    # 1. PDF 폴더 내의 모든 파일 목록 가져오기 (csv와 xlsx 모두 지원)
    pdf_files = glob.glob(os.path.join(pdf_folder_path, "*.*"))
    valid_extensions = ['.csv', '.xlsx', '.xls']
    
    file_map = {}
    for file in pdf_files:
        filename = os.path.basename(file)
        ext = os.path.splitext(filename)[1].lower()
        
        if ext in valid_extensions and '_' in filename:
            code = filename.split('_')[1].replace(ext, '')
            file_map[code] = file
            
    print(f"  ▶ PDF 폴더에서 총 {len(file_map)}개의 구성종목 파일을 찾았습니다.")

    # 2. 각 ETF별 구성종목 세트를 불러오는 내부 함수
    def get_portfolio_set(etf_code):
        if not etf_code.startswith('A'):
            etf_code = 'A' + etf_code
            
        if etf_code not in file_map:
            return None
            
        file_path = file_map[etf_code]
        ext = os.path.splitext(file_path)[1].lower()
        
        try:
            if ext == '.csv':
                try:
                    pdf_df = pd.read_csv(file_path, encoding='cp949', skiprows=5)
                except:
                    pdf_df = pd.read_csv(file_path, encoding='utf-8', skiprows=5)
            else: 
                pdf_df = pd.read_excel(file_path, skiprows=5)
            
            target_col = '구성종목코드'
            
            if target_col in pdf_df.columns:
                valid_codes = pdf_df[target_col].dropna().astype(str).tolist()
                return set(valid_codes)
            else:
                return None
        except Exception as e:
            return None

    # 3. 비교 결과 저장을 위한 리스트 및 진행 상황 카운터
    results = []
    pair_count = 0  # 현재 처리한 쌍의 개수를 세는 변수

    # 4. 섹터별로 그룹화하여 선도(Leader)와 후발(Follower) 비교
    valid_df = etf_df[etf_df['선도여부'].isin(['Leader', 'Follower'])]
    grouped = valid_df.groupby('섹터분류')

    print("  🏃‍♂️ 일치율 계산을 진행 중입니다... (100개 단위로 진행 상황을 안내합니다)")

    for sector, group in grouped:
        leader_row = group[group['선도여부'] == 'Leader']
        
        if leader_row.empty or len(group) < 2:
            continue
            
        leader_code = str(leader_row.iloc[0]['코드'])
        leader_name = leader_row.iloc[0]['코드명']
        leader_portfolio = get_portfolio_set(leader_code)
        
        if not leader_portfolio:
            continue

        followers = group[group['선도여부'] == 'Follower']
        
        for _, follower_row in followers.iterrows():
            follower_code = str(follower_row['코드'])
            follower_name = follower_row['코드명']
            follower_portfolio = get_portfolio_set(follower_code)
            
            if not follower_portfolio:
                continue
                
            # 일치율 계산
            intersection = len(leader_portfolio.intersection(follower_portfolio))
            union = len(leader_portfolio.union(follower_portfolio))
            
            jaccard_sim = (intersection / union) * 100 if union > 0 else 0
            copy_ratio = (intersection / len(follower_portfolio)) * 100 if len(follower_portfolio) > 0 else 0
            
            results.append({
                '섹터분류': sector,
                '선도_코드': leader_code,
                '선도_ETF명': leader_name,
                '후발_코드': follower_code,
                '후발_ETF명': follower_name,
                '선도_종목수': len(leader_portfolio),
                '후발_종목수': len(follower_portfolio),
                '겹치는_종목수': intersection,
                '자카드_유사도(%)': round(jaccard_sim, 2),
                '카피율(%)': round(copy_ratio, 2)
            })
            
            # ✨ 진행 상황 알림 로직 (100개 처리할 때마다 출력)
            pair_count += 1
            if pair_count % 100 == 0:
                print(f"    ... {pair_count}개의 카피캣 쌍 비교 완료! (현재 진행 섹터: {sector})")

    # 5. 결과를 DataFrame으로 변환
    result_df = pd.DataFrame(results)
    if not result_df.empty:
        print(f"  ✅ 모든 계산 완료! 총 {len(result_df)}쌍의 선도-후발 ETF 비교를 마쳤습니다.")
    else:
        print("  ⚠️ 일치율을 계산할 수 있는 ETF 쌍이 없습니다.")
        
    return result_df