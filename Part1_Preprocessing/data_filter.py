import pandas as pd

def filter_domestic_equity(df):
    print("\n⏳ [1단계] 국내주식형 선별, 상장일 필터링, 불필요 테마 제외를 시작합니다...")
    
    # 1. '유형분류(대)'가 '국내주식형'인 항목만 필터링
    if '유형분류(대)' in df.columns:
        df_filtered = df[df['유형분류(대)'] == '국내주식형'].copy()
        print(f"  ✅ '국내주식형' 선별 완료: {len(df_filtered)}건 남음")
    else:
        print("  ⚠️ '유형분류(대)' 컬럼을 찾을 수 없습니다. 엑셀 파일의 열 이름을 확인하세요.")
        return df

    # 2. 상장일 필터링 (2025년 12월 31일 이전 상장 종목만 선별)
    # 이미지 H열 '상장일' 컬럼 (예: 20021014)
    if '상장일' in df_filtered.columns:
        # 결측치를 '0'으로 채우고 안전하게 문자열로 변환
        df_filtered['상장일_str'] = df_filtered['상장일'].fillna(0).astype(str)
        
        # 엑셀에서 숫자로 인식되어 소수점(.0)이 붙거나 하이픈(-)이 있을 경우 제거
        df_filtered['상장일_str'] = df_filtered['상장일_str'].str.replace('-', '', regex=False)
        df_filtered['상장일_str'] = df_filtered['상장일_str'].str.replace('.0', '', regex=False)
        
        # 20251231 이하인 데이터만 남기기 (즉, 2026년 상장 종목 제외)
        df_filtered = df_filtered[df_filtered['상장일_str'] <= '20251231'].copy()
        
        # 비교용으로 만들었던 임시 열은 깔끔하게 삭제
        df_filtered = df_filtered.drop(columns=['상장일_str'])
        print(f"  ✅ 2025년 12월 31일 이전 상장 ETF 선별 완료: {len(df_filtered)}건 남음")
    else:
        print("  ⚠️ '상장일' 컬럼을 찾을 수 없어 날짜 필터링을 건너뜁니다.")

    # 3. 제외할 키워드 설정
    exclude_keywords = [
        '인버스', '레버리지', '액티브', '고배당', '배당', '커버드콜', 
        '리츠', 'REITs', '채권', '혼합'
    ]
    
    # 제외 키워드를 포함하지 않는 항목만 남기기
    pattern = '|'.join(exclude_keywords)
    df_filtered = df_filtered[~df_filtered['코드명'].str.contains(pattern, case=False, na=False)].copy()
    
    print(f"  ✅ 불필요한 테마(인버스/액티브 등) 제외 완료: {len(df_filtered)}건 남음")
    
    return df_filtered