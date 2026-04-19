import pandas as pd

def identify_leader_etf(df):
    print("\n⏳ [3단계] 섹터별 선도(Leader) ETF 식별을 시작합니다...")
    
    # 1. 분석 대상 추리기
    # '기타/분류불가'와 '시장대표/...' 그룹은 선도 ETF 분석 대상에서 제외합니다.
    valid_mask = (df['섹터분류'] != '기타/분류불가') & (~df['섹터분류'].str.startswith('시장대표/', na=False))
    
    # 분석 대상(순수 테마)과 비대상(기타/시장대표)을 분리
    df_target = df[valid_mask].copy()
    df_others = df[~valid_mask].copy()
    
    if df_target.empty:
        print("⚠️ 선도 ETF를 식별할 테마형 데이터가 없습니다.")
        return df
        
    # 2. 상장일을 숫자형 또는 datetime으로 변환하여 비교 가능하게 만듦
    # (예: 20201014 같은 숫자로 되어있다고 가정)
    df_target['상장일_비교용'] = pd.to_numeric(df_target['상장일'], errors='coerce')
    
    # 3. 정렬 기준 설정
    # 1순위: 섹터분류 (가나다순)
    # 2순위: 상장일 (오름차순, 빠른 날짜가 먼저 오도록)
    # 3순위: 기존 인덱스 (엑셀 원본 기준 윗줄에 있던 것 우선)
    # 원본 인덱스 유지를 위해 임시 열 생성
    df_target['원본_순서'] = df_target.index 
    
    df_sorted = df_target.sort_values(
        by=['섹터분류', '상장일_비교용', '원본_순서'], 
        ascending=[True, True, True]
    )
    
    # 4. Leader / Follower 태깅
    # 기본값은 모두 'Follower' (카피캣)로 설정
    df_sorted['선도여부'] = 'Follower'
    
    # 각 섹터그룹('섹터분류') 내에서 첫 번째 행(상장일이 가장 빠른 행)의 인덱스를 찾음
    # sort_values를 이미 했으므로 .first() 또는 .idxmin() 개념과 동일하게 맨 위가 Leader임
    leader_indices = df_sorted.groupby('섹터분류').head(1).index
    
    # 찾은 첫 번째 행들의 '선도여부'를 'Leader'로 변경
    df_sorted.loc[leader_indices, '선도여부'] = 'Leader'
    
    # 불필요한 임시 열 삭제
    df_sorted = df_sorted.drop(columns=['상장일_비교용', '원본_순서'])
    
    # 5. 분리해두었던 데이터(기타/시장대표)와 다시 합치기
    # 기타/시장대표는 선도여부 열을 비워둡니다(None)
    df_others['선도여부'] = None 
    
    # 두 데이터프레임을 위아래로 결합
    final_df = pd.concat([df_sorted, df_others], ignore_index=True)
    
    # 보기 좋게 섹터분류 기준으로 전체 정렬 (기타 항목은 맨 아래로)
    final_df = final_df.sort_values(by=['섹터분류', '선도여부', '상장일'], ascending=[True, False, True])
    
    print("  ✅ 선도(Leader) 및 카피캣(Follower) 태깅 완료")
    
    return final_df