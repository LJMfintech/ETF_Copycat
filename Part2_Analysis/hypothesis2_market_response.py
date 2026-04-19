import pandas as pd
import numpy as np
import os
import warnings
from scipy import stats

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

def process_fnguide_timeseries(file_path, value_name):
    try:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            try:
                df_raw = pd.read_csv(file_path, header=None, encoding='cp949', low_memory=False)
            except:
                df_raw = pd.read_csv(file_path, header=None, encoding='utf-8-sig', low_memory=False)
        else:
            df_raw = pd.read_excel(file_path, header=None)
            
        codes = df_raw.iloc[8, 1:].values
        df_data = df_raw.iloc[14:].copy()
        df_data.columns = ['날짜'] + list(codes)
        
        df_data = df_data.dropna(subset=['날짜'])
        df_data['날짜'] = pd.to_datetime(df_data['날짜'], errors='coerce')
        
        df_melted = df_data.melt(id_vars=['날짜'], var_name='종목코드', value_name=value_name)
        df_melted['종목코드'] = df_melted['종목코드'].astype(str).apply(lambda x: x if x.startswith('A') else 'A' + x)
        
        df_melted[value_name] = pd.to_numeric(df_melted[value_name], errors='coerce')
        df_melted = df_melted.dropna(subset=[value_name])
        
        return df_melted
        
    except Exception as e:
        print(f"  ⚠️ 전처리 오류 ({os.path.basename(file_path)}): {e}")
        return None

def analyze_market_response(overlap_path, raw_dir):
    # 1. 일치율 결과 로드
    overlap_df = pd.read_csv(overlap_path, encoding='utf-8-sig')
    
    # 2. 파일 로드 (csv, xlsx 둘 다 호환)
    file_aum = os.path.join(raw_dir, "ETF_5.csv") if os.path.exists(os.path.join(raw_dir, "ETF_5.csv")) else os.path.join(raw_dir, "ETF_5.xlsx")
    file_vol = os.path.join(raw_dir, "ETF_3.csv") if os.path.exists(os.path.join(raw_dir, "ETF_3.csv")) else os.path.join(raw_dir, "ETF_3.xlsx")
    
    df_aum = process_fnguide_timeseries(file_aum, 'AUM')
    df_vol = process_fnguide_timeseries(file_vol, '거래량')
    
    if df_aum is None or df_vol is None:
        print("❌ 데이터를 불러오지 못했습니다.")
        return
        
    # 3. 병합 및 일평균 계산
    ts_merged = pd.merge(df_aum, df_vol, on=['날짜', '종목코드'], how='outer')
    avg_stats = ts_merged.groupby('종목코드').agg({'AUM': 'mean', '거래량': 'mean'}).reset_index()
    
    # 4. 통계값 매칭 (평균)
    result_df = pd.merge(overlap_df, avg_stats, left_on='선도_코드', right_on='종목코드', how='left')
    result_df.rename(columns={'AUM': '선도_평균AUM', '거래량': '선도_평균거래량'}, inplace=True)
    result_df.drop(columns=['종목코드'], inplace=True)
    
    result_df = pd.merge(result_df, avg_stats, left_on='후발_코드', right_on='종목코드', how='left')
    result_df.rename(columns={'AUM': '후발_평균AUM', '거래량': '후발_평균거래량'}, inplace=True)
    result_df.drop(columns=['종목코드'], inplace=True)
    
    final_df = result_df.dropna(subset=['선도_평균AUM', '후발_평균AUM', '선도_평균거래량', '후발_평균거래량']).copy()
    
    # ==========================================================
    # 🌟 NEW: 개별 ETF 쌍(Pair)마다 p-value 계산하기
    # ==========================================================
    # 날짜별로 매칭된 시계열 데이터가 필요하므로 ts_merged에서 데이터를 뽑아 계산합니다.
    
    p_values_aum = []
    p_values_vol = []
    
    for _, row in final_df.iterrows():
        lead_code = row['선도_코드']
        lag_code = row['후발_코드']
        
        # 선도와 후발 ETF의 시계열 데이터 추출
        lead_data = ts_merged[ts_merged['종목코드'] == lead_code].set_index('날짜')
        lag_data = ts_merged[ts_merged['종목코드'] == lag_code].set_index('날짜')
        
        # 날짜를 기준으로 병합 (두 ETF가 모두 존재하는 날짜만 추출)
        pair_data = lead_data.join(lag_data, lsuffix='_선도', rsuffix='_후발').dropna()
        
        # T-test 실행 (데이터가 너무 없거나 변동이 없는 에러 상황 방지)
        if len(pair_data) > 2:
            try:
                # AUM p-value
                _, p_aum = stats.ttest_rel(pair_data['AUM_선도'], pair_data['AUM_후발'])
                # 거래량 p-value
                _, p_vol = stats.ttest_rel(pair_data['거래량_선도'], pair_data['거래량_후발'])
            except:
                p_aum, p_vol = np.nan, np.nan
        else:
            p_aum, p_vol = np.nan, np.nan
            
        p_values_aum.append(p_aum)
        p_values_vol.append(p_vol)
        
    # 데이터프레임에 p-value 추가
    final_df['p-value(AUM)'] = p_values_aum
    final_df['p-value(거래량)'] = p_values_vol
    
    # ==========================================================
    # 5. 차이 계산 및 선점효과 판별 로직 고도화 (p-value < 0.05 반영)
    # ==========================================================
    final_df['AUM_격차(선도-후발)'] = final_df['선도_평균AUM'] - final_df['후발_평균AUM']
    final_df['거래량_격차(선도-후발)'] = final_df['선도_평균거래량'] - final_df['후발_평균거래량']
    
    # 선점효과 O : 선도 ETF가 수치도 크고(격차>0), 통계적으로 유의미해야(p<0.05) 함
    final_df['선점효과(AUM)'] = np.where(
        (final_df['AUM_격차(선도-후발)'] > 0) & (final_df['p-value(AUM)'] < 0.05), 'O', 'X'
    )
    final_df['선점효과(거래량)'] = np.where(
        (final_df['거래량_격차(선도-후발)'] > 0) & (final_df['p-value(거래량)'] < 0.05), 'O', 'X'
    )
    
    # 소수점 정리
    numeric_cols = ['선도_평균AUM', '후발_평균AUM', '선도_평균거래량', '후발_평균거래량', 'AUM_격차(선도-후발)', '거래량_격차(선도-후발)']
    for col in numeric_cols:
        final_df[col] = final_df[col].round(2)
        
    final_df['p-value(AUM)'] = final_df['p-value(AUM)'].round(4)
    final_df['p-value(거래량)'] = final_df['p-value(거래량)'].round(4)
        
    # 결과 저장
    output_path = os.path.join(os.path.dirname(overlap_path), "Hypothesis2_Result.csv")
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n💾 p-value가 포함된 개별 섹터/종목별 상세 비교 결과가 CSV로 저장되었습니다: {output_path}")

    # ==========================================================
    # 6. 전체 통계 및 섹터별 요약 출력
    # ==========================================================
    print("\n" + "="*50)
    print(" 📊 [가설 2] 시장 반응(AUM, 거래량) 요약 리포트")
    print("="*50)
    print(f" ✅ 분석 완료: 총 {len(final_df)}개 카피캣 쌍(Pair) 매칭됨\n")
    
    aum_t, aum_p = stats.ttest_rel(final_df['선도_평균AUM'], final_df['후발_평균AUM'], nan_policy='omit')
    aum_diff = (final_df['선도_평균AUM'] - final_df['후발_평균AUM']).mean()
    
    print(f" [1] 전체 순자산총액(AUM) 평균 비교")
    print(f"  - 선도 ETF: {final_df['선도_평균AUM'].mean():,.0f}  |  후발 ETF: {final_df['후발_평균AUM'].mean():,.0f}")
    print(f"  - 전체 p-value : {aum_p:.4f} ", end="")
    print("✨ (전체적으로 유의미한 선점효과 O)" if (aum_p < 0.05 and aum_diff > 0) else "(유의미한 차이 X)")
    
    print(f"\n [2] 전체 일평균 거래량 비교")
    vol_t, vol_p = stats.ttest_rel(final_df['선도_평균거래량'], final_df['후발_평균거래량'], nan_policy='omit')
    vol_diff = (final_df['선도_평균거래량'] - final_df['후발_평균거래량']).mean()
    print(f"  - 선도 ETF: {final_df['선도_평균거래량'].mean():,.0f}  |  후발 ETF: {final_df['후발_평균거래량'].mean():,.0f}")
    print(f"  - 전체 p-value : {vol_p:.4f} ", end="")
    print("✨ (전체적으로 유의미한 선점효과 O)" if (vol_p < 0.05 and vol_diff > 0) else "(유의미한 차이 X)")
    
    # 7. 섹터별 선점효과 승률 요약
    print("\n 🏢 [주요 섹터별 확실한 선점효과(p<0.05) 비율]")
    sector_summary = final_df.groupby('섹터분류')['선점효과(AUM)'].apply(lambda x: (x == 'O').mean() * 100)
    
    sector_counts = final_df['섹터분류'].value_counts()
    valid_sectors = sector_counts[sector_counts >= 3].index
    
    if len(valid_sectors) > 0:
        for sector in valid_sectors[:5]:
            print(f"  - {sector}: 선도 ETF가 승리한 비율 {sector_summary[sector]:.0f}%")
    else:
        print("  - 카피캣 쌍이 충분한(3개 이상) 섹터가 없습니다.")
        
    print("="*50 + "\n")

def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = os.path.dirname(CURRENT_DIR)
    
    PROCESSED_DIR = os.path.join(BASE_DIR, "Data", "Processed_Data")
    RAW_DIR = os.path.join(BASE_DIR, "Data", "Raw_Data")
    
    OVERLAP_FILE = os.path.join(PROCESSED_DIR, "04_ETF_Overlap_Results.csv")
    
    if os.path.exists(OVERLAP_FILE):
        analyze_market_response(OVERLAP_FILE, RAW_DIR)

if __name__ == "__main__":
    main()