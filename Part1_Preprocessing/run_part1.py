import pandas as pd
import os
from data_filter import filter_domestic_equity
from sector_classifier import classify_etf_sectors
from leader_identifier import identify_leader_etf
from portfolio_comparator import calculate_portfolio_overlap

def main():
    # ==========================================
    # 1. 동적 파일 및 폴더 경로 설정 (폴더 구조 반영)
    # ==========================================
    # 기준(Base) 경로 설정 (폴더명 'ETF_Copycat'으로 변경)
    BASE_DIR = r"C:\Users\82104\Desktop\ETF_Copycat"
    
    # [입력] 원본 데이터 경로
    INPUT_FILE = os.path.join(BASE_DIR, "Data", "Raw_Data", "ETF_List.xlsx")
    PDF_FOLDER = r"C:\Users\82104\Desktop\ETF_Data\ETF_PDF"  # 구성종목 PDF 폴더는 바탕화면의 기존 경로 유지
    
    # [출력] 결과물을 저장할 Processed_Data 폴더 경로
    PROCESSED_DIR = os.path.join(BASE_DIR, "Data", "Processed_Data")
    
    # Processed_Data 폴더가 없으면 파이썬이 자동으로 생성함
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)
        print(f"📁 결과물 저장 폴더가 생성되었습니다: {PROCESSED_DIR}")
    
    # 출력 파일 상세 경로 지정
    STEP1_OUTPUT = os.path.join(PROCESSED_DIR, "01_ETF_Filtered.csv")
    STEP2_OUTPUT = os.path.join(PROCESSED_DIR, "02_ETF_Classified.csv")
    STEP3_OUTPUT = os.path.join(PROCESSED_DIR, "03_ETF_Final_with_Leaders.csv")
    FINAL_OVERLAP_OUTPUT = os.path.join(PROCESSED_DIR, "04_ETF_Overlap_Results.csv")
    
    try:
        # ==========================================
        # 2. 데이터 파이프라인 실행 (1~4단계)
        # ==========================================
        print(f"📂 원본 데이터 로드 중: {INPUT_FILE}")
        raw_df = pd.read_excel(INPUT_FILE)
        
        # 1단계: 필터링
        filtered_df = filter_domestic_equity(raw_df)
        filtered_df.to_csv(STEP1_OUTPUT, index=False, encoding='utf-8-sig')
        
        # 2단계: 섹터 분류
        classified_df = classify_etf_sectors(filtered_df)
        classified_df.to_csv(STEP2_OUTPUT, index=False, encoding='utf-8-sig')
        
        # 3단계: 선도/후발 식별
        leader_df = identify_leader_etf(classified_df)
        leader_df.to_csv(STEP3_OUTPUT, index=False, encoding='utf-8-sig')
        
        # 4단계: 일치율 계산
        overlap_df = calculate_portfolio_overlap(leader_df, PDF_FOLDER)
        
        if overlap_df is not None and not overlap_df.empty:
            overlap_df.to_csv(FINAL_OVERLAP_OUTPUT, index=False, encoding='utf-8-sig')
            print(f"\n💾 1파트(전처리 및 일치율 분석) 완료!")
            print(f"   ▶ 결과 파일이 저장되었습니다: {FINAL_OVERLAP_OUTPUT}")
            
    except Exception as e:
        print(f"\n❌ 실행 중 오류가 발생했습니다. 파일 경로가 맞는지, 엑셀 파일이 열려있지 않은지 확인하세요: {e}")

if __name__ == "__main__":
    main()