import pandas as pd
from thefuzz import process, fuzz
import time
import os

# --- ตั้งค่าชื่อไฟล์ ---
# วางไฟล์ Excel ไว้ที่เดียวกับไฟล์โปรแกรมนี้
INPUT_FILE = 'data_migration.xlsx' 
OUTPUT_FILE = 'Match_Result_Final.xlsx'

def heavy_clean(text):
    if not isinstance(text, str): return ""
    # 1. แปลงเป็นตัวเล็ก ลบจุด ลบช่องว่าง
    text = text.replace(".", "").replace(" ", "").lower()
    # 2. ลบคำนำหน้า/ต่อท้ายที่มักพิมพ์ไม่เหมือนกัน
    bad_words = ["บริษัท", "บจก", "จำกัด", "หจก", "บมจ", "คุณ", "หสน", "นาง", "นาย", "ร้าน"]
    for word in bad_words:
        text = text.replace(word, "")
    return text

def run_migration():
    # ตรวจสอบว่ามีไฟล์ต้นฉบับไหม
    if not os.path.exists(INPUT_FILE):
        print(f"❌ ไม่พบไฟล์ {INPUT_FILE} ในโฟลเดอร์นี้!")
        return

    print("📖 กำลังโหลดข้อมูลจาก Excel...")
    try:
        # โหลดแยก Sheet ตามที่คุณเตรียมไว้
        oracle_df = pd.read_excel(INPUT_FILE, sheet_name='Oracle')
        sap_df = pd.read_excel(INPUT_FILE, sheet_name='SAP')
    except Exception as e:
        print(f"❌ โหลดข้อมูลไม่สำเร็จ: {e}")
        return

    # เตรียม Search Key (รวม Name 1 + 2)
    print("🧹 กำลัง Clean ข้อมูลและเตรียม Search Key...")
    oracle_df['Full_Name'] = oracle_df['Name1'].fillna('') + " " + oracle_df['Name2'].fillna('')
    oracle_df['Search_Key'] = oracle_df['Full_Name'].apply(heavy_clean)

    sap_df['Full_Name'] = sap_df['Name1'].fillna('') + " " + sap_df['Name2'].fillna('')
    sap_df['Search_Key'] = sap_df['Full_Name'].apply(heavy_clean)

    # เก็บลิสต์ของ SAP Search Key ไว้ในตัวแปรเดียวเพื่อความเร็ว
    sap_choices = sap_df['Search_Key'].tolist()

    results = []
    total = len(oracle_df)
    start_time = time.time()

    print(f"🚀 เริ่มค้นหา 5 อันดับที่ใกล้เคียงที่สุด (Total: {total} รายการ)...")

    for i, o_row in oracle_df.iterrows():
        # แสดงความคืบหน้าทุกๆ 50 รายการ
        if i % 50 == 0 and i > 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / i
            remaining = avg_time * (total - i)
            print(f"✅ ทำไปแล้ว {i}/{total} | ใช้เวลา {elapsed/60:.1f} นาที | ประมาณการเวลาที่เหลือ {remaining/60:.1f} นาที")

        # ค้นหา Top 5 (ใช้ token_sort_ratio เพื่อให้ "ลุมพินี เพลส" แมตช์กับ "เพลส ลุมพินี")
        top_5 = process.extract(o_row['Search_Key'], sap_choices, scorer=fuzz.token_sort_ratio, limit=5)

        res = {
            'Oracle_ID': o_row['ID'],
            'Oracle_Name': o_row['Full_Name']
        }

        # วนลูปเก็บผลลัพธ์ 5 อันดับ
        for j, (match_str, score) in enumerate(top_5):
            # หา index ของ match_str ใน sap_choices
            idx = sap_choices.index(match_str)
            sap_row = sap_df.iloc[idx]
            res[f'Match_{j+1}_BP_Number'] = sap_row['BP_Number']
            res[f'Match_{j+1}_SAP_Name'] = sap_row['Full_Name']
            res[f'Match_{j+1}_Score'] = score
            
        results.append(res)

    # บันทึกผลลัพธ์
    print("💾 กำลังบันทึกผลลัพธ์ลงไฟล์...")
    pd.DataFrame(results).to_excel(OUTPUT_FILE, index=False)
    
    end_time = time.time()
    print(f"✨ เสร็จสิ้น! ใช้เวลาทั้งหมด {(end_time - start_time)/60:.2f} นาที")
    print(f"📂 ผลลัพธ์อยู่ที่ไฟล์: {OUTPUT_FILE}")

if __name__ == "__main__":
    run_migration()