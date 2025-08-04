import pandas as pd
from datetime import datetime
import time
from playwright.sync_api import sync_playwright
import os

download_dir=r"C:\Sawy_Automation\Infenion Handling\excels"

def get_day_name():
    today=datetime.today()
    today_name=today.strftime("%A")
    return today_name

def read_input():
    day_name=get_day_name()
    df=pd.read_excel(r"C:\Sawy_Automation\Infenion Handling\Book2.xlsx",sheet_name=day_name)
    parts=df['Parts']
    parts=parts.drop_duplicates()
    return parts

def Download_Excele(part_number):
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=False)
        context=browser.new_context(accept_downloads=True)
        page=context.new_page()
        page.goto("https://www.infineon.com/search/eccn-hts-finder",timeout=5000)

        try:
            page.wait_for_selector("button:has-text-('Accept)",timeout=4000)
            page.click("button:has-text-('Accept')")
            print('Cookie popup accepted.')
        except:
            print("No cookie popup appeared.")

        try:
            page.wait_for_selector("button:has-text-('Confirm and continue')",timeout=4000)    
            page.click("button:has-text-('Confirm and continue')")
        except :
            pass    
        
        try:
            page.get_by_placeholder('Search by Sales name IFX, OPN, or SP number').fill(part_number)
            page.click("button:has-text('Search')")

            page.wait_for_selector("text=Search Results",timeout=10000)

            export_buttons = page.locator("div.export-box.downloadButton")
            count = export_buttons.count()
            print(f"[{part_number}] Found {count} export boxes")
            export_button = export_buttons.nth(1)
            export_button.wait_for(state="visible", timeout=10000)
            with page.expect_download() as download_info:
                export_button.scroll_into_view_if_needed()
                export_button.click(force=True)
            download = download_info.value
            filename = f"{part_number}.xlsx"
            filepath = os.path.join(download_dir, filename)
            download.save_as(filepath)
            print(f"✅ [{part_number}] Exported and saved as {filename}")

        except Exception as e:
            page.screenshot(path=f"{part_number}_error.png", full_page=True)
            print(f"❌ [{part_number}] Failed: {e}")

        page.close()
        browser.close()    
        
def append_excels(parts):
    files=[os.path.join(download_dir,f) for f in os.listdir(download_dir) if f.endswith('xlsx')]
    appended_data=pd.DataFrame()
    for file in files:
        print (file)
        data=pd.read_excel(file)
        data=data[['OPN','CoO']]
        appended_data=appended_data._append(data,ignore_index=True)        
    appended_data=appended_data.drop_duplicates()
    appended_data=appended_data.rename(columns={'OPN':'Part','CoO':'Original Company Name'})
    appended_data=appended_data[appended_data['Part'].isin(parts)]
    return appended_data

def offline_creation(df):
    df['Man_name']='Infineon Technologies AG'
    df['Man_code']='SMN'
    df['Online']='N/A'
    df['Parent']='N/A'
    df['FilePath']=f"{download_dir}\{df['Part']}.xlsx"
    df['Source']='Direct Feed'
    df['Type']='XLSX'
    df['task name']='Mfg_Sites_Daily'
    df['team name']='Supply_Chain'
    df['Document Type']='N/A'
    desired_sequence=['Man_name','Man_code','Online','Parent','FilePath','Source','Type','task name','team name','Document Type']
    df = df[desired_sequence]
    return df

def to_mongo(df, download_dir):
    df = df.copy()

    df['Vendor'] = 'Infineon Technologies AG'
    df['Source Type'] = 'Supplier Site'
    df['Effective Date'] = datetime.today().strftime('%m-%d-20%y')
    df['Core Type'] = 'Part Number'
    df['Original Core Value'] = df['Part']
    df['Core Value'] = df['Part']
    
    # Create Source path dynamically for each row
    df['Source'] = df['Part'].apply(lambda x: os.path.join(download_dir, f"{x}.xlsx"))

    df['Change Impact'] = 'New Value'
    df['Part Insertion Date'] = None
    df['Original Site Type'] = 'COO_Supplier Recommended'
    df['Site Type'] = 'COO_Supplier Recommended'
    df['Original Facility Name'] = ''
    df['Original Facility Scope'] = 'All Data'
    df['Priority'] = 'High'

    desired_sequence = [
        'Vendor', 'Source', 'Source Type', 'Effective Date', 'Core Type',
        'Original Core Value', 'Change Impact', 'Core Value', 'Part',
        'Part Insertion Date', 'Original Site Type', 'Site Type',
        'Original Company Name', 'Original Facility Name',
        'Original Facility Scope', 'Priority'
    ]

    df1 = df[desired_sequence]

    # Create df2 with modified Site Type and Original Company Name
    df2 = df1.copy()
    df2['Original Site Type'] = 'COO_Supplier Recommended'
    df2['Site Type'] = 'COO_Custom'
    df2['Original Company Name'] = ''

    # Combine both
    df_import_header = pd.concat([df1, df2], ignore_index=True)
    df_import_header=df_import_header.drop_duplicates()

    return df_import_header

        
    

append_excels(read_input())
df=to_mongo(append_excels(read_input()),download_dir)
df.to_csv(r'C:\Sawy_Automation\Infenion Handling\mongoo.txt',sep='\t',encoding='utf-8',index=False)