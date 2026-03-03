from bs4 import BeautifulSoup as bs
import re
import pandas as pd
import datetime as dt
import time
import numpy as np
import sys




text_path = r"C:\Users\asvarun\OneDrive - Information Services Group (ISG)\Shashikala M's files - Varun CD Workspace\Forbes_2000_extraction\Path__text.txt"





with open(text_path, mode = "r") as f:
    count = 1
    textlist = []
    for lines in f:
        if count > 1:
            pos = lines.find("=:")
            textlist.append(lines[pos+2:].strip().strip("\""))
            
            count+=1
        count+=1

fetch_path = textlist[0]
base_path = textlist[1]


# --------------------------------------------------------------------------------------------
# Functions block
# --------------------------------------------------------------------------------------------

def extract_values(tablegroup):
    rank = []
    companyname = []
    industry = []
    hq = []
    sales = []
    profit =  []
    assets = []
    mv = []
    
    company = tablegroup.find_all("a", {"aria-label": True})
    for x in company:
        y = x.find_all("div", class_ = "row-cell-value")
        rank_ = y[0].text
        companyname_ = y[1].text
        HEADQUARTERS_ = y[2].text
        INDUSTRY_ = y[3].text
        SALES_ = y[4].text
        PROFIT_ = y[5].text
        ASSETS_ = y[6].text
        MARKETVALUE_ = y[7].text
        
        rank.append(rank_)
        companyname.append(companyname_)
        industry.append(INDUSTRY_)
        hq.append(HEADQUARTERS_)
        sales.append(SALES_)
        profit.append(PROFIT_)
        assets.append(ASSETS_)
        mv.append(MARKETVALUE_)

            
    return list(zip(rank, companyname, hq, industry, sales, profit, assets, mv))


# --------------------------------------------------------------------------------------------
# Functions block end
# --------------------------------------------------------------------------------------------


try:  
    with open(fetch_path, mode = 'r', encoding= "utf-8") as f:
        count = 1
        textlist = []
        
        for lines in f:
            
            if count > 2:

                textlist.append(lines)
                count+=1
                continue
            
            count+=1

except Exception as e:
    print(f"There is an error reading/accessing the HTML reference path, make sure the path is keyed in correctly in the code.\nError is:    {e}")
    time.sleep(15)
    sys.exit()


text = ''.join(textlist)
response_html = bs(text, features= "html.parser")

container = response_html.find_all("div", {"class": "table"})
container_tablegroup = []

for tablegroups in container:
    x = tablegroups.find_all("div", {"class": "table-row-group"})
    container_tablegroup.append(x)
    
outputlist = []
for companies in container_tablegroup:
    
    result = extract_values(companies[0])

    outputlist.extend(result)


df = pd.DataFrame(data = outputlist, columns = ['rank', 'companyname', 'hq', 'industry', 'sales', 'profit', 'assets', 'mv'])

df['rank'] = df['rank'].str.replace(",", "")
    
df.astype({"rank": "int32", "companyname": "str", "hq": "str", "industry": "str",
           "sales": "str", "profit": "str", "assets": "str", "mv": "str"})


#converting sales, assets, marketvalue(mv), profit columns to numeric from string
numericcols = ['sales', 'profit', 'assets', 'mv']

for cols in df.columns:
    
    if cols in numericcols:
        
        col_var = df[cols].str.replace(r"[\$,]", "", regex= True)
        
        conditions = [ col_var.str.contains("B", na = None),
                       col_var.str.contains("M", na = None),
                       col_var.str.contains("K", na = None) ]
        
        choices = [ (pd.to_numeric(col_var.str.extract(r"(\d+(?:\.\d+)?)")[0], errors= "coerce") * (10**9)).round(0),
                    (pd.to_numeric(col_var.str.extract(r"(\d+(?:\.\d+)?)")[0], errors= "coerce") * (10**6)).round(0),
                    (pd.to_numeric(col_var.str.extract(r"(\d+(?:\.\d+)?)")[0], errors= "coerce") * (10**3)).round(0) ]
        
        df[f"{cols}_converted"] = np.select(conditions, choices)



path = "\\".join([base_path, f"Forbes-2025_Python_{dt.datetime.now().date()}.csv"])


df.to_csv(path, index = False)

print(f"Success 🍻!!\nThe extracted Forbes data is now stored at: {path}")

time.sleep(15)
