import pandas as pd
import dataSetting as ds
import checkNode as cn

df = pd.read_excel('inputExcel/Sample_Excel.xlsx')
df = df.fillna('')
excelList = df.to_dict('records')

for excelData in excelList:
    data = ds.dataSetting(excelData)
    cn.makeXml(data)


print("=== 작업 종료 ===")

