# coding=utf-8
import csv,codecs
import xlsxwriter

def saveBrandXlsx(data, fileName):
    #fileName = time.strftime('%Y-%m-%d',time.localtime(time.time()))+'_profileTest.xls'
    workbook = xlsxwriter.Workbook(fileName)
    sheet = workbook.add_worksheet('sheet1')

    line = 0
    columns = ['name', 'lookname','count']
    for i, col in enumerate(columns):
        sheet.write(line, i, col)

    for name,lookname,count in data:
        line +=1
        sheet.write(line, 0, name)
        sheet.write(line, 1, lookname)
        sheet.write(line, 2, count)

    workbook.close()
           


