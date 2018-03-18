from core.utils.process import Big
import pandas as pd
import time
import mysql.connector
import sys

col_scores = {}
col_names = {'name': None, 'gender': None, 'age': None, 'city': None, 'speciality': None, 'mobile': None, 'email': None, 'type': None, 'subtype': None,'pcode': None}

fmt = sys.argv[1]
path = sys.argv[2]
odf = None
if fmt == 'excel':
    odf = pd.read_csv(path)
elif fmt == 'sql':
    con = mysql.connector.connect(user='root',
                                  password='idhant',
                                  host='127.0.0.1',
                                  database='cipla')
    odf = pd.read_sql_query('select * from {} LIMIT 500'.format(path), con=con)

x = int(input('Are there two name fields? 0 or 1 - '))
if x == 1:
    odf['names'] = ''
    names = []
    y = int(input('how many are there? (num) -'))
    for i in range(y):
        names.append(input('enter {} name field -'.format(i)))
    for name in names:
        odf['names'] = odf['names'] + odf[name] + ' '

for col in col_names.keys():
    if x == 1:
        col_names[col] = 'names'
        col_scores[col] = int(input('enter the {} score - '.format(col)))
        x = 0
        continue
    x = input('enter the column name for {} - '.format(col))
    if x is not '':
        col_names[col] = x
        col_scores[col] = int(input('enter the {} score - '.format(x)))
x = input('please enter unique field name - ')
if x is not '':
    col_names['id'] = x
x = input('enter the name of file...  -')
startTime = time.time() * 1000
result = Big.process_dataframe(odf, col_scores, col_names)
odf['matchwith'] = ''
writer = pd.ExcelWriter('results/{}.xlsx'.format(str(x)))
added = []

for gender in result:
    for p in gender:
        for pos1 in p.keys():
            for pos2 in p[pos1]:
                odf.at[pos1, 'matchwith'] = odf.at[pos1, 'matchwith'] + pos2 + ' '

odf.to_excel(writer, 'sheet1')
writer.save()
print('complete ', time.time() * 1000 - startTime)
