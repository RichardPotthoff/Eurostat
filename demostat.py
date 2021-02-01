import os
import csv
import datetime
import sys
import gzip
import certifi
import urllib3
import io
from matplotlib import pyplot as plt

http=urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where()) 

def open_eurostat_file(filename,localpath=os.getcwd()+'/Data/',remotepath='https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/'):
  if not os.path.exists(localpath+filename+'.gz'):
    r=http.request('GET',remotepath+filename+'.gz')
    s=r.data
    with open(localpath+filename+'.gz','wb') as f:
      f.write(s)
  else:
    with open(localpath+filename+'.gz','rb') as f:
      s=f.read()
  return io.StringIO(gzip.decompress(s).decode('utf8'))

def extract_eurostat_data(file='demo_r_mwk_10.tsv', filter={'geo':'NL','sex':'T'}, fields=('age','year','week','value')):
  with open_eurostat_file(file) as f:
    Table=csv.DictReader(f,delimiter='\t')
    rowparamkeys=(Table.fieldnames[0].split('\\')[0]).split(',')
    for rec in Table:
      fields_={key:value for key,value in zip(rowparamkeys,rec[Table.fieldnames[0]].split(','))}
      if sum([fields_[key]!=value for key,value in filter.items()])==0:
        for key,value in rec.items():
          if key!=Table.fieldnames[0]:
            if 'W' in key:
              year,week=key.split('W')
            else:
              year=key
              week=0
            fields_['year']=int(year)
            fields_['week']=int(week)
            val=value.split()[0]
            fields_['value']=int(val) if val.isnumeric() else 0
            yield(fields_[key] for key in fields)
            
db_={}
for age,year,week,val in extract_eurostat_data('demo_r_mwk_10.tsv',filter={'geo':'NL','sex':'T'}, fields=('age','year','week','value')):
  if not age in db_:
    db_[age]=dict([])
  if not year in db_[age]:
    db_[age][year]=[0]*54
  if week<54:
   db_[age][year][week]=val
  else:
    if week!=99:
      print(week)
      
db={age:{year:sum(weeks) for year,weeks in years.items()} for age,years in db_.items()}#sum over weeks

db1={}
for age,year,val in extract_eurostat_data('demo_pjan.tsv',filter={'geo':'NL','sex':'T'}, fields=('age','year','value')):
  if age=='Y_LT1':
    age='Y0'
  if age=='Y_OPEN':
    age='Y_GE90'
  if age[:1]=='Y':
    if (age[1:]).isnumeric():
      age=int(age[1:])
      if age<10:
        age='Y_LT10'
      elif age>=90:
        age='Y_GE90'
      else:
        age=f'Y{age//10*10:2d}-{age//10*10+9:2d}'
  if not age in db1:
    db1[age]=dict([])
  if not year in db1[age]:
    db1[age][year]=0
  db1[age][year]+=val
  

age_ranges=[ 'Y_LT10', 'Y10-19', 'Y20-29', 'Y30-39', 'Y40-49', 'Y50-59', 'Y60-69', 'Y70-79', 'Y80-89', 'Y_GE90', 'TOTAL']

base_year=2020
years=range(base_year-8,base_year+1)

print(f'{"age-range":15s}{"".join([f" {y:6d}" for y in years])}')
for age in age_ranges:
  print(f'{age:15s}',end=' ')
  for year in years:
    print(f'{db[age][year]/db1[age][year-1]*100.0:6.3f}',end=' ')
  print()     

print(f'{"age-adj 2020":15s}',end=' ')   
for year in years:
  s=0
  for age in age_ranges[:-1]:
    s+=db[age][year]/db1[age][year-1]*db1[age][base_year-1]
  print(f'{s/db1["TOTAL"][base_year-1]*100.0:6.3f}',end=' ')
print()        

#plt.ylim(65,100)
#plt.xlim(datetime.date(2006,1,1),datetime.date(2022,1,1))
#plt.scatter(list(db.keys()),list(db.values()),color='r',marker='.')
#plt.show()
