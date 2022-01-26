#!/usr/bin/env python
# coding: utf-8

# In[1]:


geo='DE'  
#['AD', 'AL', 'AM', 'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES', 'FI', 'FR', 'GE', 'HR', 'HU', 'IE'x, 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'ME', 'MT', 'NL', 'NO', 'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'UK']
age_ranges=[ 'Y_LT10', 'Y10-19', 'Y20-29', 'Y30-39', 'Y40-49', 'Y50-59', 'Y60-69', 'Y70-79', 'Y80-89', 'Y_GE90', 'TOTAL']
sex='T'

base_year=2020
current_year=2022
years=range(base_year-8,current_year+1)

deathsfile='demo_r_mwk_10.tsv.gz'#Deaths by week, sex and 10-year age groups
popfile='demo_pjan.tsv.gz'#Population on 1 January by age and sex
eurostat_base_url='https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/'

DEBUG=True


# In[2]:


import os
import csv
import datetime
import sys
import gzip
import certifi
import urllib3
import io
from matplotlib import pyplot as plt
plt.rcParams['axes.formatter.useoffset'] = False

http=urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where()) 

def open_eurostat_file(filename,localpath=os.getcwd()+'/Data/',remotepath='https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/'):
  if not os.path.exists(localpath+filename):
    if globals().get('DEBUG'): print(f'Downloading "{remotepath+filename}" from Eurostat.')
    with http.request('GET',remotepath+filename,preload_content=False) as r:
      if globals().get('DEBUG'): print(f'Saving "{localpath+filename}" locally.')
      with open(localpath+filename,'wb') as f:
        f.write(r.read())
  if globals().get('DEBUG'): print(f'Loading local "{localpath+filename}".')
  return gzip.open(localpath+filename,'rt',encoding='utf8')

def extract_eurostat_data(file='demo_r_mwk_10.tsv.gz', filter={'geo':'DE','sex':'T'}, fields=('age','year','week','value')):
  with open_eurostat_file(file) as f:
    Table=csv.DictReader(f,delimiter='\t')
    rowparamkeys=(Table.fieldnames[0].split('\\')[0]).split(',')
    rowparamkeyindex={key:i for i,key in enumerate(rowparamkeys)}
    filterset=set((rowparamkeyindex[key],value) for key,value in filter.items())
    for rec in Table:
#      fields_={key:value for key,value in zip(rowparamkeys,rec[Table.fieldnames[0]].split(','))}
      rowparams=rec[Table.fieldnames[0]].split(',')
#      if sum([fields_[key]!=value for key,value in filter.items()])==0:
      if filterset.issubset(set(enumerate(rowparams))):#filter for rows that match the filter
        fields_=dict(zip(rowparamkeys,rowparams))
        for key,value in rec.items(): #iterate through all columns of the table
         if key!=Table.fieldnames[0]: #skip the first column with the row parameters
            val=value.split()[0]
            if val.isnumeric():
              fields_['value']=int(val)
            else:
#              fields_['value']=0
              continue #skip if there is no value in the row/column
            if 'W' in key:
              year,week=key.split('W') #split the column heading in year and week of year
            else:
              year=key
              week=0
            fields_['year']=int(year)
            fields_['week']=int(week)
#            val=value.split()[0]
#            fields_['value']=int(val) if val.isnumeric() else 0
            yield(fields_[key] for key in fields)

def extract_eurostat_Fieldvalues(file='demo_r_mw.tsv.gz',):
  with open_eurostat_file(file) as f:
    Table=csv.DictReader(f,delimiter='\t')
    fieldnames=Table.fieldnames[0].split('\\')[0].split(',')
    fieldindex={f:i for i,f in enumerate(fieldnames)}
    fieldvalue={fieldname:set() for fieldname in fieldnames}
    for r in Table:
      fields=r[Table.fieldnames[0]].split(',')
      for fieldname,field in zip(fieldnames,fields):
        fieldvalue[fieldname].add(field)
  return fieldvalue

# In[3]:


deaths_w={}
for age,year,week,val in extract_eurostat_data(deathsfile,filter={'geo':geo,'sex':sex}, fields=('age','year','week','value')):
  if not age in deaths_w:
    deaths_w[age]=dict([])
  if not year in deaths_w[age]:
    deaths_w[age][year]=[0]*54
  if week<54:
   deaths_w[age][year][week]=val
  else:
    if week!=99:
      print(week)
      
deaths_a={age:{year:sum(weeks) for year,weeks in years.items()} for age,years in deaths_w.items()}#sum over weeks


# In[4]:


pop={}#sum up the population into age-groups that match the age groups in the deaths database
for age,year,val in extract_eurostat_data(popfile,filter={'geo':geo,'sex':sex}, fields=('age','year','value')):
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
  if not age in pop:
    pop[age]=dict([])
  if not year in pop[age]:
    pop[age][year]=0
  pop[age][year]+=val


# In[5]:
max_year=max(deaths_a['TOTAL'])
min_year=min(deaths_a['TOTAL'])
years=range(max(min_year,years[0]),min(max_year,years[-1])+1)
print(f'max_year:{max_year}, min_year:{min_year}')

mort={}
for age in age_ranges:
  mort[age]={}
  for year in years:
    try:
      x=deaths_a[age][year]/pop[age][year-1]
      mort[age][year]=x   
    except KeyError:
      continue

mort['age-adj 2020']={}
for year in years:
  try:
    s=0
    for age in age_ranges[:-1]:
      s+=deaths_a[age][year]/pop[age][year-1]*pop[age][base_year-1]
    x=s/pop["TOTAL"][base_year-1]
    mort['age-adj 2020'][year]=x
  except KeyError:
    continue
mortyears=list(mort['age-adj 2020'].keys())
print(f'{"age-range|years:":16s}{"".join([f" {y:6d}" for y in mortyears])}')      
for age,row in mort.items():
  print(f'{age:16s}',end=' ')   
  print(''.join([f'{mort[age].get(y,0)*100.0:6.3f} ' for y in mortyears]))


# In[6]:


#get_ipython().run_line_magic('matplotlib', 'inline')
plt.close()
plt.ylim(0,0.02)
plt.ylabel('Total Age-Adjusted Mortality [1/year]')
plt.xlabel('Year')
plt.title(f'Mortality {geo}')
#if mortyears[-1]==current_year:
plt.plot(list(mort['age-adj 2020'].keys())[:],list(mort['age-adj 2020'].values())[:],'b')
#else:
#  plt.plot(mortyears[:],list(mort['age-adj 2020'].values())[:],'b')
plt.show()


# In[7]:


plt.close()
for year,weeks in deaths_w['TOTAL'].items() :
    if (not (year in mortyears)) or (mort['TOTAL'][year]==0):
        continue
    last_week=len(weeks)
    while weeks[last_week-1]==0 and last_week>0:
      last_week-=1
    age_adj=mort['age-adj 2020'][year]/mort['TOTAL'][year]
#    age_adj=1.0
    plt.plot([age_adj*y/pop["TOTAL"][year-1]*365.25/7 for y in weeks[1:last_week]], 
     **(  
          dict(c='r',lw=2,zorder=1,) if year==2021 
     else dict(c='b',lw=2,zorder=1,) if year==2020 
     else dict(c='k',lw=1,zorder=0,) 
       ),
        label=f'{year}')
plt.ylabel('Total Age-Adjusted Mortality [1/year]')
plt.xlabel('Week of Year')
plt.title(f'Seasonal Mortality {geo}')
plt.legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0.0)
plt.ylim(bottom=0)
plt.xlim(right=52)
plt.subplots_adjust(right=0.82)# make space for legend to the right
#plt.savefig('test.pdf')
plt.show()

# In[8]:

#check if calculated weekly and annual mortalities add up:
year=mortyears[-2]
age_adj=mort['age-adj 2020'][year]/mort['TOTAL'][year]
print(mort['age-adj 2020'][year])
print(sum([age_adj*deaths/pop["TOTAL"][year-1]*365.25/7 for deaths in deaths_w['TOTAL'][year][1:]])/(365.25/7))


# In[ ]:




