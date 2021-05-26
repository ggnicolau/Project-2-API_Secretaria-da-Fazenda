#!/usr/bin/env python
# coding: utf-8

# In[4]:


#Run all this first
# Libraries
import requests
import pycurl
import os
import json
import re
import pandas as pd
from tqdm import tqdm
import sqlalchemy as db
#SQL Server [rename it as you need]
db_server = 'postgresql'
user = 'postgres'
password = 'admin'
ip = 'localhost'
db_name = 'fazenda'
# create the engine
engine = db.create_engine(f'{db_server}://{user}:{password}@{ip}/{db_name}')
#Type filter (will use it later)
types_df = ['int64', 'float64', "object", 'int64', "object", 'float64', "object", 'int64', 'int64', 'float64', "object", 'int64', 'int64', 'int64', 'int64', "object", "object", "object", "object", 'int64', "object", "object", "object", 'int64', "object", "object", "object", 'float64', 'int64', "object", 'float64', 'int64', "object", 'float64', "object", 'float64', 'int64', 'int64', "object", 'float64', "object", "object", "object",'int64', 'int64'] 


# In[7]:


pip install multiprocess


# # Push (don't need to run, just if want to check)

# ## With Curl

# In[2]:


#buffer = StringIO()
#c = pycurl.Curl()
#c.setopt(c.URL, 'https://gatewayapi.prodam.sp.gov.br:443/financas/orcamento/sof/v3.0.1/programas?codPrograma=3005&anoExercicio=2021')
#c.setopt(pycurl.HTTPHEADER, ("Accept: application/json", "Authorization: Bearer cd91fcdee240b8ad9ad5aa2335b2526"))
#c.perform()
#c.close()
#body = buffer.getvalue()
#buffer
#curl -X GET --header "Accept: application/json" --header "Authorization: Bearer cd91fcdee240b8ad9ad5aa2335b2526" "https://gatewayapi.prodam.sp.gov.br:443/financas/orcamento/sof/v3.0.1/programas?codPrograma=3005&anoExercicio=2021"


# ## With Requests

# In[64]:


#url = 'https://gatewayapi.prodam.sp.gov.br:443/financas/orcamento/sof/v3.0.1/empenhos?anoEmpenho=2019&mesEmpenho=10&numPagina=800'
#headers = {"Accept": "application/json", "Authorization": "Bearer cd91fcdee240b8ad9ad5aa2335b2526"}
#response = requests.get(url, headers = headers)


# In[65]:


#response.json()


# # Parallel

# ### If it's your first page, create it here

# In[8]:


#Create your first page without Parallel
from multiprocess import Pool
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
    #from concurrent.futures import ProcessPoolExecutor
import threading
import time

#get_first_page
    # 1) Make your profile on https://api.prodam.sp.gov.br/store/;
    # 2) Generate the link you want on https://api.prodam.sp.gov.br/store/apis/info?name=SOF&version=v3.0.1&provider=admin;
    # 3) put your Authorization key below and edit the link to the one you got:
list1 = []
for i in tqdm(range(1)):
    for j in tqdm(range(1,13)):
        url = f'https://gatewayapi.prodam.sp.gov.br:443/financas/orcamento/sof/v3.0.1/empenhos?anoEmpenho=2017&mesEmpenho={j}&codOrgao=84&numPagina=' + str(i+1)
        headers = {"Accept": "application/json", "Authorization": "Bearer cd91fcdee240b8ad9ad5aa2335b2526"}
        response = requests.get(url, headers = headers)
        list1.append(response.json())

#Make Dataframe            
df_final = pd.DataFrame()
for k in range(len(list1)):
    df_temp = pd.DataFrame(list1[k]['lstEmpenhos'])
    for i in range(len(df_temp.columns)):
        df_temp[df_temp.columns[i]] = df_temp[df_temp.columns[i]].astype(types_df[i])
    df_final = df_final.append(df_temp)

#Add column Page
df_final['pagina'] = df_final.apply(lambda _: '', axis=1)
df_final['pagina'] = 1
df_final

#Send to SQL
conn = engine.connect()
df_final.to_sql('saude_2017', con = conn, index = False, if_exists = 'append')
df_sql = pd.read_sql('saude_2017', con = conn)[['pagina', 'mesEmpenho', 'anoEmpenho', 'codOrgao']]
conn.close()
df_sql


# In[ ]:


#Check DataFrame
#b = pd.DataFrame(list1[0]['lstEmpenhos'])
#b


# In[ ]:


#Check Types 1
#for i in range(len(b.columns)):
#    b[b.columns[i]] = b[b.columns[i]].astype(types_df[i])
#x


# In[ ]:


#Check Types 2
#mask = (pd.DataFrame(list1[0]['lstEmpenhos'], dtype = types_df) == pd.DataFrame(list1[1]['lstEmpenhos'])).sum() < 50
#pd.DataFrame(list1[0]['lstEmpenhos']).loc[:,mask].info()


# In[ ]:


#Check types 3
#pd.DataFrame(list1[1]['lstEmpenhos']).dtypes.values


# In[22]:


#Define function to Create your Table, just run it!
#def create_your_table(x):
#    conn = engine.connect()
#    df_final.to_sql(x, con = conn, index = False, if_exists = 'append')
#    conn.close()


# In[24]:


#name your table and send it to DataBase
#create_your_table("your_table_name_here")


# ### Go for it!

# In[9]:


#Import Libraries
from multiprocess import Pool
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
    #from concurrent.futures import ProcessPoolExecutor
import threading
import time

#get_data function
    #Change your URL and Headers, go to API STORE SOF to find what you need
def get_data(pair):
    try:
        i, j = pair
        url = f'https://gatewayapi.prodam.sp.gov.br:443/financas/orcamento/sof/v3.0.1/empenhos?anoEmpenho=2017&mesEmpenho={j}&codOrgao=84&numPagina={i+1}'
        headers = {"Accept": "application/json", "Authorization": "Bearer cd91fcdee240b8ad9ad5aa2335b2526"}
        response = requests.get(url, headers = headers)
        pages = pd.DataFrame(response.json()['lstEmpenhos'])
        if len(pages) > 0:
            pages['pagina'] = i
            return pages
        else:
            return response.json()
    except Exception as e: 
        return e

#Define pages
def num_pages(x):
    for i in (range(x)):
    #if want to select interval
#        if x < 370:
#            pass
        for j in (range(1,13)):
            parameters.append((i, j))


# In[10]:


# Variables
list1 = []
parameters = []
pool = ThreadPoolExecutor(max_workers=6)
#Define pages
num_pages(488)


# In[45]:


#if want to select parameters use this:
#parameters[::12]


# In[11]:


# Check duplicates with your SQL Table
conn = engine.connect()
df_sql = pd.read_sql('saude_2017', con = conn)[['pagina', 'mesEmpenho', 'anoEmpenho', 'codOrgao']]
conn.close()
df_sql


# In[12]:


duplicates = list(df_sql.apply(lambda x : (int(x['pagina']), int(x['mesEmpenho'])), axis = 1).unique())
parameters2 = list(filter(lambda x : x not in duplicates, tqdm(parameters)))


# In[ ]:


# Sound alert: insert whatever audio file you want below
from IPython.display import Audio, display
def allDone():
    display(Audio(url='https://sound.peal.io/ps/audios/000/000/537/original/woo_vu_luvub_dub_dub.wav', autoplay=True))
#Run parallel
list1 = list(tqdm(pool.map(get_data, tqdm(parameters2))))
allDone()


# In[48]:


#If needed, test list1
#[i for i in list1 if isinstance(i,dict)]


# In[16]:


#Create you final Dataframe
df_final = pd.DataFrame()
for df_temp in list1:
    try:
        for i in range(len(df_temp.columns)):
            df_temp[df_temp.columns[i]] = df_temp[df_temp.columns[i]].astype(types_df[i])
        df_final = df_final.append(df_temp)
    except:
        print(df_temp)
df_final
allDone()


# In[17]:


# append to SQL
conn = engine.connect()
df_final.to_sql('saude_2018', con = conn, index = False, if_exists = 'append')
conn.close()
allDone()


# # Filter your table

# In[9]:


#Call DataFrame from SQL
conn = engine.connect()
df_final = pd.read_sql('as_social', con = conn)


# In[10]:


df_final


# In[7]:


df_final['txtRazaoSocial'] = df_final['txtRazaoSocial'].str.replace('SPDM ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA', 'SPDM - ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA')


# In[13]:


df_final['txtRazaoSocial'] = df_final['txtRazaoSocial'].str.replace('SPDM ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA', 'SPDM - ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA')
df_final['txtRazaoSocial'] = df_final['txtRazaoSocial'].str.replace('SPDM ASSOCIAÇÃO  PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA', 'SPDM - ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA')
df_final['txtRazaoSocial'] = df_final['txtRazaoSocial'].str.replace('SPDM -  ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA', 'SPDM - ASSOCIAÇÃO PAULISTA PARA O DESENVOLVIMENTO DA MEDICINA')
df_final['txtRazaoSocial'] = df_final['txtRazaoSocial'].str.replace('APOIOASSOCIACAO DE AUXILIO MUTUO DA REGIAO LESTE', 'APOIO ASSOCIACAO DE AUXILIO MUTUO DA REGIAO LESTE')
df_final['txtRazaoSocial'] = df_final['txtRazaoSocial'].str.replace('APOIO-ASSOCIAÇÃO DE AUXILIO MUTUO DA REGIÃO LESTE', 'APOIO ASSOCIACAO DE AUXILIO MUTUO DA REGIAO LESTE')


# In[ ]:


#Filter
# Remember NOT to SUM all values; if you want final execution payment select only 12th month 


# In[14]:


# Upload to SQL
conn = engine.connect()
df_final.to_sql('ass_social2', con = conn, index = False, if_exists = 'append')
conn.close()


# # EXCEL_FILE

# In[58]:


xl = pd.ExcelFile("C:/Users/user/Downloads/basedadosexecucaoConsolidados.xlsx")


# In[59]:


xl.sheet_names
[u'_2003Em_Diante']
df = xl.parse("_2003Em_Diante")
df.head()
df.describe


# In[62]:


orgaos = [7,8,35,75,84,86, 87,89, 90, 91,93,96,99,'07','08']
df2= df[df.Cd_Orgao.isin(orgaos)]


# In[63]:


df2.describe


# In[64]:


df2


# In[ ]:




