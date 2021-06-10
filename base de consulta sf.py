import pandas as pd
import numpy as np
import pandas.io.sql as psql
from tabulate import tabulate
from mysql.connector import Error
from json import JSONEncoder
import ast
from maluforce import Maluforce
from simple_salesforce import Salesforce, SalesforceLogin, SFType


#Conectar ao salesforce
sf_maluforce = Maluforce(username='username', password='password', security_token='security_token')

#Colocar os casos
querysoql = """SELECT Id, CaseNumber, Account.CNPJ_Limpo__c, Stonecode__c FROM Case WHERE CaseNumber in ('')"""

#NÃO MEXE

response = sf_maluforce.query(querysoql)
lstrecords = response.get('records')
nextrecordsurl = response.get('nextrecordsurl')

while not response.get('done'):
    response = sf_maluforce.query_more(nextrecordsurl, identifier_is_url=True)
    lstrecords.extend(response.get('records'))
    nextrecordsurl = response.get('nextRecordsUrl')

df_records = pd.DataFrame(lstrecords)

dfaccount = df_records['Account'].apply(pd.Series).drop(labels='attributes', axis=1, inplace=False)
dfaccount.columns = ('Account.{0}'.format(name) for name in dfaccount.columns)

df_records.drop(labels=['Account', 'attributes'], axis=1, inplace=True)

dfOpptyacct = pd.concat([df_records, dfaccount], axis=1)
dfOpptyacct.to_excel('Arquivo_Novo.xlsx', index=False)

print('Arquivo gerado')

records = sf_maluforce.search('FIND {United Oil Installations} RETURNING Opportunity (Id, Name, StageName)')
