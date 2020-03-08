#!/usr/bin/env python
# coding: utf-8
import jellyfish
import os
import numpy as np
import pandas as pd
import requests as r
import re
from string import digits
import sqlalchemy as sa

import access_to_db


URL_API = 'https://api.hh.ru/vacancies'     # ссылка для доступа к API
RECORDS_PER_PAGE = 100                      # записей на странице
MAX_PAGES = 20                              # максимальное количество страниц


okpdtr_id_name = pd.read_csv(os.path.join('tables_hh', 'id_okpdtr_okpdtr.csv'),
                             dtype={
                                 'id': object,
                                 'name': object
                                 })
id_okpdtr_lst = okpdtr_id_name['id'].tolist()
okpdtr_lst = okpdtr_id_name['name'].tolist()
okpdtr_lst_raw = okpdtr_lst.copy()

for i in range(len(okpdtr_lst)):
    okpdtr_lst[i] = re.sub(r'[(]|[)]|[^\w\s]', '', okpdtr_lst[i].lower())

# запрос вакансий через API
data = []
for okpdtr in okpdtr_lst:
    print(okpdtr_lst.index(okpdtr))
    page = 1
    while page > 0 and page < 20:
        parameters = {
            'text' : okpdtr,
            'area': 4,
            'per_page': RECORDS_PER_PAGE,
            'page': page,
        }
        req = r.get(URL_API, params=parameters)
        answer = req.json()
        if answer['found'] == 0 or len(answer['items']) == 0:
            page = 0
        else:
            data.append(answer)
            page += 1

# представление данных JSON в Pandas DataFrame
df_norm = pd.DataFrame()
for record in data:
    df_tmp = pd.io.json.json_normalize(record['items'])
    df_norm = pd.concat([df_norm, df_tmp],
                        sort=False,
                        ignore_index=True)
df_norm = df_norm.replace({
    False : np.nan,
    'NaN' : np.nan,
    'nan' : np.nan,
    np.nan : None,
    })
df_norm = df_norm[pd.notnull(df_norm['employer.id'])]
df_norm.to_csv(os.path.join('tables_hh', 'df_raw.csv'), index=None, header=True)

# формирование новых таблиц "Компании" и "Вакансии" из иходной таблицы
if not df_norm.empty:
    companies_hh = df_norm[[
        'employer.id',
        'employer.name',
        'address.raw',
        'address.lat',
        'address.lng',
        'contacts.name',
        'contacts.email',
        'employer.url',
    ]]
    companies_hh = companies_hh.drop_duplicates(subset='employer.id',
                                                keep='first')
    companies_hh = companies_hh.reset_index(drop=True)
    companies_hh = companies_hh.rename(
        columns={
            'employer.id': 'id',
            'employer.name': 'name',
            'address.raw': 'address',
            'address.lat': 'address_lat',
            'address.lng': 'address_lng',
            'contacts.name': 'contact_name',
            'contacts.email': 'contact_email',
            'employer.url': 'url',
        })
    companies_hh.to_csv(os.path.join('tables_hh', 'companies_hh.csv'),
                        index=None,
                        header=True)
    vacancies_hh = df_norm[[
        'id',
        'name',
        'employer.id',
        'area.name',
        'address.raw',
        'address.lat',
        'address.lng',
        'salary.from',
        'salary.to',
        'salary.currency',
        'snippet.requirement',
        'snippet.responsibility',
        'url',
        'type.name',
        'created_at',
        'published_at',
    ]]
    vacancies_hh = vacancies_hh.rename(
        columns={
            'employer.id': 'company_id',
            'area.name': 'region',
            'address.raw': 'address',
            'address.lat': 'address_lat',
            'address.lng': 'address_lng',
            'salary.from': 'salary_min',
            'salary.to': 'salary_max',
            'salary.currency': 'salary_currency',
            'snippet.requirement': 'requirement',
            'snippet.responsibility': 'responsibility',
            'type.name': 'type',
            'created_at': 'creation_date',
            'published_at': 'publication_date',
        })
    vacancies_hh = vacancies_hh.astype({
        'address_lat': 'str',
        'address_lng': 'str',
    })
    vacancies_hh.to_csv(os.path.join('tables_hh', 'vacancies_hh.csv'),
                        index=None,
                        header=True)