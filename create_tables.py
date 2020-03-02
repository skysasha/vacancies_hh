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
from string import digits

import access_to_db


pd.set_option('display.max_columns', None)


def find_locate_max(lst):
    biggest = max(lst)
    return biggest, [
        index for index, element in enumerate(lst) if biggest == element
    ]


# запрос вакансий (максимумально возращает 2000 вакансий)
api_url = 'https://api.hh.ru/vacancies'
pages = 20
per_page = 100

data = []
for page in range(pages):
    parameters = {
        'area': 4,
        'per_page': per_page,
        'page': page,
    }
    req = r.get(api_url, params=parameters)
    answer = req.json()
    data.append(answer)


# представление данных json в Pandas DataFrame
df_norm = pd.DataFrame()
for record in data:
    df_tmp = pd.io.json.json_normalize(record['items'])
    df_norm = pd.concat([df_norm, df_tmp], sort=False, ignore_index=True)
df_norm = df_norm.replace({
    False: np.nan,
    'NaN': np.nan,
    'nan': np.nan,
    np.nan: None,
})

df_norm = df_norm[pd.notnull(df_norm['employer.id'])]


# формирование новых таблиц
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


# получение таблицы с id_okpdtr с сервера НГТУ
okptdr_query = """
        SELECT data.okpdtr.id, data.okpdtr.name
        FROM data.okpdtr
        ORDER BY 1
        """
id_okpdtr_okpdtr = access_to_db.get_table_from_query(okptdr_query)
id_okpdtr_okpdtr.to_csv(os.path.join('tables_hh', 'id_okpdtr_okpdtr.csv'),
                        index=None,
                        header=True)


# добавление id_okpdtr в vacancies_hh
SIMILARITY_LEVEL = 0.85

okpdtr_splits = [
    r'\sтц',
    r'\sсфо\s',
    r'\sг\s',
    r'\sтд',
    r'\sв\s',
    r'\sдсу\s',
    r'\sул\s',
    r'\sдц',
    r'\sтрц',
    r'\sбкс',
    r'сан\sсити',
    r'\sкатегории',
    r'\sмдж',
    r'\sкатегория',
    r'\sразряд',
    r'\sгум',
    r'\sцум',
    r'\sразряд',
    r'\sкласс',
    r'\sкласса',
    r'\sяслей',
    r'\sясли',
    r'\sсад',
    r'\sсада',
    r'\sмладший',
    r'\sвысшей',
    r'\sсредней',
    r'\sлиния',
    r'\sулица',
    r'\sпер\s',
    r'\sпл\s',
    r'\sпер\s',
    r'звонить',
    r'\sмкр\s',
    r'\sш\s',
    r'пр[-]кт',
    r'переулок',
    r'ржд',
    r'крс',
    r'магазине',
    r'мсб',
    r'рц',
    r'\sр\s',
    r'\sпер',
    r'новосибирск'
    r'фгис',
    r'академгородок',
    r'республика',
    r'танцор',
    r'сфера',
    r'рубл',
]

vacancies_hh = pd.read_csv(os.path.join('tables_hh', 'vacancies_hh.csv'),
                           dtype={
                               'id': object,
                               'company_id': object,
                               'address_lat': object,
                               'address_lng': object,
                               'salary_min': object,
                               'salary_max': object
})
okpdtr_id_name = pd.read_csv(os.path.join('tables_hh', 'id_okpdtr_okpdtr.csv'),
                             dtype={'id': object, 'name': object})

id_okpdtr_lst = okpdtr_id_name['id'].tolist()
okpdtr_lst = okpdtr_id_name['name'].tolist()
okpdtr_lst_raw = okpdtr_lst.copy()
job_lst = vacancies_hh['name'].tolist()

for i in range(len(okpdtr_lst)):
    okpdtr_lst[i] = re.sub(r'[(]|[)]|[^\w\s]', '', okpdtr_lst[i].lower())

remove_digits = str.maketrans('', '', digits)
for i in range(len(job_lst)):
    job_lst[i] = job_lst[i].translate(remove_digits)
    job_lst[i] = re.sub(r'[A-Za-z]|[^\w\s]|[(]|[)]|', '',
                        re.sub(r'\s+|[-]', ' ', str(job_lst[i])).lower()).strip()
    job_lst[i] = re.split(r'{}'.format(
        '|'.join(okpdtr_splits)), job_lst[i])[0]

indexes = []
for i in range(len(job_lst)):
    sub_lst = []
    for j in range(len(okpdtr_lst)):
        sub_lst.append(jellyfish.jaro_distance(okpdtr_lst[j], job_lst[i]))
    max_indexes = find_locate_max(sub_lst)
    if max_indexes[0] < SIMILARITY_LEVEL:
        indexes.append(len(okpdtr_lst))
    else:
        indexes.append(max_indexes[1][0])
id_okpdtr_lst.append(None)
okpdtr_lst.append(None)

vacancies_hh.insert(2, 'id_okpdtr',
                    [id_okpdtr_lst[indexes[i]] for i in range(len(job_lst))],
                    True)
vacancies_hh.to_csv(os.path.join('tables_hh', 'vacancies_hh_okpdtr.csv'),
                    index=None,
                    header=True)

vacancies_hh_name_id_okpdtr_okpdtr = vacancies_hh[['name', 'id_okpdtr']]
okpdtr_lst_raw.append(None)
vacancies_hh_name_id_okpdtr_okpdtr.insert(
    2, 'okpdtr', [okpdtr_lst_raw[indexes[i]] for i in range(len(job_lst))],
    True)
print(vacancies_hh_name_id_okpdtr_okpdtr.isna().sum())
vacancies_hh_name_id_okpdtr_okpdtr.to_csv(os.path.join('tables_hh', 'vacancies_hh_name_id_okpdtr_okpdtr.csv'),
                                          index=None,
                                          header=True)
