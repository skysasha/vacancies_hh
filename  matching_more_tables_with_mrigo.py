#!/usr/bin/env python
# coding: utf-8
import jellyfish
import os
import pandas as pd
import re
import string

import funcs
import okpdtr_splits


SIMILARITY_LEVEL = 0.80                     # "уровень" схожести строк по Джаро


vacancies_hh = pd.read_csv(os.path.join('tables_hh', 'vacancies_hh.csv'),
                           dtype={
                               'id': object,
                               'company_id': object,
                               'address_lat': object,
                               'address_lng': object,
                               'salary_min': object,
                               'salary_max': object})

okpdtr_id_name = pd.read_csv(os.path.join('tables_hh', 'id_okpdtr_okpdtr.csv'),
                             dtype={
                                 'id': object,
                                 'name': object})

id_okpdtr_lst = okpdtr_id_name['id'].tolist()
okpdtr_lst = okpdtr_id_name['name'].tolist()
okpdtr_lst_raw = okpdtr_lst.copy()
job_lst = vacancies_hh['name'].tolist()

for i in range(len(okpdtr_lst)):
    okpdtr_lst[i] = re.sub(r'[(]|[)]|[^\w\s]', '', okpdtr_lst[i].lower())

remove_digits = str.maketrans('', '', string.digits)
for i in range(len(job_lst)):
    job_lst[i] = job_lst[i].translate(remove_digits)
    job_lst[i] = re.sub(r'[A-Za-z]|[^\w\s]|[(]|[)]|',
                        '',
                        re.sub(r'\s+|[-]', ' ', str(job_lst[i])).lower()).strip()
    job_lst[i] = re.split(r'{}'.format('|'.join(okpdtr_splits.dictionary)),
                          job_lst[i])[0]

indexes = []
for i in range(len(job_lst)):
    print(i)
    sub_lst = []
    for j in range(len(okpdtr_lst)):
        sub_lst.append(jellyfish.jaro_distance(okpdtr_lst[j], job_lst[i]))
    max_indexes = funcs.find_locate_max(sub_lst)
    if max_indexes[0] < SIMILARITY_LEVEL:
        indexes.append(len(okpdtr_lst))
    else:
        indexes.append(max_indexes[1][0])
id_okpdtr_lst.append(None)
okpdtr_lst.append(None)

vacancies_hh.insert(2,
                    'id_okpdtr',
                    [id_okpdtr_lst[indexes[i]] for i in range(len(job_lst))],
                    True)
vacancies_hh.to_csv(os.path.join('tables_hh', 'vacancies_hh_okpdtr.csv'),
                    index=None,
                    header=True)

vacancies_hh_name_id_okpdtr_okpdtr = vacancies_hh[['name', 'id_okpdtr']]
okpdtr_lst_raw.append(None)
vacancies_hh_name_id_okpdtr_okpdtr.insert(2,
                                          'okpdtr',
                                          [okpdtr_lst_raw[indexes[i]] for i in range(len(job_lst))],
                                          True)
print(vacancies_hh_name_id_okpdtr_okpdtr.isna().sum())
vacancies_hh_name_id_okpdtr_okpdtr.to_csv(os.path.join('tables_hh', 'vacancies_hh_name_id_okpdtr_okpdtr.csv'),
                                          index=None,
                                          header=True)
