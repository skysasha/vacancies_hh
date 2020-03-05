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
PAGES = 10                                  # количество страниц для поиска
RECORDS_PER_PAGE = 100                      # записей на странице


# запрос вакансий через API (максимумально возращает 2000 вакансий)
data = []
for page in range(PAGES):
    parameters = {
        'area': 4,
        # 'per_page': RECORDS_PER_PAGE,
        # 'page': page,
    }
    req = r.get(URL_API, params=parameters)
    answer = req.json()
    data.append(answer)


# представление данных JSON в Pandas DataFrame
df_norm = pd.DataFrame()
for record in data:
    df_tmp = pd.io.json.json_normalize(record['items'])
    df_norm = pd.concat([df_norm, df_tmp],
                        sort=False,
                        ignore_index=True)
df_norm = df_norm.replace({
    False: np.nan,
    'NaN': np.nan,
    'nan': np.nan,
    np.nan: None,
})
df_norm = df_norm[pd.notnull(df_norm['employer.id'])]
df_norm.to_csv(os.path.join('test_hh', 'df_norm.csv'), index=None, header=True)
