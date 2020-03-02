#!/usr/bin/env python3
# -*- coding: utf8 -*-
import numpy as np
import os
import pandas as pd
import re
import sqlalchemy as sa

import access_to_db

companies_hh = pd.read_csv(os.path.join('tables_hh', 'companies_hh.csv'), dtype={
    'id': object,
    'name': object,
    'address': object,
    'address_lat': object,
    'address_lng': object,
    'contact_name': object,
    'contact_email': object,
    'url': object,
})
companies_hh = companies_hh.replace({np.nan: None})
companies_hh.to_sql(
    'companies_hh',
    con=access_to_db.engine,
    schema='blinov',
    if_exists='replace',
    index=False,
    chunksize=None,
    method=None,
    dtype={
        'id': sa.String,
        'name': sa.String,
        'address': sa.String,
        'address_lat': sa.String,
        'address_lng': sa.String,
        'contact_name': sa.String,
        'contact_email': sa.String,
        'url': sa.String,
    })
access_to_db.engine.execute(
    'ALTER TABLE blinov.companies_hh ADD PRIMARY KEY(id)')

vacancies_hh = pd.read_csv(os.path.join('tables_hh', 'vacancies_hh_okpdtr.csv'), dtype={
    'id': object,
    'name': object,
    'company_id': object,
    'id_okpdtr' : object,
    'region': object,
    'address': object,
    'address_lat': object,
    'address_lng': object,
    'salary_min': object,
    'salary_max': object,
    'salary_currency': object,
    'requirement': object,
    'responsibility': object,
    'url': object,
    'type': object,
    'creation_date': object,
    'publication_date': object,
})

vacancies_hh = vacancies_hh.replace({np.nan: None})
vacancies_hh.to_sql(
    'vacancies_hh',
    con=access_to_db.engine,
    schema='blinov',
    if_exists='replace',
    index=False,
    chunksize=None,
    method='multi',
    dtype={
        'id': sa.String,
        'name': sa.String,
        'id_okpdtr' : sa.String,
        'company_id': sa.String,
        'region': sa.String,
        'address': sa.String,
        'address_lat': sa.String,
        'address_lng': sa.String,
        'salary_min': sa.String,
        'salary_max': sa.String,
        'salary_currency': sa.String,
        'requirement': sa.String,
        'responsibility': sa.String,
        'url': sa.String,
        'type': sa.String,
        'creation_date': sa.String,
        'publication_date': sa.String,
    })
access_to_db.engine.execute(
    'ALTER TABLE blinov.vacancies_hh ADD PRIMARY KEY (id)')
access_to_db.engine.execute(
    'ALTER TABLE blinov.vacancies_hh ADD CONSTRAINT vac_comp_f_key_ FOREIGN KEY (company_id) REFERENCES blinov.companies_hh (id)')
access_to_db.engine.execute(
    'ALTER TABLE blinov.vacancies_hh ADD CONSTRAINT vac_okpdtr_f_key_ FOREIGN KEY (id_okpdtr) REFERENCES blinov.okpdtr (id)')
