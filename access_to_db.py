#!/usr/bin/env python3
# -*- coding: utf8 -*-
import pandas as pd
import sqlalchemy as sa


def get_table_from_query(query):
    df = pd.read_sql(query, engine)
    return df


engine = sa.create_engine(r"postgresql://blinov:GE1vmEN@217.71.129.139:4194/ias", echo=False)