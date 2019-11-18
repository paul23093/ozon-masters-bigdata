#!/opt/conda/envs/dsenv/bin/python

import sys
from joblib import load
import numpy as np
import pandas as pd

# from model import fields

model = load("2.joblib")

numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]
fields = ["id", "label"] + numeric_features + categorical_features

fields.pop(1)

read_opts=dict(
        sep='\t', names=fields, index_col=False, header=None)

# for df in pd.read_table(sys.stdin, **read_opts):
#     print(df)
#     pred = model.predict(df)
#     out = zip(df['id'], pred)
#     print(["{0},{1}".format(*i) for i in out])
    
for line in sys.stdin:
    values = line.strip().split('\t')
#     print(values)
    df = pd.DataFrame([values], columns=fields)
    df = df.replace('\\N', np.nan)
    df.loc[:, numeric_features] = df.loc[:, numeric_features].apply(pd.to_numeric, errors='raise')
    if (( df.loc[0, 'if1'] > 20 ) and ( df.loc[0, 'if1'] < 40 )):
        pred = model.predict(df)
        out = zip(df.loc[0, 'id'], pred)
        print(["{0}\t{1}".format(*i) for i in out])