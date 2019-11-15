#!/opt/conda/envs/dsenv/bin/python

import sys
from joblib import load
import pandas as pd
from model import fields

model = load("2.joblib")
fields.pop(1)

read_opts=dict(
        sep='\t', names=fields, index_col=False, header=None,
        iterator=True, chunksize=100)

# for line in sys.stdin:
#     values = pd.DataFrame(data=[line.strip().split("\t")], columns=fields)
#     pred = model.predict(values)
#     print('\t'.join([values.loc[0,'id'], str(pred[0])]))
    
for df in pd.read_csv(sys.stdin, **read_opts):
    pred = model.predict(df)
    out = zip(df['id'], pred)
    print('\n'.["{0}\t{1}".format(*i) for i in out])
