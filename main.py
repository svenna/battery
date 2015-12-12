import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import datetime as dt
from IPython.display import display
import os
import os.path


file_path = 'D:/projects/battery'
csv_file = 'batteries.csv'
db_file = 'batteries.db'

#1972023 3430529

def load_db(db):
    disk_engine = create_engine('sqlite:///{}'.format(db))
    return disk_engine

def read_device(deviceid, disk_engine):
    return pd.read_sql_query('SELECT * FROM data WHERE deviceid=3430529 ORDER BY change ASC', disk_engine)

def create_db(csv, db, overwrite=True):
    disk_engine = create_engine('sqlite:///{}'.format(db))

    start = dt.datetime.now()
    chunksize = 20000
    j = 0
    index_start = 1

    for df in pd.read_csv(csv, chunksize=chunksize, iterator=True, encoding='utf-8', sep='|'):

        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})  # Remove spaces from columns

        df['change'] = pd.to_datetime(df['change'])  # Convert to datetimes

        df.index += index_start

        # Remove the un-interesting columns
        # columns = ['Agency', 'CreatedDate', 'ClosedDate', 'ComplaintType', 'Descriptor',
        #            'CreatedDate', 'ClosedDate', 'TimeToCompletion',
        #            'City']

        # for c in df.columns:
        #     if c not in columns:
        #         df = df.drop(c, axis=1)

        j += 1
        print('{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize))

        df.to_sql('data', disk_engine, if_exists='append')
        index_start = df.index[-1] + 1


def main() -> object:
    csv = os.path.join(file_path, csv_file)
    db = os.path.join(file_path, db_file)
    create_db(csv, db)

if __name__ == '__main__':
    main()
