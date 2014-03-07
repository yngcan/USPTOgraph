import sys
import sqlite3
import pandas.io.sql as psql
import os
sys.path.append('..')
import config

# Example usage: node_file('inventors')
node_file = lambda x: config.data['processed_path'] + '/n_' + x + '.csv'
rel_file = lambda x: config.data['processed_path'] + '/r_' + x + '.csv'

config = config

# Example usage: select('inventor') or select('rawinventor')
def select(tablename):
  query = "SELECT * FROM "
  query += str(tablename)
  if 'TEST' in os.environ.keys():
    query += ' LIMIT 500000'
  return query

### DB SETUP ###
conn = sqlite3.connect(config.data['raw_sqlite'])

def from_sql(tablename, output = True):
  if output:
    print "Loading table '" + str(tablename) + "'..."
  return psql.read_frame(select(tablename), conn)

def output_tsv(df, filename):
  type = filename.split("/")[-1][2:-4].upper() # /Users/test/n_yee.csv --> YEE
  df.to_csv(filename, index=False, sep="\t")
  print type, len(df), "--->", filename