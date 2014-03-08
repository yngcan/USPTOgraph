import sys
import sqlite3
import pandas.io.sql as psql
import os
sys.path.append('..')
import config

# Example usage: node_file('inventors')
node_file = lambda x: config.data['processed_path'] + '/n_' + x + '.tsv'
rel_file = lambda x: config.data['processed_path'] + '/r_' + x + '.tsv'

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

def from_sql(tablename, output = True, columns = False):
  if output:
    print "Loading table '" + str(tablename) + "'..."

  if not columns:
    return psql.read_frame(select(tablename), conn)
  else:
    fields = ", ".join(columns)
    return psql.read_frame('SELECT ' + fields + ' FROM ' + str(tablename), conn)

def output_tsv(df, filename, order = False):
  type = filename.split("/")[-1][2:-4].upper() # /Users/test/n_yee.csv --> YEE
  if not order:
    df.to_csv(filename, index=False, sep="\t", encoding='utf-8')
  else:
    df.to_csv(filename, index=False, sep="\t", encoding='utf-8', cols=order)
  print type, len(df), "--->", filename

lambdas = {
  'concat_class': lambda x: str(x['mainclass_id']) + '/' + str(x['subclass_id'].split('/')[-1]),
  'sid': lambda x: "id:string:" + str(x) # string id
}