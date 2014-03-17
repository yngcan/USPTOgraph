import os
base_path = os.path.dirname(os.path.abspath(__file__))

class Module:
  """ Keeps track of paths. """

  def __init__(self, _path, _dict):
    self.path = base_path + '/' + _path
    self.dict = {}
    for i in _dict.items():
      self.dict[i[0]] = self.path + '/' + i[1]

  def __getitem__(self, item):
    if item.lower() == 'path': return self.path
    return self.dict[item]


### PREFERENCES ###
TEST_SELECT_LIMIT = '1000000'

data = Module('data', {
  'raw_path': 'raw',
  'raw_sqlite': 'raw/raw.sqlite',
  'processed_path': 'processed'
})

fetch = Module('mysql_fetch', {
  'convert_script_path': 'convert.sh'
})

db_import = Module('importer', {
  'script': 'import.sh'  
})

neo4j = Module('neo4j', {
  'bin_path': 'bin/neo4j', # use this with os.system - eg os.system(neo4j['bin_path'] + " start")
  'data': 'data/graph.db'
})