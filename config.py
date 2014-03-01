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


data = Module('data', {
  'raw_path': 'raw',
  'raw_mysqldump': 'raw/dump.sql',
  'raw_sqlite': 'raw/raw.sqlite',
  'processed_path': 'processed'
})

fetch = Module('mysql_fetch', {
  'convert_script_path': 'convert.sh'
})