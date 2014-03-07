from os import listdir, system
import sys
sys.path.append('..')
import config

def run():
  p_path = config.data['processed_path']
  processed_files = [f for f in listdir(p_path)]
  nodes = [i for i in processed_files if i[0] == 'n']
  rels = [i for i in processed_files if i[0] == 'r']

  nodes_str = ",".join(map(lambda x: p_path + "/" + x, nodes))
  rels_str = ",".join(map(lambda x: p_path + "/" + x, rels))

  commands = [
    'cd ' + config.db_import.path + ';',
    config.db_import['script'],
    config.neo4j['data'],
    nodes_str,
    rels_str
  ]

  CMD = " ".join(commands)

  print "Calling", CMD
  system(CMD)

if __name__ == '__main__':
  run()