import sys
import os
from mysql_fetch import dump
from processor import nodes, rels
from importer.import_db import run as run_import
import config

def fetch():
  print "Fetching..."
  if dump.run():
    print "sqlite created!"
  else:
    print "Fetch failed"

def process_nodes():
  nodes.run()

def process_rels():
  rels.run()

def process():
  process_nodes()
  process_rels()

def neo4j_import():
  run_import()

def neo4j(arg = False):
  try:
    cmd = sys.argv[2]
    os.system(config.neo4j['bin_path'] + " " + cmd)
  except:
    if not arg:
      print "Please pass an additional command to run - e.g. python run.py neo4j console"
      sys.exit(1)
    else:
      os.system(config.neo4j['bin_path'] + " " + arg)

def run_all():
  fetch()
  process()
  neo4j_import()
  neo4j('start')

commands = {
  'fetch': fetch,
  'process_nodes': process_nodes,
  'process_rels': process_rels,
  'process': process,
  'import': neo4j_import,
  'all': run_all,
  'neo4j': neo4j
}

try:
  cmd = sys.argv[1]
except:
  print "Please try running `python run.py [arg]` one of the following:"
  print "\n".join(map(lambda x: "  - " + str(x), commands.keys()))
  sys.exit(1)

# Run command out of dictionary...so python run.py fetch will call fetch(), for example
commands[cmd.lower()]()