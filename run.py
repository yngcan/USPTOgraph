import sys
from mysql_fetch import dump
from processor import nodes

def fetch():
  print "Fetching..."
  if dump.run():
    print "sqlite created!"
  else:
    print "Fetch failed"

def process_nodes():
  nodes.run()


commands = {
  'fetch': fetch,
  'process_nodes': process_nodes
}

try:
  cmd = sys.argv[1]
except:
  print "Please try running `python run.py [arg]` one of the following:"
  print "\n".join(map(lambda x: "  - " + str(x), commands.keys()))
  sys.exit(1)

# Run command out of dictionary...so python run.py fetch will call fetch(), for example
commands[cmd.lower()]()