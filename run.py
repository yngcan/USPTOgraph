import sys
import os
import config
from mysql_fetch import dump
cmd = sys.argv[1]

def fetch():
  print "Fetching..."
  if dump.run():
    print "Constructing sqlite..."
    os.system('sh %s %s %s' % (config.fetch['convert_script_path'], config.data['raw_mysqldump'], config.data['raw_sqlite']))
  else:
    print "Fetch failed"

commands = {
  'fetch': fetch
}

# Run command out of dictionary...so python run.py fetch will call fetch(), for example
commands[cmd.lower()]()