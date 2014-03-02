# This script relies on environment variables. To avoid checking db creds into git repo,
# store them in a file called .env, then execute `source .env` before running this script.
# Needs HOST, USER, DB, and PASS variables, and they need to be `export`ed.
# See the sample.env file in this directory.

import os
import sys
sys.path.append('..') # for relative imports
import config

def run():

  # Only dump these tables
  tables = [
    'assignee',
    'inventor',
    'lawyer',
    'location',
    'location_assignee',
    'location_inventor',
    'mainclass',
    'subclass',
    'patent',
    'patent_assignee',
    'patent_inventor',
    'patent_lawyer',
    'uspatentcitation',
    'uspc',
    'rawassignee',
    'rawinventor',
    'rawlawyer',
    'rawlocation'
  ]

  try:
    options = [
      '--verbose',
      '-h ' + os.environ['HOST'],
      '-u ' + os.environ['USER'],
      '-p',
      '-p' + os.environ['PASS'],
      '--single-transaction',
      '--compatible=ansi',
      '--skip-extended-insert',
      '--compact'
    ]

    output_file = config.data['raw_path'] + '/dump.sql'
    db = os.environ['DB']
    tables_str = " ".join(tables)
    opts_str = " ".join(options)

    command = " ".join(['mysqldump', db, tables_str, opts_str, '>', output_file])

    print "Calling", command
    os.system(command)
    return True
  except KeyError:
    print "Remember to export database credentials!"
    print "Need keys HOST, DB, USER, and PASS"
    return False

if __name__ == '__main__':
  run()
