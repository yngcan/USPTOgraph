from helpers import node_file, from_sql, output_tsv

def patents():
  patents = from_sql('patent')[['id', 'date']]
  patents['id:string:patents'] = patents.pop('id')
  patents['l:label'] = 'Patent'
  output_tsv(patents, node_file('patents'))

def main_classes():
  classes = from_sql('mainclass').dropna(subset=['title'])
  classes['l:label'] = 'Class'
  classes['id:string:classes'] = classes.pop('id')
  output_tsv(classes, node_file('classes'))

### Process ###
def run():
  print "Processing nodes\n"
  nodes_to_output = [patents, main_classes]
  for fn in nodes_to_output:
    fn()

if __name__ == '__main__':
  print "Running manually...\n"
  run()