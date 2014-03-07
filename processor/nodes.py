from helpers import node_file, from_sql, output_tsv, lambdas

def patents():
  patents = from_sql('patent')[['id', 'date']]
  patents['id:string:patents'] = patents.pop('id')
  patents['l:label'] = 'Patent'
  output_tsv(patents, node_file('patents'))

def main_classes():
  classes = from_sql('mainclass').dropna(subset=['title'])
  classes = classes.drop('text', 1)
  classes['l:label'] = 'Class'
  classes['id:string:classes'] = classes.pop('id')
  output_tsv(classes, node_file('classes'))

def subclasses():
  subclasses = from_sql('uspc')[['mainclass_id', 'subclass_id']].drop_duplicates()
  subclasses['id:string:subclasses'] = subclasses.apply(lambdas['concat_class'], axis=1)
  subclasses = subclasses.drop(['mainclass_id', 'subclass_id'], axis=1)
  subclasses['l:label'] = 'Subclass'
  subclasses = subclasses.drop_duplicates(cols=['id:string:subclasses'])
  output_tsv(subclasses, node_file('subclasses'))

def lawyers():
  lawyers = from_sql('lawyer')
  lawyers = lawyers.drop('country', 1)
  lawyers['l:label'] = 'Lawyer'
  lawyers['id:string:lawyers'] = lawyers.pop('id')
  output_tsv(lawyers, node_file('lawyers'))

def locations():
  locs = from_sql('location')
  locs['id:string:locations'] = locs.pop('id')
  locs['l:label'] = 'Location'
  locs['country'] = locs['country'].apply(lambda x: x.upper())
  output_tsv(locs, node_file('locations'))

def inventors():
  inventors = from_sql('rawinventor').drop_duplicates(cols=['inventor_id'])
  inventors = inventors.drop(['patent_id', 'rawlocation_id', 'nationality', 'sequence'], 1)
  inventors['l:label'] = 'Inventor'
  output_tsv(inventors, node_file('inventors'))

def assignees():
  assignees = from_sql('assignee')
  assignees = assignees.drop(['type', 'residence', 'nationality'], 1)
  assignees['l:label'] = 'Assignee'
  output_tsv(assignees, node_file('assignees'))

### Process ###
def run():
  print "Processing nodes\n"
  nodes_to_output = [patents, main_classes, subclasses, lawyers, locations, inventors, assignees]
  for fn in nodes_to_output:
    fn()

if __name__ == '__main__':
  print "Running manually...\n"
  run()