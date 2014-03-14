from helpers import node_file, from_sql, output_tsv, lambdas

def patents():
  patents = from_sql('patent', ['id', 'date', 'abstract', 'title'])
  patents.rename(columns={'id':'id:string:Patent'}, inplace=True)
  patents['l:label'] = 'Patent'
  patents['year:int'] = patents['date'].apply(lambda x: x.split("-")[0])
  patents['month:int'] = patents['date'].apply(lambda x: x.split("-")[1])
  patents['day:int'] = patents['date'].apply(lambda x: x.split("-")[2])
  output_tsv(patents, node_file('patents'))

def main_classes():
  classes = from_sql('mainclass').dropna(subset=['title'])
  classes = classes.drop('text', 1) # it's empty for all entries right now...
  classes['l:label'] = 'Class'
  classes.rename(columns={'id':'id:string:Class'}, inplace=True)
  output_tsv(classes, node_file('classes'))

def subclasses():
  subclasses = from_sql('uspc')[['mainclass_id', 'subclass_id']].drop_duplicates().dropna()
  subclasses['id:string:Subclass'] = subclasses.apply(lambdas['concat_class'], axis=1)
  subclasses = subclasses.drop(['mainclass_id', 'subclass_id'], axis=1)
  subclasses['l:label'] = 'Subclass'
  subclasses = subclasses.drop_duplicates(cols=['id:string:Subclass'])
  output_tsv(subclasses, node_file('subclasses'))

def lawyers():
  lawyers = from_sql('lawyer')
  lawyers = lawyers.drop('country', 1)
  lawyers['l:label'] = 'Lawyer'
  lawyers.rename(columns={'id':'id:string:Lawyer'}, inplace=True)
  output_tsv(lawyers, node_file('lawyers'))

def locations():
  locs = from_sql('location').dropna(subset=['country'])
  locs['country'] = locs['country'].apply(lambda x: x.upper())
  locs.rename(columns={
    'id':'id:string:Location',
    'city':'city:string:Location',
    'country':'country:string:Location',
    'state':'state:string:Location'
  }, inplace=True)
  locs['l:label'] = 'Location'
  output_tsv(locs, node_file('locations'))

def inventors():
  inventors = from_sql('rawinventor').drop_duplicates(cols=['inventor_id'])
  inventors = inventors.drop(['patent_id', 'rawlocation_id', 'sequence'], 1)
  inventors.rename(columns={'inventor_id':'id:string:Inventor'}, inplace=True)
  inventors['l:label'] = 'Inventor'
  output_tsv(inventors, node_file('inventors'))

def assignees():
  assignees = from_sql('assignee')
  assignees.rename(columns={'id':'id:string:Assignee'}, inplace=True)
  assignees['l:label'] = 'Assignee'
  output_tsv(assignees, node_file('assignees'))

### Process ###
def run():
  print "Processing nodes\n"
  nodes_to_output = [patents, main_classes, subclasses, lawyers, locations, inventors, assignees]
  for fn in nodes_to_output:
    fn()
  print "\n"

if __name__ == '__main__':
  print "Running manually...\n"
  run()