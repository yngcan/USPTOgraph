from helpers import rel_file, from_sql, output_tsv, lambdas

sid = lambdas['sid']

# Patent -[:ASSIGNED_TO]-> Assignee
def assigned_to():
  ORDER = [sid('patents'), sid('assignees'), 'type']

  patent_assignee = from_sql('patent_assignee')
  patent_assignee.rename(columns={'patent_id':'id:string:patents', 'assignee_id':'id:string:assignees'}, inplace=True)
  patent_assignee['type'] = 'ASSIGNED_TO'
  output_tsv(patent_assignee, rel_file('assigned_to'), ORDER)

# Subclass -[:SUBCLASSES]-> Class
def subclasses():
  ORDER = [sid('subclasses'), sid('classes'), 'type']

  subclasses = from_sql('uspc')[['mainclass_id', 'subclass_id']].drop_duplicates().dropna()
  subclasses['id:string:subclasses'] = subclasses.apply(lambdas['concat_class'], axis=1)
  subclasses = subclasses.drop('subclass_id', axis=1)
  subclasses.rename(columns={'mainclass_id':'id:string:classes'}, inplace=True)
  subclasses['type'] = 'SUBCLASSES'
  output_tsv(subclasses, rel_file('subclasses'), ORDER)

# Patent -[:CLASSIFIED_AS]-> Subclass
def classified_as():
  ORDER = [sid('patents'), sid('subclasses'), 'type']

  uspc = from_sql('uspc')[['mainclass_id', 'subclass_id', 'patent_id']].drop_duplicates().dropna()
  uspc['id:string:subclasses'] = uspc.apply(lambdas['concat_class'], axis=1)
  uspc.rename(columns={'patent_id':sid('patents')}, inplace=True)
  uspc = uspc.drop('subclass_id', 1)
  uspc['type'] = 'CLASSIFIED_AS'
  output_tsv(uspc, rel_file('classified_as'), ORDER)

# Inventor -[:INVENTED]-> Patent
def invented():
  ORDER = [sid('inventors'), sid('patents'), 'type']

  patent_inventor = from_sql('patent_inventor')
  patent_inventor.rename(columns={'patent_id':sid('patents'), 'inventor_id':sid('inventors')}, inplace=True)
  patent_inventor['type'] = 'INVENTED'
  output_tsv(patent_inventor, rel_file('invented'), ORDER)

# Lawyer -[:REPRESENTED]-> Patent
def represented():
  ORDER = [sid('lawyers'), sid('patents'), 'type']

  patent_lawyer = from_sql('patent_lawyer')
  patent_lawyer.rename(columns={'patent_id':sid('patents'), 'lawyer_id':sid('lawyers')}, inplace=True)
  patent_lawyer['type'] = 'REPRESENTED'
  output_tsv(patent_lawyer, rel_file('represented'), ORDER)

def assignee_from():
  pass

def inventor_from():
  pass

### Process ###
def run():
  print "Processing relationships (edges)\n"
  rels_to_output = [
    assigned_to,
    subclasses,
    classified_as,
    invented,
    represented,
    assignee_from,
    inventor_from
  ]
  for fn in rels_to_output:
    fn()

if __name__ == '__main__':
  print "Running manually...\n"
  run()