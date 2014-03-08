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
  ORDER = [sid('assignees'), sid('locations'), 'type']

  assignee_locs = from_sql('location_assignee').dropna()
  assignee_locs.rename(columns={'location_id':sid('locations'), 'assignee_id':sid('assignees')}, inplace=True)
  assignee_locs['type'] = 'FROM'
  output_tsv(assignee_locs, rel_file('assignee_from'), ORDER)

def inventor_from():
  ORDER = [sid('inventors'), sid('locations'), 'type']

  inventor_locs = from_sql('location_inventor').dropna()
  inventor_locs.rename(columns={'location_id':sid('locations'), 'inventor_id':sid('inventors')}, inplace=True)
  inventor_locs['type'] = 'FROM'
  output_tsv(inventor_locs, rel_file('inventor_from'), ORDER)

def cites():
  uspatentcitation = from_sql('uspatentcitation')
  citations = uspatentcitation[['patent_id', 'citation_id']].dropna() # Make sure to order them properly...
  citations.columns = [sid('patents'), sid('patents')]
  citations['type'] = 'CITES'

  # No order needed because we've whittled it down to only the fields we want
  output_tsv(citations, rel_file('cites'))

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
    inventor_from,
    cites
  ]
  for fn in rels_to_output:
    fn()
  print "\n"

if __name__ == '__main__':
  print "Running manually...\n"
  run()