from helpers import rel_file, from_sql, output_tsv, lambdas

sid = lambdas['sid']

# Patent -[:ASSIGNED_TO]-> Assignee
def assigned_to():
  ORDER = [sid('Patent'), sid('Assignee'), 'type']

  patent_assignee = from_sql('patent_assignee')
  patent_assignee.rename(columns={'patent_id':'id:string:Patent', 'assignee_id':'id:string:Assignee'}, inplace=True)
  patent_assignee['type'] = 'ASSIGNED_TO'
  output_tsv(patent_assignee, rel_file('assigned_to'), ORDER)

# Subclass -[:SUBCLASSES]-> Class
def subclasses():
  ORDER = [sid('Subclass'), sid('Class'), 'type']

  subclasses = from_sql('uspc')[['mainclass_id', 'subclass_id']].drop_duplicates().dropna()
  subclasses['id:string:Subclass'] = subclasses.apply(lambdas['concat_class'], axis=1)
  subclasses = subclasses.drop('subclass_id', axis=1)
  subclasses.rename(columns={'mainclass_id':'id:string:Class'}, inplace=True)
  subclasses['type'] = 'SUBCLASSES'
  output_tsv(subclasses, rel_file('subclasses'), ORDER)

# Patent -[:CLASSIFIED_AS]-> Subclass
def classified_as():
  ORDER = [sid('Patent'), sid('Subclass'), 'type']

  uspc = from_sql('uspc')[['mainclass_id', 'subclass_id', 'patent_id']].drop_duplicates().dropna()
  uspc['id:string:Subclass'] = uspc.apply(lambdas['concat_class'], axis=1)
  uspc.rename(columns={'patent_id':sid('Patent')}, inplace=True)
  uspc = uspc.drop('subclass_id', 1)
  uspc['type'] = 'CLASSIFIED_AS'
  output_tsv(uspc, rel_file('classified_as'), ORDER)

# Inventor -[:INVENTED]-> Patent
def invented():
  ORDER = [sid('Inventor'), sid('Patent'), 'type', 'from']

  rawinventor = from_sql('rawinventor', True, ['rawlocation_id', 'patent_id', 'inventor_id'])
  rawlocation = from_sql('rawlocation', True, ['id', 'location_id'])
  patent_inventor = rawinventor.merge(rawlocation, left_on=['rawlocation_id'], right_on=['id']).dropna(subset=['patent_id', 'inventor_id', 'location_id'])
  patent_inventor.rename(columns={'patent_id':sid('Patent'), 'inventor_id':sid('Inventor'), 'location_id': 'from'}, inplace=True)
  patent_inventor['type'] = 'INVENTED'
  output_tsv(patent_inventor, rel_file('invented'), ORDER)

# Patent -[:INVENTED_IN]-> Location
def invented_in():
  ORDER = [sid('Patent'), sid('Location'), 'type', 'by']

  rawinventor = from_sql('rawinventor', True, ['rawlocation_id', 'patent_id', 'inventor_id'])
  rawlocation = from_sql('rawlocation', True, ['id', 'location_id'])
  patent_location = rawinventor.merge(rawlocation, left_on=['rawlocation_id'], right_on=['id']).dropna(subset=['patent_id', 'inventor_id', 'location_id'])
  patent_location.rename(columns={'patent_id':sid('Patent'), 'inventor_id': 'by', 'location_id': sid('Location')}, inplace=True)
  patent_location['type'] = 'INVENTED_IN'
  output_tsv(patent_location, rel_file('invented_in'), ORDER)

# Lawyer -[:REPRESENTED]-> Patent
def represented():
  ORDER = [sid('Lawyer'), sid('Patent'), 'type']

  patent_lawyer = from_sql('patent_lawyer')
  patent_lawyer.rename(columns={'patent_id':sid('Patent'), 'lawyer_id':sid('Lawyer')}, inplace=True)
  patent_lawyer['type'] = 'REPRESENTED'
  output_tsv(patent_lawyer, rel_file('represented'), ORDER)

# Assignee -[:FROM]-> Location
def assignee_from():
  ORDER = [sid('Assignee'), sid('Location'), 'type']

  assignee_locs = from_sql('location_assignee').dropna()
  assignee_locs.rename(columns={'location_id':sid('Location'), 'assignee_id':sid('Assignee')}, inplace=True)
  assignee_locs['type'] = 'FROM'
  output_tsv(assignee_locs, rel_file('assignee_from'), ORDER)

# Inventor -[:FROM]-> Location
def inventor_from():
  ORDER = [sid('Inventor'), sid('Location'), 'type']

  inventor_locs = from_sql('location_inventor').dropna()
  inventor_locs.rename(columns={'location_id':sid('Location'), 'inventor_id':sid('Inventor')}, inplace=True)
  inventor_locs['type'] = 'FROM'
  output_tsv(inventor_locs, rel_file('inventor_from'), ORDER)

# Patent -[:CITES]-> Patent
def cites():
  # Manual query because pulling all fields was too slow...
  citations = from_sql('uspatentcitation', True, ['patent_id', 'citation_id']).dropna() # Make sure to order them properly...
  citations.columns = [sid('Patent'), sid('Patent')]
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
    invented_in,
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