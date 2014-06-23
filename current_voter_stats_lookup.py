#!/usr/env python

# June 21, 2014
# OVERVIEW: We want an interface to GOPDataTrust.com data; this can include (but not yet) individual voter info, volunteer info,
# + and (currently) counts by ZIP code (optional param), state (optional param), and so on (see the link for more info on params:
# + http://api.gopdatatrust.com/beta/documentation.html).

# NB: It appears our API subscription only allows for access to FL data. (This is largely a note to myself.)

# Also note the server returns (using basic query structure) a JSON file of... more JSON. So the initial "layer" is little
# + JSON subsets, which then can be keyed as normal; i.e., jsonDict below (commented out under LINE 1) will choke on
# + jsonDict['person'] or jsonDict['x'] or whatever; however, we can at least iterate over the individual piles of data that
# + comprise a voter. Thusly, for p in jsonDict: ... will work fine, so long as the subsequent line then keys into p.



def make_keys_ordered(requests_text_object):
  # TEMP
  print 'Currently unused.'
  import sys; sys.exit()
  # ^ TEMP

  converted_to_ordered_json = json.loads(requests_text_object, object_pairs_hook=OrderedDict) #FIXME
  converted_to_json_text = json.dumps(converted_to_ordered_json, indent=2)  # FIXME: the indent may be superfl.
  return json.loads(converted_to_json_text)
  
def generate_fl_zip_code_list():
  import os
#  print '<<<<>>>>'
#  os.chdir('/Users/eorcutt/Downloads')
  os.chdir(os.getcwd())
  file_loc = 'zip_code_database.csv'
  t = {zip:(county,state) for zip,type,primary_city,acceptable_cities,unacceptable_cities,state,county,\
                              timezone,area_codes,latitude,longitude,world_region,country,decommissioned,\
                              estimated_population,notes in csv.reader(open(file_loc),delimiter=',')\
                          if state=='FL'}

  c_zip = {}

  for z in t:
    c,s=t[z]
    if c not in c_zip.keys():
      c_zip[c] = [z]
    else:
      c_zip[c].append(z)  
  
  # Now we have a {county:[zip1, zip2]} dictionary.
  
  fl_zip_codes = []

  for c in c_zip:
    for z in c_zip[c]:
      fl_zip_codes.append(z)

  return fl_zip_codes

# In order to dump the collected data to CSV, we need to make sure the data is ordered, so each position corresponds with the
# + proper column (i.e., so we won't find "James" in the "voterID" column for person keyed 1234567). While I haven't observed
# + this behavior yet, no point taking anyone's assurance the keys are perpetually, uniformly ordered.

def dump_json_to_csv_output(jsonObj,outputName='test_gop_people.csv'):
  """
  Input: sorted JSON (JSON set by ordered keys with collections.OrderedDict as json.loads(Request.text,object_pairs_hook=OrderedDict))
  Output: CSV file (name set by optional outputName var) to disk
  """
  headerLine = ''
  csvOutput = open(outputName,'a')
  
  if jsonObj != []:
    for person in jsonObj:
      if person != []:  # FIXME: determine why there are blank entries in the returned JSON
        try:
          if headerLine == '':
            headerLine = ','.join([key for key in person.keys()])
            csvOutput.write(headerLine+'\n')
          else:
            csvOutput.write(','.join([value for value in person.values()]) + '\n')
          
        except AttributeError, e:
          print 'Error caught with object {0} in container {1}: \"{2}\"'.format(person,jsonObj,e)
          sys.exit()
#          continue
  csvOutput.close()


### vvv The below is under construction vvv ###
def get_property_counts(token, state='FL', party=None, rnc_calcd_partisan_score=None, rnc_juris_code=None, precinct_abbrv=None, \
                        precinct=None, cong_district_num=None, state_upper_house_district_num=None, state_lower_house_district_num = None, \
                        first_name=None, last_name=None, sex=None, tel_area_code=None, reg_addr_line1=None, reg_addr_line2=None, reg_addr_city=None, \
                        reg_addr_zip5=None, mail_addr1=None, mail_addr2=None, mail_city=None, mail_state=None, mail_zip5=None, self_reported_demo=None, \
                        modeled_ethnic_group=None):

  def check_opt_param_present_append_to_url(paramIn, strOut):
    if not paramIn:
      return strOut + paramIn
### ^^^ The above is under construction ^^^ ###      
      
if __name__ == '__main__':
  
  import requests as r, csv, json, sys, os
  from collections import OrderedDict
  
  token = '3d4cd2a9-264f-4756-850b-3537956499aa'  # FIXME: make this required parameter for CL script (if publically Git'd).
  voterCountUrl = 'http://api.gopdatatrust.com/beta/basicquery_returncount.php/?APIToken='
  voterCountUrl += token
  state = 'FL'
  getUrl = voterCountUrl+'&State='+state  # I'm not changing this simple structure, in case of future states' additions to our stream.
  getVoterTagsUrl = 'http://api.gopdatatrust.com/beta/votertags_gettaglist.php/?APIToken='+token
  #statesAvailableForAccess = r.get(getVoterTagsUrl)
  #print statesAvailableForAccess.json()
  # ^ The above 3 lines show we can access all available tags.. for FL. So double-confirmation on that.

  #basicLastNameQueryUrl = 'http://api.gopdatatrust.com/beta/basicquery_returndata.php/?APIToken='+token+'&State='+state+'&LastName=Orcutt'
  # The mamma-jamma link to grab _all_ FL data: 
  basicLastNameQueryUrl = 'http://api.gopdatatrust.com/beta/basicquery_returndata.php/?APIToken='+token+'&State='+state

  # LINE 1 # jsonDict = r.get(basicLastNameQueryUrl).json()  # <-- this works, but I don't trust the ordering.
  
  ### TEMP BLOCK
  if False:
    basicLastNameQueryResponseText = r.get(basicLastNameQueryUrl).text
    jsonFromText = make_keys_ordered(basicLastNameQueryResponseText)
    dump_json_to_csv_output(jsonFromText)
  ### END TEMP BLOCK

  # ^ TODO OK, so now that we can reliably dump voter data to a CSV, we can work on sorting the columns by hand,
  # + so that we get sensible series of data, such as first name followed by last name (in a row).
  
  # Now, let's get as much as we can for FL:
  fl_zips = generate_fl_zip_code_list()
#  print fl_zips
  fl_master = []
  
  length = len(fl_zips)
  i = 0

  # Change to where this script is located):
#  os.chdir('/Users/eorcutt')
  os.chdir(os.getcwd())
  
  for zipc in fl_zips:  # FIXME: need to chunk this stuff now. Try in blocks of 100. 
  # KIM that l[:100] will produce a sublist of indices [0,99]. So start from [100, 201] for the next one hundred.
    query_url = basicLastNameQueryUrl + '&RegistrationZip5=' + zipc  # Canonical URL for voter-search based on ZIP5.

    i += 1
    print 'Loading {0}/{1} ZIP codes...'.format(i,length)

    if False:  # Currently overriding ordering enforcement. FIXME
      zip_text = r.get(query_url).text
      zip_json = json.loads(zip_text,object_pairs_hook=OrderedDict)
      zip_text = json.dumps(zip_json)  # have to purge the OrderedDict object; just want a flat string
      zip_json = json.loads(zip_text)
      
    zip_json = r.get(query_url).json()
    
    for person in zip_json:  # Consider each dict in the JSON as a person (as above).
    
      fl_master.append(person)
    
  dump_json_to_csv_output(fl_master,'fl_data.csv')
    
#  print fl_master
    
