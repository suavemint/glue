def calculate_signature(string_to_sign, private_key):
    from hashlib import sha1
    import hmac, binascii, base64

    hashed = hmac.new(private_key, string_to_sign, sha1)

    return binascii.b2a_base64(hashed.digest())[:-1]
#    return base64.urlsafe_b64encode(hashed.digest())


def expiration_timestamp(lifetime=1200):
    import time
    # Return current time + 20 min (default) in UNIX UTC:
    return time.time() + lifetime
    
    
def call_url(method='GET', route='forms'):
  import time, requests, logging
  
  logging.basicConfig(level=logging.DEBUG)  
  logger = logging.getLogger(__name__)
#  logger.setLevel(logging.INFO)
  
  
#  print 'Calling route %s ...' % route

  logger.info('Sending call to route %s' % route)
  
#  original_url = url
  base_url = 'http://www.reclaimamericapac.com/gravityformsapi/'
  public_key = '5dd413313e'
  private_key = '5518f270a33c1c6'
  
  current_time = str(expiration_timestamp())[:-3]
  string_to_sign = public_key + ':' + method + ':' + route + ':' + current_time
  signature = calculate_signature(string_to_sign, private_key)
  url = base_url + route + '/?api_key=' + public_key + '&signature=' + signature + '&expires=' + current_time
#  print url
  request = requests.get(url)
  req_json = request.json()['response']

  # The server is not the best at receiving calls, so try again if the call fails.
  # It should succeed after a few tries.
  if request is None or req_json == 'Not authorized' or req_json == 'N':
    print 'Not authorized. Trying again...'
    time.sleep(1)
    call_url(method, route)
    
  return request
#  print request.json()
  
  try:
#    r = requests.get(original_url).json()
#    print route
#    return request
    pass
  except ValueError as e:
    print 'Server refused. Waiting two seconds before trying request again...'
    time.sleep(2)
    call_url(url, method, route)
  

if __name__ == '__main__':
  
    import requests, logging, sys

    base_url = 'http://www.reclaimamericapac.com/gravityformsapi/'
    public_key = '5dd413313e'
    private_key = '5518f270a33c1c6'
    method = 'GET'
    route = 'forms'

    string_to_sign = public_key + ':' + method + ':' + route + ':' + str(expiration_timestamp())[:-3]
    signature = calculate_signature(string_to_sign, private_key) 
    url = base_url + route + '/?api_key=' + public_key + '&signature=' + signature + '&expires=' + str(expiration_timestamp())[:-3]
#    test_request = requests.get(url)

    logger = logging.getLogger(__name__)
    logger.info('Getting ')

    test_request = call_url()
    forms = test_request.json()['response']
    
#    print forms['60']
#    sys.exit(1)
    
#    form_dict = {forms[k] for k in forms}
#    print form_dict
#    if forms[0] != 'N':    
    form_numbers_dict = [x for x in forms]
    print form_numbers_dict
    
    complete_entries_dict = {}
    
    # The verb structure for retrieving the entries of a given form is:
    # GET /forms/[Form ID]/entries.
    
    for form_number in form_numbers_dict:
      entry_specific_entries = call_url('GET', 'forms/'+str(form_number)+'/entries')
#      print 'Object returned:',entry_specific_entries
    
#    form_dict = [y:forms[y]['id'] for y in form_numbers_dict]
#    print form_dict

    # JSON has the following structure:
    # {u'response': {u'41': {u'entries': u'85',
    #                           u'id': u'41',
    #                           u'title': u'Email Sign-up'},
    #                   u'42': {u'entries': u'128',
    #                           u'id': u'42',
    #                           u'title': u'Hero Form'},
    #                   u'43': {u'entries': u'2',
    #                           u'id': u'43',
    #                           u'title': u'Donate Form'},
    #                   u'44': {u'entries': u'0',
    #                           u'id': u'44',
    #                           u'title': u'Email Pop Up Form'},
    #                   u'51': {u'entries': u'30',
    #                           u'id': u'51',
    #                           u'title': u'Over-Regulation Form'}
    
#    for form in forms:
#      entry = forms[form]
#      id_number = entry['id']
#      num_of_entries = entry['entries']
#      form_title = entry['title']
      
    get_url = 'http://www.reclaimamericapac.com/gravityformsapi/'
    route = 'entries'
    
    recent_entries = call_url('GET', route)
    recent_entries_json = recent_entries.json()['response']
    
#    total_count_entries = recent_entries_json['total_count']
#    print 'Total possible entries:', total_count_entries
#    print recent_entries_json
#    print recent_entries_json['entries'][0]

    import csv
    
#    recent_entries_output = {}
    
    with open('recent_entries.csv','w') as f:
        recent_entries_output = csv.writer(f)
#      for row in recent_entries_json:   
        for entry in recent_entries_json['entries']:
          try:
#          print entry
              payment = entry['payment_method']
              payment_amount = entry['payment_amount']
              payment_status = entry['payment_status']
              payment_date = entry['payment_date']
              transaction_id = entry['transaction_id']
              transaction_type = entry['transaction_type']
              currency = entry['currency']
              
              id = entry['id']
              created_by = entry['created_by']
              first_name = entry['1']
              last_name = entry['2']
              zip_code = entry['3']
              email_address = entry['4']
              
              ip = entry['ip']
              source_url = entry['source_url']
              facebook_profile_link = entry['5']

              status = entry['status']
              is_starred = entry['is_starred']
              date_created = entry['date_created']
              
              user_agent = entry['user_agent']

              print source_url.split('&utm_')              

          except KeyError, e:
              continue

          # TODO:  v Change the below to keep only email, entry id, entry date, source_url (separated by '&utm_<x>=y', for x={source, medium, content, campaign}.

 
#          sys.exit(1)
          
          recent_entries_output.writerow((first_name, last_name, email_address, \
                                          facebook_profile_link, zip_code, created_by, status,\
                                          is_starred, date_created, user_agent, payment_date,\
                                          payment_status, currency, payment_amount, transaction_id,
                                          transaction_type))  


      
#      form_retrieval_route = 'forms/entries'

      
      
#      print email_address
      
     # Next, grab all entries for all forms:
    
#    print recent_entries_json['response']
    
#    print recent_entries.json()
    
    
    
    

    


