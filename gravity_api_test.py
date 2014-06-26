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
    
    
def call_url(url='', method='GET', route='forms'):
  base_url = 'http://www.reclaimamericapac.com/gravityformsapi/'
  public_key = '5dd413313e'
  private_key = '5518f270a33c1c6'
  
  current_time = str(expiration_timestamp())[:-3]
  string_to_sign = public_key + ':' + method + ':' + route + ':' + current_time
  signature = calculate_signature(string_to_sign, private_key)
  url = base_url + route + '/?api_key=' + public_key + '&signature=' + signature + '&expires=' + current_time
  
  request = requests.get(url)
  
#  if request['response'] != 'Not available':  
  return requests.get(url)
#  print 'Waiting two seconds...'
#  time.sleep(2)
  
    
def get_forms(url):
  import time, requests as r
  
  time_to_wait = 5
  # Put code here to call the server.
  
  request = r.get(url)
  
  # Next, if randomly not authorized, try again:
  if output['response'] == 'Not authorized':
    time.sleep(time_to_wait)
    print 'Waiting for %i seconds...' % time_to_wait
    get_forms(url)
    
  print 'Forms retrieved from %s' % url  
  return output

###
if __name__ == '__main__':
  
    import requests

    base_url = 'http://www.reclaimamericapac.com/gravityformsapi/'
    public_key = '5dd413313e'
    private_key = '5518f270a33c1c6'
    method = 'GET'
    route = 'forms'

    string_to_sign = public_key + ':' + method + ':' + route + ':' + str(expiration_timestamp())[:-3]
    signature = calculate_signature(string_to_sign, private_key) 
    url = base_url + route + '/?api_key=' + public_key + '&signature=' + signature + '&expires=' + str(expiration_timestamp())[:-3]
#    test_request = requests.get(url)
#    forms = test_request.json()['response']

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
    
    next_test = call_url(get_url+'entries','GET', route)
    next_test_json = next_test.json()['response']
    
    total_count_entries = next_test_json['total_count']
    print total_count_entries
    
    for entry in next_test_json['entries']:
      payment = entry['payment_method']
      payment_amount = entry['payment_amount']
      payment_status = entry['payment_status']
      payment_date = entry['payment_date']
      currency = entry['currency']
      
      id = entry['id']
      created_by = entry['created_by']
      first_name = entry['1']
      last_name = entry['2']
      email_address = entry['4']
      facebook_profile_link = entry['5']
            
      ip = entry['ip']
      
      print email_address
      
      
    
#    print next_test_json['response']
    
#    print next_test.json()
    
    
    
    

    


