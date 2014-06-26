import requests

key = ''
gravity_id = ''
gravity_secret_key = ''  # May not need this...

def calculate_signature(string_to_sign, private_key):

    from hashlib import sha1
    import hmac, binascii

    hashed = hmac.new(private_key, string_to_sign, sha1)

    return binascii.b2a_base64(hashed.digest().rstrip('\n')

    #pass


def expiration_timestamp(lifetime=4):
    import time
    # Try a lifetime of four hours, which will be ocnverted into current time + 4 hr in UNIX UTC:
    return time.time() + 1200


if __name__ == '__main__':
#    base_url = 'http://mydomain.com/gravityformsapi'
    base_url = 'http://reclaimamericapac.com/gravityformsapi/'

    public_key = '5dd413313e'
    private_key = '5518f270a33c1c6'

    base_url += 'forms'
    base_url += 

    # base_url = 'http://mydomain.com/wordpress/gravityformsapi/'  # <-- if WP install is in a subdirectory.
    # If you want a resource (either forms or entries), append '/forms' or '/entries' to the url:

    base_url += '/entries?api_key=' + public_key
    base_url += '&signature=' + sign_signature(base_url, private_key)
    # string_to_sign = api_key+method+route+expiration => public_key + 'GET' + 'forms/1/entries'
    base_url += '&expires=' + expiration_timestamp()

    string_to_sign = public_key + ':GET' + ':' + method + ':' + expiration_timestamp()

    signature = calculate_signature(string_to_sign, private_key)

    test_request = requests.get(base_url)
    print test_request
    


