import time
import hmac
import hashlib
from requests.auth import AuthBase

class BitsoAuth(AuthBase):
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def __call__(self, request):
        nonce = str(int(time.time() * 1000))
        method = request.method
        request_path = request.path_url
        if method == 'POST':
            body = request.body or ''
        else:
            body = ''

        message = nonce + method + request_path + body
        print(f'Message: {message}')
        signature = hmac.new(self.api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()

        request.headers.update({
            'Authorization': f'Bitso {self.api_key}:{nonce}:{signature}',
            'Content-Type': 'application/json'
        })
        return request