"""Binance.com cryptocurrency Exchange API."""

import requests

# private query timestamp
import time

# private query signing
import urllib.parse
import hashlib
import hmac
import base64
import os

class BinanceAPI(object):
    

    def __init__(self, key='', secret=''):
        """ Create an object with authentication information.
        :param key: (optional) key identifier for queries to the API
        :type key: str
        :param secret: (optional) actual private key used to sign messages
        :type secret: str
        :returns: None
        """
        self.key = key
        self.secret = secret
        self.uri = 'https://api.binance.com'
        self.apiversion = 'v3'
        self.session = requests.Session()
        self.response = None
        self._json_options = {}
        self._timestamp = int(1000*time.time())
        return

    def json_options(self, **kwargs):
        """ Set keyword arguments to be passed to JSON deserialization.
        :param kwargs: passed to :py:meth:`requests.Response.json`
        :returns: this instance for chaining
        """
        self._json_options = kwargs
        return self

    def close(self):
        """ Close this session.
        :returns: None
        """
        self.session.close()
        return

    def load_key(self, path):
        """ Load key and secret from file.
        Expected file format is key and secret on separate lines.
        :param path: path to keyfile
        :type path: str
        :returns: None
        """
        with open(path, 'r') as f:
            self.key = f.readline().strip()
            self.secret = f.readline().strip()
        return

    def _query(self, urlpath, data, headers=None):
        """ Low-level query handling.
        .. note::
           Use :py:meth:`query_private` or :py:meth:`query_public`
           unless you have a good reason not to.
        :param urlpath: API URL path sans host
        :type urlpath: str
        :param data: API request parameters
        :type data: dict
        :param headers: (optional) HTTPS headers
        :type headers: dict
        :param timeout: (optional) if not ``None``, a :py:exc:`requests.HTTPError`
                        will be thrown after ``timeout`` seconds if a response
                        has not been received
        :type timeout: int or float
        :returns: :py:meth:`requests.Response.json`-deserialised Python object
        :raises: :py:exc:`requests.HTTPError`: if response status not successful
        """
        if data is None:
            data = {}
        if headers is None:
            headers = {}



        url = self.uri + urlpath
        self.response = self.session.get(url, params = data, headers = headers)

        if self.response.status_code not in (200, 201, 202):
            self.response.raise_for_status()

        return self.response.json(**self._json_options)


    def query_public(self, method, data=None):
        """ Performs an API query that does not require a valid key/secret pair.
        :param method: API method name
        :type method: str
        :param data: (optional) API request parameters
        :type data: dict
        :param timeout: (optional) if not ``None``, a :py:exc:`requests.HTTPError`
                        will be thrown after ``timeout`` seconds if a response
                        has not been received
        :type timeout: int or float
        :returns: :py:meth:`requests.Response.json`-deserialised Python object
        """
        if data is None:
            data = {}

        urlpath = '/' + 'api' + '/' + self.apiversion + "/"  + method

        return self._query(urlpath, data)

    def query_private(self, method, data=None):
        """ Performs an API query that requires a valid key/secret pair.
        :param method: API method name
        :type method: str
        :param data: (optional) API request parameters
        :type data: dict
        :param timeout: (optional) if not ``None``, a :py:exc:`requests.HTTPError`
                        will be thrown after ``timeout`` seconds if a response
                        has not been received
        :type timeout: int or float
        :returns: :py:meth:`requests.Response.json`-deserialised Python object
        """
        if data is None:
            data = {}

        if not self.key or not self.secret:
            raise Exception('Either key or secret is not set! (Use `load_key()`.')

        data['timestamp'] = self._timestamp
        data['signature'] = self._sign(data)
        urlpath = '/' + 'api' + '/' + self.apiversion + "/" + method

        headers = {
            'X-MBX-APIKEY': self.key
        }

        return self._query(urlpath, data, headers)

    def _sign(self, data):
        """ Sign request data according to Kraken's scheme.
        :param data: API request parameters
        :type data: dict
        :param urlpath: API URL path sans host
        :type urlpath: str
        :returns: signature digest
        """
        message = urllib.parse.urlencode(data)
        #print(message)
        # Create signature with timestamp, query/data and api secret
        signature = hmac.new(bytes(self.key, 'utf-8'), bytes(message, 'utf-8'), hashlib.sha256)
        print(f"python sign : {signature.hexdigest()}")
        
        
        req = os.popen(f"echo -n 'recvWindow=10000&timestamp={self._timestamp}' | openssl dgst -sha256 -hmac '{self.secret}'").read().split('= ')
        print(f"openSSL sign : {req[1].strip()}")
        return req[1].strip()


if __name__ == '__main__':
    
    api = BinanceAPI(key=os.environ.get("BINANCE_API_KEY"), secret=os.environ.get("BINANCE_API_SECRET"))
    data = {
        "recvWindow": 10000
        #"symbol": "AAVEUSDT"
    }

    request = api.query_private(f'account', data=data)
    print(request)
    api.close()

