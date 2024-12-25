
from functools import wraps
from flask import abort, jsonify, request
import requests
import sys
sys.path.insert(1,'../flaskConfig')
from setup import getSetup;


def authorized(f):
    @wraps(f)
    def decorated_func(*args,**kwargs):
        isAuthenticatedUrl=getSetup('isAuthenticatedUrl')
        #print ("isAuthenticatedUrl" , isAuthenticatedUrl )
        ip=request.remote_addr ;
        user=request.headers.get('user')
        token=request.headers.get('token')
        #print ("authInfo" , ip , user ,token , request.headers.get('User'))
        authInfoReq=requests.get(isAuthenticatedUrl, {'User':user, 
                                                   'Token' : token ,
                                                   'Ip':ip} ) ;
        authInfo=authInfoReq.json()
        print ("authInfo" , authInfo  )

        if authInfo['authenticated']:
            return f(*args,**kwargs)
        else:
            response = jsonify({'message':'Authentication failure'})
            abort(401)
    return decorated_func
