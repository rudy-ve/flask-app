# Import flask module
from flask import Flask, request 
from flask_cors import CORS
import sys
sys.path.insert(1,'../flaskConfig')
from setup import getSetup;

from authModule import authorized
import json
from clsPgDatabase import pgDatabase;



app = Flask(__name__)
app.config.update({
    #'SECRET_KEY': 'SomethingNotEntirelySecret',
    'TESTING': True,
    'DEBUG': True
})
CORS(app)

import budget;
app.register_blueprint(budget.bp)

import test;
app.register_blueprint(test.bp)

dbSetup=getSetup('budgetDb')
db=pgDatabase(dbSetup['host'],dbSetup['port'],dbSetup['database'],dbSetup['schema'],dbSetup['user'],dbSetup['password']);


 
@app.route('/')
def index():
    row = db.fetchAll("select version() as vers")
    
    return 'Hello to Flask! new <br>' + row[0]['vers']

@app.route('/getTry', methods=['GET'])
def getTry():
    try:
        para=request.args.get('test')
        return "Para is " + para
    except:
        return " no para "

@app.route('/getCostTypes', methods=['GET'])
def getCostTypes():
    retval = db.executeJson('select * from ref_cost_type');
    return jsonify(retval)

@app.route('/tryAuth', methods=['GET'])
@authorized
def tryAuth():
    return "AUTH OK"

# main driver function
if __name__ == "__main__":
    app.run()


