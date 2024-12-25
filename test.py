from flask import  Blueprint ,request
import psycopg2
from clsPgDatabase import pgDatabase;
import sys
sys.path.insert(1,'../flaskConfig')

import time

from setup import getSetup;

bp=Blueprint('test',__name__,url_prefix='/test')

dbSetup=getSetup('budgetDb')
db=pgDatabase(dbSetup['host'],dbSetup['port'],dbSetup['database'],dbSetup['schema'],dbSetup['user'],dbSetup['password']);



@bp.route('alive',methods=['GET'])
def alive():
    return "Alive and kicking"

@bp.route('getVersion',methods=['GET'])
def getVersion():
    return  db.fetchOne('select version() version from ref_cost_type');

@bp.route('getVersionWithAuth',methods=['GET'])
def getVersionWithAuth():
    return  db.fetchOne("select 'AUTH -- ' || version() version from ref_cost_type");
    
@bp.route('getYears',methods=['GET'])
def getYears():
    years = []
    try:
        print (" GETTING YEARS ")
        return  db.fetchAll("select year from year");
   
    except Exception as e:
        print ('E ', f'Error {e}' )
        db.close()
        db.connect()
        return [ { 'year':'Error found' } ]


@bp.route('getSqlError',methods=['GET'])
def getSqlError():
    return  db.fetchOne("select year_error from year");
