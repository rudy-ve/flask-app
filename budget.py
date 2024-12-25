from flask import  Blueprint ,request
import sys
sys.path.insert(1,'../flaskConfig')

from clsPgDatabase import pgDatabase;
from setup import getSetup;
from authModule import authorized



from sqlLib import sqlDefinitions 
bp=Blueprint('budget',__name__,url_prefix='/budget')


dbSetup=getSetup('budgetDb')
db=pgDatabase(dbSetup['host'],dbSetup['port'],dbSetup['database'],dbSetup['schema'],dbSetup['user'],dbSetup['password']);

def getPostData(request):
    data = None;
    if ('data' in request.form): 
        data = request.form['data]']
    elif ('data' in request.get_json()):
        data = request.get_json()['data']
    return data

@bp.route('alive',methods=['GET'])
def alive():
    return "App is alive and kicking"

@bp.route('getVersionWithAuth',methods=['GET'])
@authorized
def getVersionWithAuth():
    return  db.fetchOne("select 'AUTH -- ' || version() version from ref_cost_type");

# @bp.route('checkYear',methods=['GET'])
# @authorized
# def checkYear():
#     print ("CHECK NEW YEAR ---------------------------------------------------------")     
#     return {'status':'Check year not yet implemented '}

@bp.route('getYears',methods=['GET'])
@authorized
def getYears():
   
    yearList = []
    for year in db.fetchAll("select year  from year") :
        yearList.append({'id':year['year'],'text':year['year']})


    return  {'data':yearList} ;

@bp.route('getPeriods',methods=['GET'])
@authorized
def getPeriods():
    periodList = []
    year =  request.args.get('year') 
    sql  = "select p.id_period as id, p.period_name as text, p.period_type  "
    sql += "  from period p "
    sql += "       inner join period_year py on py.id_period = p.id_period "
    sql += "  where py.year = %s"
    for period in db.fetchAll(sql,[year]) :
         periodList.append( period ) 
    return  {'data':periodList} ; 



@bp.route('getPaymentStateList',methods=['GET'])
@authorized
def getPaymentStateList():
    paymentStateList = []
    sql  = 'select payment_state as id, payment_state as text from ref_payment_state'
    for row in db.fetchAll(sql) :
      paymentStateList.append(row)
    return  {'data':paymentStateList} ; 


@bp.route('getPaymentTypeList',methods=['GET'])
@authorized
def getPaymentTypeList():
    paymentTypeList = []
    sql  = 'select payment_type as id, payment_type as text from ref_payment_type'
    for row in db.fetchAll(sql) :
      paymentTypeList.append(row)
      
    return  {'data':paymentTypeList} ; 

@bp.route('getCostTypeList',methods=['GET'])
@authorized
def getCostTypeList():
    paymentCostList = []
    sql  = 'select cost_type as id, cost_type as text from ref_cost_type'
    for row in db.fetchAll(sql) :
      paymentCostList.append(row)
    return  {'data':paymentCostList} ; 

@bp.route('getCostList',methods=['GET'])
@authorized
def getCostList():
    year   =  request.args.get('year') 
    period =  request.args.get('idPeriod') 
    costList = []
    sql  = 'select c.id_cost  , c.cost_type , c.payment_type  ,c.name  , c.amount,  '
    sql += '       cd.payment_state , cd.amount_payed   , cd.amount_flex  , cd.payment_date ,  '
    sql += '       cd.not_payed_reason , cd.id_cost_detail ,'
    sql += '       py.year , py.id_period  '
    sql += ' from cost c'
    sql += '   inner join cost_detail cd  on c.id_cost=cd.id_cost '
    sql += '   inner join period_year py on py.id_period_year = cd.id_period_year '
    sql += '   inner join ref_cost_type ct on ct.cost_type = c.cost_type'
    sql += '   where ct.cost_type_show and py.year=%s and py.id_period=%s '
    for row in db.fetchAll(sql,[year,period]) :
      costList.append( {"idCost":row['id_cost'] ,
                        "costType":row['cost_type'],
                        "payment_type":row['payment_type'],
                        "name":row['name'],
                        "amount":row['amount'],
                        "detail":{"idCostDetail":row['id_cost_detail'] ,
                                  "paymentState":row['payment_state'] ,
                                  "amountPayed":row['amount_payed'] ,
                                  "amountFlex":row['amount_flex'] ,
                                  "paymentDate":row['payment_date'] ,
                                  "notPayedReason":row['not_payed_reason'] ,
                                  "year":row['year'] ,
                                  "idPeriod":row['id_period'] ,
                                  }
                      } )
    return  {'data':costList } ; 


@bp.route('updateCost',methods=['POST'])
@authorized   
def updateCost():
    data = getPostData(request);
    idCost = data['idCost']

    parameters = [] 
    items = [
        {'key':'costType',    'field' : 'cost_type'},
        {'key':'paymentType', 'field' : 'payment_type'},
        {'key':'name',        'field' : 'name'},
        {'key':'amount',      'field' : 'amount'}
    ]
    separator = ""
    sql = " update  cost set "
    for item in items:
        if (item['key'] in data ):
            sql += separator + item['field'] + ' = %s '
            parameters.append(data [item['key']])
            separator = ", "

    if ( len(parameters) > 0 ) :
        sql += " where id_cost = %s "
        parameters.append(idCost)
        db.execute(sql,parameters)
        db.commit()
        return {'status' : 'Done'}
    else:
        return {'status' : "Nothing to update"}

@bp.route('updateCostDetail',methods=['POST'])
@authorized
def updateCostDetail():
    data = getPostData(request);
    idCostDetail = data['idCostDetail']
    parameters = [] 
    items = [
        {'key':'paymentState',      'field' : 'payment_state'},
        {'key':'amountPayed',       'field' : 'amount_payed'},
        {'key':'amountFlex',        'field' : 'amount_flex'},
        {'key':'paymentDate',       'field' : 'payment_date'},
        {'key':'notPayedReason',    'field' : 'not_payed_reason'}
    ]
    separator = ""
    sql = " update  cost_detail set "
    for item in items:
        if (item['key'] in data ):
            sql += separator + item['field'] + ' = %s '
            if ( data [item['key']] == '' ) :
                parameters.append(None )
            else:
                parameters.append(data [item['key']] )
            separator = ", "

    if ( 'year' in data):
        sqlPeriod= '' ;
        parametersPeriod = [];
        if ( 'idPeriod' in data) :
            sqlPeriod = 'select id_period_year from period_year where year = %s and id_period = %s ';
            parametersPeriod = [data['year'] , data['idPeriod']]  
        else:
            return {'status' : 'Missing idPeriod'}
        if ( len (parametersPeriod) > 0 ) :
            row = db.fetchOne(sqlPeriod,parametersPeriod)
            sql += separator + 'id_period_year = %s '
            parameters.append(row['id_period_year'])

    if ( len(parameters) > 0 ) :
        sql += " where id_cost_detail = %s "
        parameters.append(idCostDetail)
        db.execute(sql,parameters)
        db.commit()
        return {'status' : 'Done'}
    else:
        return {'status' : "Nothing to update"}

def __addCostDetail(data):
    idCost = data['idCost']
    parameters = [] 
    items = [
        {'key':'paymentState',      'field' : 'payment_state'},
        {'key':'amountPayed',       'field' : 'amount_payed'},
        {'key':'amountFlex',        'field' : 'amount_flex'},
        {'key':'paymentDate',       'field' : 'payment_date'},
        {'key':'notPayedReason',    'field' : 'not_payed_reason'}
    ]
    
    separator = ", "
    parameters = [idCost] 
    sql = " insert into cost_detail ( id_cost  "
    sqlValues = " %s "
    for item in items:
        if (item['key'] in data ):
            sql += separator + item['field'] 
            parameters.append(data [item['key']])
            sqlValues += separator + "%s "

 
    sqlPeriod = 'select id_period_year from period_year where year = %s and id_period = %s ';
    parametersPeriod = [data['year'] , data['idPeriod']]  
    row = db.fetchOne(sqlPeriod,parametersPeriod)
    sql += separator + 'id_period_year'
    sqlValues += ",%s "
    parameters.append(row['id_period_year'])
    
    sql += " ) Values ( " + sqlValues + ") returning id_cost_detail "
    row = db.fetchOne(sql,parameters)
    db.commit()
    return {'status':'Done', 'idCostDetail':row['id_cost_detail']}

@bp.route('addCostDetail',methods=['POST'])
@authorized
def addCostDetail():
    data = getPostData(request); 
    return __addCostDetail(data)   

@bp.route('addCost',methods=['POST'])
@authorized
def addCost():
    retval = {'status':'Done'}
    data = getPostData(request);
    if ( data is None):
        return  {'status':'No data delivered' }
    parameters = [] 
    items = [
        {'key':'costType',    'field' : 'cost_type'},
        {'key':'paymentType', 'field' : 'payment_type'},
        {'key':'name',        'field' : 'name'},
        {'key':'amount',      'field' : 'amount'}
    ]
    separator = ""
    sqlStart = " insert into  cost ( "
    valueSql = " "
    for item in items:
        if (item['key'] in data ):
            sqlStart += separator + item['field'] 
            valueSql += separator + ' %s ' 
            parameters.append(data [item['key']])
            separator = ", "

    sql = sqlStart + ') VALUES (' + valueSql + ') returning id_cost'
    row = db.fetchOne(sql,parameters)
    db.commit()
    idCost=row['id_cost'];

    for detail in data['details']:
        detail['idCost']=idCost
        __addCostDetail(detail)
    
    db.commit()
    return {'status' : 'Done' , 'idCost' : row['id_cost']}

@bp.route('getCostWithDetails',methods=['Get'])
@authorized
def getCostWithDetails():

    idCost = request.args.get('idCost') 
    year = request.args.get('year') 
    sql  = ' select c.id_cost as "idCost", c.cost_type as "costType" , c.payment_type as "paymentType" , c.name as "name" , '
    sql += '        c.amount as amount,'
    sql += " (select array_agg(el)  "
    sql += "    from (select id_cost , json_build_object ('idCostDetail',cd.id_cost_detail  , 'year' , py.year"
    sql += "                                             , 'idPeriod', p.id_period  , 'paymentState' , cd.payment_state"
    sql += "                                             , 'amountPayed' , cd.amount_payed , 'amountFlex' , cd.amount_flex "
    sql += "                                             , 'paymentDate' , cd.payment_date , 'notPayedReason' , cd.not_payed_reason "
    sql += "                                             ) as el "
    sql += "            from cost_detail cd   "
    sql += "              inner join period_year py on py.id_period_year = cd.id_period_year "
    sql += "              inner join period p on p.id_period = py.id_period "
    sql += "              where id_cost =c.id_cost "
    sql += "                 and py.year = %s "
    sql += "         ) as t group by id_cost "
    sql += " ) as details"
    sql += ' from cost c where c.id_cost = %s ' 

    row = db.fetchOne(sql,[year,idCost])
    return  dict(row)

@bp.route('deleteCostDetail',methods=['POST'])
@authorized
def deleteCostDetail():
    data = getPostData(request);
    if ( data is None):
        return  {'status':'No data delivered' }
    idCostDetail = data['idCostDetail']
    sql = " delete from cost_detail cd where id_cost_detail = %s " 
    db.execute(sql,[idCostDetail])
    db.commit();
    return {'status' : 'Done'  }

@bp.route('getBudget',methods=['GET'])
@authorized
def getBudget():
    idPeriod = request.args.get('idPeriod') 
    year = request.args.get('year') 
    row = db.fetchOne(sqlDefinitions['getBudget'], [int(year),int(idPeriod)])
    retval = {
        'idCost' : row['id_cost'],
        'idCostDetail' : row['id_cost_detail'],
        'paymentState' : row['payment_state'],
        'amountPayed' : row['amount_payed'],
        'paymentDate' : row['payment_date'],
        'notPayedReason' : row['not_payed_reason'],
        'yearAmountToPay' : row['year_amount_to_pay'],
        'yearAmountPayed' : row['year_amount_payed'],
        'avgAmountToPay' : row['avg_amount_to_pay'],
        'maxMonthAmountToPay' : row['max_month_amount_to_pay'],
        'monthAmountToPay' : row['month_amount_to_pay'],
        'monthAmountPayed' : row['month_amount_payed'],
        'monthRemainingAmountToPay' : row['month_remaining_amount_to_pay']
    }
    return {'status' : 'Done' , 'budget':retval }


@bp.route('deleteCost',methods=['POST'])
@authorized
def deleteCost():
    return {'status' : 'Delete cost not yet implemented' }
    # data = getPostData(request);
    # if ( data is None):
    #     return  {'status':'No data delivered' }
    # idCost = data['idCost']
    # sql = " delete from cost_detail where id_cost = %s " 
    # db.execute(sql,[idCost])
    # sql = " delete from cost  where id_cost = %s "
    # db.execute(sql,[idCost])
    # db.commit();
    # return {'status' : 'Done' }

@bp.route('getOpenPeriods',methods=['GET'])
@authorized
def getOpenPeriods():
    sql  = ' select distinct py.year as year, py.id_period as "idPeriod", p.period_name as "periodName"'
    sql += ' from  cost_detail cd '
    sql += '   inner join period_year py on py.id_period_year = cd.id_period_year'
    sql += '   inner join "period" p on p.id_period = py.id_period'
    sql += "   where (    py.year < date_part('year', CURRENT_DATE)"
    sql += ' 		  or p.id_period < extract ( month from CURRENT_DATE) '
    sql += ' 		) '
    sql += " 		and cd.payment_state  = 'none'"
    rows = db.fetchAll(sql)
    return  rows

@bp.route('getBudgetPrediction',methods=['GET'])
@authorized
def getBudgetPrediction():
    sql  = ' select month_order_nr as "monthOrderNr" , budget_amount as "budgetAmount", year , id_period as "idPeriod" , period_name as "periodName", '
    sql += ' month_amount_to_pay "monthAmountToPay", total_year "totalYear", avg_month "avgMonth", to_budget "toBudget" ,budget_prediction as "budgetPrediction"'
    sql += ' from  v_budget_prediction'
    sql += '  order by v_budget_prediction'
    rows = db.fetchAll(sql)
    return  rows

@bp.route('getBudgetAmounts',methods=['GET'])
@authorized
def getBudgetAmounts():
    months = request.args.get('months') 
    if (months is None):
        months=12

    sql  = 'select   py.year , py.id_period as "idPeriod", p.period_name as "periodName", py.budget_amount as "budgetAmount" '
    sql += '  from period_year py '
    sql += "   inner join period p on p.id_period = py.id_period   and p.period_type  = 'month' "
    sql += "   where py.year*100 + py.id_period <= extract(year from (now() + interval '1 month') )*100 + extract(month from now() + interval '1 month') "
    sql += '  order by py.year*100 + py.id_period  desc '
    sql += '   limit %s '

    rows = db.fetchAll(sql,[months])
    return  rows

@bp.route('updateBudgetAmount',methods=['POST'])
@authorized
def updateBudgetAmount():
    data = getPostData(request);
    year = data['year'] 
    idPeriod = data['idPeriod']
    budgetAmount = data['budgetAmount']
    print ("updateBudgetAmount year" , year)
    
    sql  = "update period_year set budget_amount =%s "
    sql += " where year=%s and id_period=%s "

    rows = db.execute(sql,[budgetAmount,year,idPeriod])
    db.commit()
    return {'status' : 'Done'  }


@bp.route('getCostHistory',methods=['GET'])
@authorized
def getCostHistory():

    sql  = 'select id_cost as "idCost", cost_type as "costType" , name , countcost as "countCost", avgcost as "avgCost", yearcost as "yearCost" '
    sql += ", array_agg(  json_build_object ('year',year , 'count' ,countPayed , 'avg', avgPayed , 'total' ,yearPayed) ) as history  "
    sql += " from v_allCost  "
    sql += " group by id_cost , cost_type , name , countcost, avgcost , yearcost "

    rows = db.fetchAll(sql)
    return  rows