import sys
sys.path.insert(1,'../flaskConfig')


from clsPgDatabase import pgDatabase;
from setup import getSetup;
import datetime

dbSetup=getSetup('budgetDb')
db=pgDatabase(dbSetup['host'],dbSetup['port'],dbSetup['database'],dbSetup['schema'],dbSetup['user'],dbSetup['password']);

def checkYear():
    today = datetime.date.today()
    currentYear = today.year
    row = db.fetchOne("select count(*) as cnt from year where year = %s ",[currentYear])
    if (row['cnt'] < 1):    
       row=db.fetchOne("select max(year) as last_year from year ")
       lastYear=row['last_year'];

       db.execute("insert into year (year) values (%s)" , [currentYear]) 
       sqlPeriodYear  = "insert into period_year (year,id_period )  "
       sqlPeriodYear += "select %s, id_period from period_year where year = %s"
       db.execute(sqlPeriodYear , [currentYear,lastYear]) 

       sqlCreateNewYearCost  = " with input as ( select %s as newYear , %s as oldYear)"
       sqlCreateNewYearCost += " insert into cost_detail (id_cost , payment_state, amount_flex , id_period_year )"
       sqlCreateNewYearCost += " select distinct cd.id_cost ,  'none' as paymentState , cd.amount_flex  ,    "
       sqlCreateNewYearCost += "     (select id_period_year  "
       sqlCreateNewYearCost += "       from period_year py2 "
       sqlCreateNewYearCost += "        inner join input i2 on i2.newYear = py2.year  "
       sqlCreateNewYearCost += "       where py2.id_period = py.id_period ) as id_period_year"
       sqlCreateNewYearCost += "  from  cost_detail cd  "
       sqlCreateNewYearCost += "  inner join period_year py on py.id_period_year = cd.id_period_year "
       sqlCreateNewYearCost += "  inner join input i on i.oldYear = py.year "
       db.execute(sqlCreateNewYearCost,[currentYear,lastYear])
       db.commit();

    else: 
        print ("year ok " ) 

checkYear();
