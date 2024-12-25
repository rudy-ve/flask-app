
sqlDefinitions={}

sqlDefinitions['getBudget'] = '''
with dateInfo as (
  select %s as year , %s as idPeriod
) ,
periodStats as (
select  py.id_period,
        sum (case 
            when cd.amount_payed is not null then cd.amount_payed
    			  when c.cost_type = 'fixed' then c.amount
  	   		  else cd.amount_flex  	
     	 end) as year_amount_to_pay ,
       sum(cd.amount_payed) as year_amount_payed  
from cost_detail cd 
      inner join cost c on c.id_cost = cd.id_cost 
      inner join period_year py on py.id_period_year = cd.id_period_year
      inner join dateInfo di on di.year=py."year" 
      inner join "period" p on p.id_period = py.id_period
    where c.cost_type != 'budget'
      group by py.id_period 
), 
yearTotal as (
select sum(year_amount_to_pay)      as year_amount_to_pay ,
       sum(year_amount_payed)       as year_amount_payed ,
       sum(year_amount_to_pay)/12   as avg_amount_to_pay ,
       max(year_amount_to_pay)      as max_month_amount_to_pay
  from periodStats  
)
,monthTotal as (
   select 
     sum (case 
  			  when c.cost_type = 'fixed' and cd.payment_state  = 'none' then c.amount
  			  when c.cost_type = 'variabel' and cd.payment_state  = 'none' then cd.amount_flex
  	   		  else 0 	
     	 end
     ) as month_remaining_amount_to_pay,
     sum (case 
  			  when c.cost_type = 'fixed' then c.amount
  	   		  else cd.amount_flex  	
     	 end) as month_amount_to_pay ,
     sum(cd.amount_payed) as month_amount_payed
    from cost_detail cd 
      inner join cost c on c.id_cost = cd.id_cost 
      inner join period_year py on py.id_period_year = cd.id_period_year
      inner join dateInfo di on di.year=py."year" and di.idPeriod = py.id_period 
    where c.cost_type != 'budget'
)
select c.id_cost, cd.id_cost_detail ,
       cd.payment_state , cd.amount_payed,2  , cd.payment_date , cd.not_payed_reason 
       ,yt.year_amount_to_pay , yt.year_amount_payed , yt.avg_amount_to_pay ,
       yt.max_month_amount_to_pay
       ,mt.month_amount_to_pay , mt.month_amount_payed , mt.month_remaining_amount_to_pay
   from cost  c
     inner join cost_detail cd on cd.id_cost = c.id_cost
     inner join period_year py on py.id_period_year = cd.id_period_year
     inner join dateInfo di on di.year=py."year" and di.idPeriod = py.id_period 
     , yearTotal yT
     , monthTotal mt
  where c.cost_type  = 'budget' 
    and c.name = 'budget'
 ; 
'''
