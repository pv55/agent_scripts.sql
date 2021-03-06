--ReFTR with 30 days Gorizont agent 
      
with leads as (

WITH peoples AS (

Select
f1.fleet_gk as fleet_gk ,
f1.driver_gk as driver_gk ,
d.phone as phone ,
d.registration_date_key as registration_date_key ,
d.driver_name as driver_name ,
d.ftp_date_key as ftp_date_key,
d.ltp_date_key  as  ltp_date_key,
max(date(f1.order_date_key)) as churn

FROM "emilia_gettdwh"."dwh_dim_drivers_v" d
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f1 on d.driver_gk = f1.driver_gk
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f2 on d.driver_gk = f2.driver_gk


where 1=1

and f1.fleet_gk in (200016359,200014202,200016265,200017204,200016266,200014203,200016267)
and date(d.ltp_date_key) >= date'2021-01-01'
and f1.country_key = 2
and f1.order_status_key = 7
and f1.cost_exc_vat >=1
and date(f1.order_date_key) >= date'2020-11-03'
and date_diff('day',f1.order_date_key, f2.order_date_key) >= 30
and date_diff('day',f1.order_date_key, f2.order_date_key) <= 59

group by  f1.fleet_gk,f1.driver_gk,phone,registration_date_key,driver_name,ftp_date_key,ltp_date_key)

(Select
f.fleet_gk as fleet_gk,
a.driver_gk,
f.phone as phone,
f.driver_name as driver_name,
f.registration_date_key as registration_date_key,
f.ftp_date_key as ftp_date_key,
(select
s.churn
from peoples s
Where a.driver_gk = s.driver_gk
) as churn_date,
min(date(a.order_date_key)) ReFTR_date,
min(date(a.order_date_key)) + interval '30' day ReFTR_date_plus_1_month,
max(date(a.order_date_key)) last_ride

from emilia_gettdwh.dwh_fact_drivers_orders_monetization_v a
JOIN peoples f on a.driver_gk = f.driver_gk


WHERE 1=1
--and a.courier_gk = 2000860389

and a.country_key = 2
and a.order_status_key = 7
and a.cost_exc_vat >=1

and date(a.order_date_key) > date(f.churn)


Group by a.driver_gk,f.fleet_gk,f.phone,f.driver_name,f.registration_date_key,f.ftp_date_key))

(
Select
(case
    when l.fleet_gk in (200014202,200014203) then 'Москва'
    when l.fleet_gk in (200016265,200017204) then 'Спб'
    when l.fleet_gk = 200016359 then 'НН'
    when l.fleet_gk = 200016266 then 'Казань'
    when l.fleet_gk = 200016267 then 'Рнд'

end) city,

(case
    when l.fleet_gk in (200016359,200014202,200016265,200016266,200016267) then 'Авто'
    when l.fleet_gk in (200017204,200014203) then 'Пешие'

end) type_couriers,

l.*,

count(distinct (case when z.order_date_key
                    between date(l.ReFTR_date) and date(l.ReFTR_date) + interval '7' day
                then z.order_gk end)) rides_7_days,
count(distinct (case when z.order_date_key
                    between date(l.ReFTR_date) and date(l.ReFTR_date) + interval '14' day
                then z.order_gk end)) rides_14_days,
count(distinct(case when z.order_date_key
                    between date(l.ReFTR_date) and date(l.ReFTR_date) + interval '21' day
                then z.order_gk end)) rides_21_days,
count(distinct(case when z.order_date_key
                    between date(l.ReFTR_date) and date(l.ReFTR_date_plus_1_month)
                then z.order_gk end)) rides_1st_month

from leads l
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v z on l.driver_gk =  z.driver_gk

Where date_diff('day',l.churn_date, l.ReFTR_date) BETWEEN 30 AND 59
and l.ReFTR_date >= date '2021-01-01'
and z.country_key = 2
and z.order_status_key = 7
and z.cost_exc_vat >=1

Group by l.fleet_gk,l.driver_gk,l.phone,l.driver_name,l.registration_date_key,l.ftp_date_key,l.churn_date,l.ReFTR_date,l.ReFTR_date_plus_1_month,l.last_ride
Order by l.last_ride desc
    )
