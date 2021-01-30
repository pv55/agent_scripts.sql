with leads as (

WITH peoples AS ( Select
d.fleet_gk as fleet_gk ,
f1.courier_gk as driver_gk ,
d.phone as phone ,
d.registration_date_key as registration_date_key ,
d.driver_name as driver_name ,
d.ftp_date_key as ftp_date_key,
d.ltp_date_key  as  ltp_date_key,
max(date(f1.scheduled_at)) as churn

FROM "emilia_gettdwh"."dwh_dim_drivers_v" d
left join model_delivery.dwh_fact_deliveries_v f1 on d.driver_gk = f1.courier_gk
left join model_delivery.dwh_fact_deliveries_v f2 on d.driver_gk = f2.courier_gk


where 1=1

and d.fleet_gk in (200016359,200014202,200016265,200017204,200016266,200014203,200016267)
and date(d.ltp_date_key) >= date'2021-01-01'
and f1.delivery_index <>-1
and f1.delivery_status_id = 4
and date(f1.scheduled_at) >= date'2020-11-03'
and date_diff('day',f1.scheduled_at, f2.scheduled_at) >= 30
and date_diff('day',f1.scheduled_at, f2.scheduled_at) <= 59

group by  fleet_gk,f1.courier_gk,phone,registration_date_key,driver_name,ftp_date_key,ltp_date_key)


(Select
f.fleet_gk as fleet_gk,
a.courier_gk,
f.phone as phone,
f.driver_name as driver_name,
f.registration_date_key as registration_date_key,
f.ftp_date_key as ftp_date_key,
(select
s.churn
from peoples s
Where a.courier_gk = s.driver_gk
) as churn_date,
min(date(a.scheduled_at)) ReFTR_date,
min(date(a.scheduled_at)) + interval '30' day ReFTR_date_plus_1_month,
max(date(a.scheduled_at)) last_ride

from model_delivery.dwh_fact_deliveries_v a
JOIN peoples f on a.courier_gk = f.driver_gk


WHERE 1=1
--and a.courier_gk = 2000860389

and a.delivery_status_id = 4
and a.delivery_index <>-1

and date(a.scheduled_at) > date(f.churn)


Group by a.courier_gk,f.fleet_gk,f.phone,f.driver_name,f.registration_date_key,f.ftp_date_key))

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

count(distinct (case when z.scheduled_at
                    between date(l.ReFTR_date) and date(l.ReFTR_date) + interval '7' day
                then z.journey_gk end)) rides_7_days,
count(distinct (case when z.scheduled_at
                    between date(l.ReFTR_date) and date(l.ReFTR_date) + interval '14' day
                then z.journey_gk end)) rides_14_days,
count(distinct(case when z.scheduled_at
                    between date(l.ReFTR_date) and date(l.ReFTR_date) + interval '21' day
                then z.journey_gk end)) rides_21_days,
count(distinct(case when z.scheduled_at
                    between date(l.ReFTR_date) and date(l.ReFTR_date_plus_1_month)
                then z.journey_gk end)) rides_1st_month

from leads l
left join model_delivery.dwh_fact_deliveries_v z on l.courier_gk =  z.courier_gk

Where date_diff('day',l.churn_date, l.ReFTR_date) BETWEEN 30 AND 59
and l.ReFTR_date >= date '2021-01-01'
and z.delivery_status_id = 4
and z.delivery_index <>-1

Group by l.fleet_gk,l.courier_gk,l.phone,l.driver_name,l.registration_date_key,l.ftp_date_key,l.churn_date,l.ReFTR_date,l.ReFTR_date_plus_1_month,l.last_ride
Order by l.last_ride desc
    )
