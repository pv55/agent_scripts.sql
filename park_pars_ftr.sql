WITH Payment AS (

WITH couriers AS (

select
(case when f1.fleet_gk in( 200017111) then 'МСК'
      end ) city,
d.driver_gk as driver_gk,
d.phone as phone,
(case when d.courier_type is null then 'car' else d.courier_type end ) courier_type,
d.driver_name as driver_name,
d.driver_computed_rating as driver_computed_rating,
f1.fleet_gk as fleet_gk,
d.driver_status as driver_status,
(case when d.frozen_comment = 'Unknown' then '' else d.frozen_comment end ) status,


d.registration_date_key as registration_date_key,
d.ftp_date_key as ftp_date_key_all,
min(f1.order_date_key) as ftp_date_key_park,
min(f1.order_date_key) + interval '30' day FTR_plus_30days,
max(f1.order_date_key) as ltp_date_key,
sum (f1.cost_exc_vat) cost_total

from "emilia_gettdwh"."dwh_dim_drivers_v" d
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f1 on d.driver_gk = f1.driver_gk


where 1=1
and f1.fleet_gk in (  200017111)
and f1.country_key = 2
and f1.order_status_key = 7
and f1.cost_exc_vat >=1



Group by d.driver_gk,(case when d.courier_type is null then 'car' else d.courier_type end ),d.phone,d.driver_name,d.driver_computed_rating,f1.fleet_gk,d.driver_status,d.registration_date_key,d.ftp_date_key,(case when d.car_number = 'ЧС' then 'ЧС' end),(case when d.frozen_comment = 'Unknown' then '' else d.frozen_comment end ))

(SELECT a.*,
count(distinct(case when f2.fleet_gk in (200017111) then (case when f2.order_date_key between a.ftp_date_key_park and a.FTR_plus_30days then f2.order_gk end)  end)) as All_rides_30_days,

count(distinct(case when f2.fleet_gk in (200017111) then f2.order_gk  end)) as All_rides_total,
max (case when a.ftp_date_key_all <> a.ftp_date_key_park then (case when f2.order_date_key  < a.ftp_date_key_park  then f2.order_date_key end) end ) ltp_date_different_park,


count(distinct (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.order_gk end)) rides_7_days,
sum (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.cost_inc_vat end) cumsum_7_days,
count(distinct (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.order_gk end)) rides_7_to_14_days,
sum (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.cost_inc_vat end) cumsum_7_to_14_days,
count(distinct (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.order_gk end)) rides_15_to_21_days,
sum (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.cost_inc_vat end) cumsum_15_to_21_days,
count(distinct (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.order_gk end)) rides_16_to_30_days,
sum (case when f2.fleet_gk in (200017111) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.cost_inc_vat end) cumsum_16_to_30_days



from couriers a
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f2 on a.driver_gk = f2.driver_gk

where 1=1

and f2.country_key = 2
and f2.order_status_key = 7
and f2.cost_exc_vat >=1
--and a.FTR_plus_30days >= date '2021-02-01'

GROUP by a.city,a.driver_gk,a.cost_total,a.courier_type,a.phone,a.driver_name,a.driver_computed_rating,a.fleet_gk,a.driver_status,a.status,a.registration_date_key,a.ftp_date_key_all,a.ftp_date_key_park,a.FTR_plus_30days,a.ltp_date_key))

(SELECT
s.*,
(case when s.ltp_date_different_park >= date '1900-01-01' then (case when date_diff('day', s.ltp_date_different_park,s.ftp_date_key_park) <= 59 then 'NoReFTR' else 'ReFTR' end ) else 'FTR' end) as type_couriers


from Payment s

)
