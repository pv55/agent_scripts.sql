WITH Payment AS (

WITH couriers AS (

select
(case when f1.fleet_gk = 200017083 then 'МСК-Дептакси'
      when f1.fleet_gk = 200017177 then 'МСК-Шараев'
      when f1.fleet_gk = 200017412 then 'Казань-Дептакси'
      when f1.fleet_gk = 200017342 then 'Спб-Дептакси'
      when f1.fleet_gk = 200017205 then 'Спб-Дептакси'
      when f1.fleet_gk = 200017203 then 'МСК-Дептакси'
      when f1.fleet_gk = 200017203 then 'МСК-Дептакси'
      when f1.fleet_gk = 200017430 then 'МСК-Лайттакси'
      when f1.fleet_gk = 200017524 then 'Казань-Лайттакси'
      when f1.fleet_gk = 200017523 then 'Спб-Лайттакси'
      when f1.fleet_gk = 200017517 then 'МСК-Лайттакси'
      when f1.fleet_gk = 200017548 then 'НН-Лайттакси'
      when f1.fleet_gk = 200017550 then 'РНД-Лайттакси'
      when f1.fleet_gk = 200014202 then 'МСК-Горизонт'
      when f1.fleet_gk = 200014203 then 'МСК-Горизонт'
      when f1.fleet_gk = 200016266 then 'Казань-Горизонт'
      when f1.fleet_gk = 200016359 then 'НН-Горизонт'
      when f1.fleet_gk = 200016267 then 'РНД-Горизонт'
      when f1.fleet_gk = 200016265 then 'СПБ-Горизонт'
      when f1.fleet_gk = 200017204 then 'СПБ-Горизонт'
      end ) city,
d.driver_gk as driver_gk,
d.phone as phone,
(case when d.courier_type is null then 'car' else d.courier_type end ) courier_type,
d.driver_name as driver_name,
cast(d.driver_computed_rating as integer ) as driver_computed_rating,
f1.fleet_gk as fleet_gk,
d.driver_status as driver_status,
(case when d.frozen_comment = 'Unknown' then '' else d.frozen_comment end ) status,


d.registration_date_key as registration_date_key,
      
-- В "emilia_gettdwh"."dwh_dim_drivers_v" d хранится первая поездка в GD.
d.ftp_date_key as ftp_date_key_all,
      
-- В emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f1 берем первую поездку в этом флите.
min(f1.order_date_key) as ftp_date_key_park,
      
      
min(f1.order_date_key) + interval '30' day FTR_plus_30days,
max(f1.order_date_key) as ltp_date_key,
cast(sum (f1.cost_exc_vat) as integer ) cost_total

from "emilia_gettdwh"."dwh_dim_drivers_v" d
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f1 on d.driver_gk = f1.driver_gk


where 1=1
and f1.fleet_gk in ( 200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204 )
and f1.country_key = 2
and f1.order_status_key = 7
and f1.cost_exc_vat >=1



Group by d.driver_gk,(case when d.courier_type is null then 'car' else d.courier_type end ),d.phone,d.driver_name,d.driver_computed_rating,f1.fleet_gk,d.driver_status,d.registration_date_key,d.ftp_date_key,(case when d.car_number = 'ЧС' then 'ЧС' end),(case when d.frozen_comment = 'Unknown' then '' else d.frozen_comment end ))

(SELECT a.*,
count(distinct(case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) then (case when f2.order_date_key between a.ftp_date_key_park and a.FTR_plus_30days then f2.order_gk end)  end)) as All_rides_30_days,

count(distinct(case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) then f2.order_gk  end)) as All_rides_total,
               
-- Если первая поездка в GD не равна первой поездке в парке, то это ReFTR. Вводим параметр = дата в другом парке
-- Берем максимальную дату, т.е. последнюю дату поездки, т.е. последнюю дату, когда курьер катал в другом парке
max (case when a.ftp_date_key_all <> a.ftp_date_key_park then (case when f2.order_date_key  < a.ftp_date_key_park  then f2.order_date_key end) end ) ltp_date_different_park,


count(distinct (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.order_gk end)) rides_7_days,
cast(sum (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.cost_inc_vat end) as integer) cumsum_7_days,
count(distinct (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.order_gk end)) rides_8_to_14_days,
cast(sum (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.cost_inc_vat end) as integer) cumsum_8_to_14_days,
count(distinct (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.order_gk end)) rides_15_to_21_days,
cast(sum (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.cost_inc_vat end) as integer) cumsum_15_to_21_days,
count(distinct (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.order_gk end)) rides_16_to_30_days,
cast(sum (case when f2.fleet_gk in (200017083, 200017177,200017412,200017342,200017205,200017203, 200017524,200017523,200017517,200017430,200017548,200017550,200014202,200014203,200016266,200016359,200016267,200016265,200017204) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.cost_inc_vat end) as integer) cumsum_16_to_30_days,
(case
    when a.ftp_date_key_park  between date '2020-12-01' and date '2020-12-19' then 1
    when a.ftp_date_key_park  between date '2020-12-20' and date '2021-02-14' then 2
    when a.ftp_date_key_park  >= date '2021-02-15' then 3
    end) as type_bonus


from couriers a
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f2 on a.driver_gk = f2.driver_gk

where 1=1

and f2.country_key = 2
and f2.order_status_key = 7
and f2.cost_exc_vat >=1
and a.FTR_plus_30days >= (now() - interval '30' day)

GROUP by a.city,a.driver_gk,a.cost_total,a.courier_type,a.phone,a.driver_name,a.driver_computed_rating,a.fleet_gk,a.driver_status,a.status,a.registration_date_key,a.ftp_date_key_all,a.ftp_date_key_park,a.FTR_plus_30days,a.ltp_date_key))

(SELECT

s.city city,
s.driver_gk driver_gk,
s.phone phone,
s.driver_name,
s.courier_type courier_type,
s.driver_computed_rating rating,
s.fleet_gk fleet_gk,
s.driver_status driver_status,
s.status status,

s.registration_date_key,
s.ftp_date_key_all ftp_date_key_all,
s.ftp_date_key_park ftp_date_key_park,
s.FTR_plus_30days FTR_plus_30days,
s.ltp_date_key ltp_date_key,
s.cost_total cost_total,
s.All_rides_30_days All_rides_30_days,
s.All_rides_total All_rides_total,
s.ltp_date_different_park ltp_date_different_park,
s.rides_7_days rides_7_days,
s.cumsum_7_days cumsum_7_days,
s.rides_8_to_14_days rides_8_to_14_days,
s.cumsum_8_to_14_days cumsum_8_to_14_days,
s.rides_15_to_21_days rides_15_to_21_days,
s.cumsum_15_to_21_days cumsum_15_to_21_days,
s.rides_16_to_30_days rides_16_to_30_days,
s.cumsum_16_to_30_days cumsum_16_to_30_days,

 -- если у курьера, есть дата последней поездки в другом парке date_diff между последней поездкой в другом парке и первой поездкой в парке <= 59, то это NoReFTR
 
(case when s.ltp_date_different_park >= date '1900-01-01' then (case when date_diff('day', s.ltp_date_different_park,s.ftp_date_key_park) <= 59 then 'NoReFTR' else 'ReFTR' end ) else 'FTR' end) as type_couriers,
s.type_bonus

from Payment s)
