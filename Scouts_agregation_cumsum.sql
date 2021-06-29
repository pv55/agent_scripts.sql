WITH Payment_final AS (

WITH Payment AS (

WITH couriers AS (

select
(case
      when f1.fleet_gk = 200017177 then 'МСК-Шараев'
      when f1.fleet_gk = 200017820 then 'Влг-Шараев'
      when f1.fleet_gk = 200017819 then 'СРТ-Шараев'
      when f1.fleet_gk = 200017818 then 'НН-Шараев'
      when f1.fleet_gk = 200017817 then 'РНД-Шараев'
      when f1.fleet_gk = 200017816 then 'Казань-Шараев'
      when f1.fleet_gk = 200017815 then 'Спб-Шараев'
      when f1.fleet_gk = 200017430 then 'МСК-Лайттакси'
      when f1.fleet_gk = 200017524 then 'Казань-Лайттакси'
      when f1.fleet_gk = 200017523 then 'Спб-Лайттакси'
      when f1.fleet_gk = 200049934 then 'Спб-Лайттакси'
      when f1.fleet_gk = 200017517 then 'МСК-Лайттакси'
      when f1.fleet_gk = 200017548 then 'НН-Лайттакси'
      when f1.fleet_gk = 200017550 then 'РНД-Лайттакси'
      when f1.fleet_gk = 200017739 then 'Влг-Лайттакси'
      when f1.fleet_gk = 200017738 then 'СРТ-Лайттакси'
      when f1.fleet_gk = 200023811 then 'МО-Лайттакси'




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
d.ftp_date_key as ftp_date_key_all,
min(f1.order_date_key) as ftp_date_key_park,
min(f1.order_date_key) + interval '30' day FTR_plus_30days,
max(f1.order_date_key) as ltp_date_key,
cast(sum (f1.cost_exc_vat) as integer ) cost_total

from "emilia_gettdwh"."dwh_dim_drivers_v" d
left join emilia_gettdwh.dwh_fact_drivers_orders_monetization_v f1 on d.driver_gk = f1.driver_gk


where 1=1
and f1.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738, 200023811  )
and f1.country_key = 2
and f1.order_status_key = 7
and f1.cost_exc_vat >=1



Group by d.driver_gk,(case when d.courier_type is null then 'car' else d.courier_type end ),d.phone,d.driver_name,d.driver_computed_rating,f1.fleet_gk,d.driver_status,d.registration_date_key,d.ftp_date_key,(case when d.car_number = 'ЧС' then 'ЧС' end),(case when d.frozen_comment = 'Unknown' then '' else d.frozen_comment end ))

(SELECT a.*,
count(distinct(case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) then (case when f2.order_date_key between a.ftp_date_key_park and a.FTR_plus_30days then f2.order_gk end)
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) then (case when f2.order_date_key between a.ftp_date_key_park and a.FTR_plus_30days then f2.order_gk end)
    end)) as All_rides_30_days,

count(distinct(case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) then f2.order_gk
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) then f2.order_gk
    end)) as All_rides_total,

max (case when a.ftp_date_key_all <> a.ftp_date_key_park then (case when f2.order_date_key  < a.ftp_date_key_park  then f2.order_date_key end) end ) ltp_date_different_park,


count(distinct (case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.order_gk
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514)  and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.order_gk
    end)) rides_7_days,

cast(sum (case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.cost_inc_vat
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) and f2.order_date_key between a.ftp_date_key_park and a.ftp_date_key_park + interval '6' day then f2.cost_inc_vat
    end) as integer) cumsum_7_days,

count(distinct (case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.order_gk
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (498,510,512,523) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.order_gk
    end)) rides_8_to_14_days,

cast(sum (case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.cost_inc_vat
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) and f2.order_date_key between a.ftp_date_key_park + interval '7' day and a.ftp_date_key_park + interval '13' day then f2.cost_inc_vat
    end) as integer) cumsum_8_to_14_days,

count(distinct (case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.order_gk
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.order_gk
    end)) rides_15_to_21_days,

cast(sum (case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.cost_inc_vat
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) and f2.order_date_key between a.ftp_date_key_park + interval '14' day and a.ftp_date_key_park + interval '20' day then f2.cost_inc_vat
    end) as integer) cumsum_15_to_21_days,

count(distinct (case
    when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.order_gk
    when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.order_gk
    end)) rides_16_to_30_days,

    cast(sum (case
        when f2.fleet_gk in (200017177, 200017820, 200017819, 200017818, 200017817, 200017816, 200017815, 200017430, 200017524, 200017523, 200049934, 200017517, 200017548, 200017550, 200017739, 200017738) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.cost_inc_vat
        when f2.fleet_gk = 200023811 and f2.origin_osm_location_key in (505, 523, 512, 510, 498, 526, 492, 500, 560, 506, 514) and f2.order_date_key between a.ftp_date_key_park + interval '21' day and a.ftp_date_key_park + interval '29' day then f2.cost_inc_vat
        end) as integer) cumsum_16_to_30_days,
(case
    when a.city in('МСК-Шараев','МСК-Лайттакси') and a.ftp_date_key_park  >= date '2021-06-21'  then 4
    when a.city in('МО-Лайттакси','Спб-Лайттакси','НН-Лайттакси','РНД-Лайттакси','Влг-Лайттакси','СРТ-Лайттакси','Влг-Шараев','СРТ-Шараев','НН-Шараев','РНД-Шараев','Казань-Шараев','Казань-Лайттакси','Спб-Шараев' ) and a.ftp_date_key_park  >= date '2021-03-29' then 5
    else 0
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
(case when s.city in ('МСК-Шараев','Влг-Шараев','СРТ-Шараев','НН-Шараев','РНД-Шараев','Казань-Шараев','Спб-Шараев') then 'Сергей Шараев' else f5.name end) name_agent,
(case when s.city in ('МСК-Шараев','Влг-Шараев','СРТ-Шараев','НН-Шараев','РНД-Шараев','Казань-Шараев','Спб-Шараев') then '900' else f5.id_agent end) id_agent,
(case when s.city in ('МСК-Шараев','Влг-Шараев','СРТ-Шараев','НН-Шараев','РНД-Шараев','Казань-Шараев','Спб-Шараев') then 'Сергей Шараев' else f5.Name_team end) Name_team,
(case when s.city in ('МСК-Шараев','Влг-Шараев','СРТ-Шараев','НН-Шараев','РНД-Шараев','Казань-Шараев','Спб-Шараев') then '900' else f5.id_team end) id_team,
s.city city,
s.driver_gk driver_gk,
s.phone phone,
s.driver_name,
s.courier_type courier_type,
s.driver_computed_rating rating,
s.fleet_gk fleet_gk,
s.driver_status driver_status,
s.status status,
f6.date_leads date_leads,
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
(case when s.ltp_date_different_park >= date '1900-01-01' then (case when date_diff('day', s.ltp_date_different_park,s.ftp_date_key_park) <= 59 then 'NoReFTR' else 'ReFTR' end ) else 'FTR' end) as type_couriers,
(case when f3.date_pay <> '' then date(f3.date_pay) end) as last_pay,
(case when f3.date_pay <> '' then cast (f3.cumsum as integer)  else 0 end) as last_CumSum,
(case when  s.ltp_date_different_park >= date '1900-01-01'and date_diff('day', s.ltp_date_different_park,s.ftp_date_key_park) <= 59 and f3.date_pay is null  then 0

    else (case
    when s.driver_gk in (2000634164,2000936189,2000923715) then 0
    when s.All_rides_30_days <= 4 then 0
    when cast (f3.cumsum as integer) = 4000 or (cast (f3.cumsum as integer) = 3000 and s.type_bonus = 4) then 0


    when s.type_bonus = 4 and s.All_rides_30_days between 5 and 19 then (case when cast (f3.cumsum as integer) >= 0 then 500 - cast (f3.cumsum as integer) else 500 end)
    when s.type_bonus = 4 and s.All_rides_30_days between 20 and 39 then (case when cast (f3.cumsum as integer) >= 0 then 1500 - cast (f3.cumsum as integer) else 1500 end)
    when s.type_bonus = 4 and s.All_rides_30_days >= 40 then (case when cast (f3.cumsum as integer) >= 0 then 3000 - cast (f3.cumsum as integer) else 3000 end)

    when s.type_bonus = 5 and s.All_rides_30_days between 5 and 9 then (case when cast (f3.cumsum as integer) >= 0 then 500 - cast (f3.cumsum as integer) else 500 end)
    when s.type_bonus = 5 and s.All_rides_30_days between 10 and 19 then (case when cast (f3.cumsum as integer) >= 0 then 2000 - cast (f3.cumsum as integer) else 2000 end)
    when s.type_bonus = 5 and s.All_rides_30_days between 20 and 29 then (case when cast (f3.cumsum as integer) >= 0 then 3000 - cast (f3.cumsum as integer) else 3000 end)
    when s.type_bonus = 5 and s.All_rides_30_days >= 30 then (case when cast (f3.cumsum as integer) >= 0 then 4000 - cast (f3.cumsum as integer) else 4000 end)

    else 0 end ) end) Cumsum,
s.type_bonus


from Payment s
left join sheets.default.Payments_Scouts f3  on f3.id = cast(s.driver_gk as varchar )
left join sheets.default.leads_Scouts2 f6  on f6.phone = s.phone
left join sheets.default.agent_Scouts f5  on f6.id_agent = f5.id_agent

where 1=1


))

(SELECT w.*,
(case
    when w.last_CumSum >= 500 or w.type_couriers = 'NoReFTR' or w.Cumsum = 0  then 0
    when w.type_bonus = 4 and w.All_rides_30_days >=5 and w.last_CumSum = 0 and w.Cumsum >0  then 300
    when w.type_bonus = 5 and w.All_rides_30_days >=5 and w.last_CumSum = 0 and w.Cumsum >0  then 1000

    else 0 end) a1,

(case

    when w.type_bonus = 4 and w.All_rides_30_days >=20 and w.last_CumSum < 1500 and w.Cumsum >0    then 700
    when w.type_bonus = 5 and w.All_rides_30_days >=10 and w.last_CumSum < 2000 and w.Cumsum >0    then 700

  else 0 end) a2,

(case

    when w.type_bonus = 4 and w.All_rides_30_days >=40 and w.last_CumSum < 3000 and w.Cumsum >0   then 1500
    when w.type_bonus = 5 and w.All_rides_30_days >=20 and w.last_CumSum < 3000 and w.Cumsum >0   then 800

  else 0 end) a3,

(case

    when w.type_bonus = 5 and w.All_rides_30_days >=30 and w.last_CumSum < 4000 and w.Cumsum >0    then 1000

  else 0 end) a4,

(case
    when w.last_CumSum >= 500 or w.type_couriers = 'NoReFTR' or w.Cumsum = 0  then 0
    when w.type_bonus in (4,5) and w.All_rides_30_days >=5 and w.last_CumSum = 0 and w.Cumsum >0  then 200

    else 0 end) tl,

(case

    when w.type_bonus = 4 and w.All_rides_30_days >=20 and w.last_CumSum < 1500 and w.Cumsum >0    then 300
    when w.type_bonus = 5 and w.All_rides_30_days >=10 and w.last_CumSum < 2000 and w.Cumsum >0    then 300

  else 0 end) stl,

(case when w.last_CumSum >= 500 or w.type_couriers = 'NoReFTR' or w.Cumsum = 0  then 0 else 500 end) GETT1,

(case

  when w.type_bonus = 4 and w.All_rides_30_days >=20 and w.last_CumSum < 2000 and w.Cumsum >0    then 1000
  when w.type_bonus = 5 and w.All_rides_30_days >=10 and w.last_CumSum < 2000 and w.Cumsum >0    then 1500

  else 0 end) GETT2,

(case

    when w.type_bonus = 4 and w.All_rides_30_days >=40 and w.last_CumSum < 3000 and w.Cumsum >0   then 1500
    when w.type_bonus = 5 and w.All_rides_30_days >=20 and w.last_CumSum < 3000 and w.Cumsum >0   then 1000

  else 0 end) GETT3,

(case

    when w.type_bonus = 5 and w.All_rides_30_days >=30 and w.last_CumSum < 4000 and w.Cumsum >0    then 1000

  else 0 end) GETT4,

(case

    when w.type_bonus = 4  and w.last_CumSum = 500  then 0
    when w.type_bonus = 4  and w.last_CumSum = 1500  then 0
    when w.type_bonus = 5  and w.last_CumSum = 3000  then 0


    when w.type_bonus = 5  and w.last_CumSum = 500  then 700
    when w.type_bonus = 5  and w.last_CumSum = 2000  then 200
    when w.type_bonus = 5  and w.last_CumSum = 3000  then 0
    when w.type_bonus = 5  and w.last_CumSum = 4000  then 0


  else 0 end) Overpayment

from Payment_final w)

order by ftp_date_key_park desc
