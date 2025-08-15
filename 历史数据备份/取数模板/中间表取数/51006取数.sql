('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcsu6g4ik05bnyopi0vkfs0799s_3l2h')

--指定小时内

SELECT
hour,
count(1)
FROM webtrends.event_hi_dcslog_update_all 
WHERE dt = '2023-06-16' 
AND hour in ('09','12','21')
AND  dcsid IN ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcsu6g4ik05bnyopi0vkfs0799s_3l2h')
group by hour
order by hour
FORMAT CSVWithNames
;


SELECT
toHour(event_time) as hour,
count(1)
FROM olap.event_all
WHERE toDate(event_time) = '2023-06-16' 
AND hour in ('9','12','21')
AND  data_source_id IN ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcsu6g4ik05bnyopi0vkfs0799s_3l2h')
group by hour
order by hour
FORMAT CSVWithNames
;

--51006按照小时查

SELECT
hour,
count(1) as sum
FROM webtrends.event_hi_dcslog_update_all 
WHERE dt = '2023-06-23' 
AND  dcsid IN ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcsu6g4ik05bnyopi0vkfs0799s_3l2h')
group by hour
order by hour
FORMAT CSVWithNames
;


SELECT
toHour(event_time) as hour,
count(1) as sum
FROM olap.event_all
WHERE toDate(event_time) = '2023-06-23' 
AND  data_source_id IN ('dcscx966fo4l7j258ag0s874n_3w7x','dcs47s4etp4l7jyla4pkwq3ox_5m9b','dcsg4yobzk4bgdw3x3i02ujp5_7e9m','dcschlc2kl4bgdodrniom31n2_5i5n','dcssdx7l8il2s4xtl3gr0h6r8_2r9o','dcsijcoxb4dcw8u7yiv1jdg3fnr_4t5g','dcs8l7isqwkrn4orwipv19m518p_9o4w','dcsu6g4ik05bnyopi0vkfs0799s_3l2h')
group by hour
order by hour
FORMAT CSVWithNames
;

