
select
user AS user,
anonymous_user AS ck_id,
session  AS ss_id,
ip as ip,
splitByString('|',ip2region(case when countSubstrings(ip,'.')=3 or countSubstrings(ip,':')=7 then ip else '0|0' end)) as ip_code,
attributes['$reffer'] as referer,
decodeURLComponent(attributes['WT_event']) AS event,
domain as domain,
attributes['$path'] as path,