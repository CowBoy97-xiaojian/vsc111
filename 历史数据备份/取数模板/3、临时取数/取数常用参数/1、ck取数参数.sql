
ck
url
if(position(domain,'.')>0,if(match(domain,'^[0-9]+[.]+[0-9]+[.]+[0-9]+[.]+[0-9]+$'),concat('http://',ifNull(domain,''),ifNull(attributes['$path'],'')),concat('https://',ifNull(domain,''),ifNull(attributes['$path'],''))),domain) as url,

es
concat(url,ifNull(concat('?',attributes['$query']),'')) as es