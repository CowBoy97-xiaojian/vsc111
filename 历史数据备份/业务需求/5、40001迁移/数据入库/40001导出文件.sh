#!/bin/bash


clickhouse-client -h 10.253.248.73 -m --receive_timeout=3600 --format_csv_delimiter=$'\x01' --query="
select
    rowNumberInAllBlocks()+1,
    daytime, 
    ip, 
    channel_id, 
    page_id, 
    seller_id, 
    wtdcsdat, 
    wtchannelid, 
    wtco_f, 
    wtevent, 
    wturl, 
    wtmc_ev, 
    wtsellerid, 
    wtpageid, 
    wtlast_pageid, 
    wtlast_url, 
    wtlast_pagename, 
    wtcurrent_url, 
    wtcurrent_pagename, 
    wtmodule_no, 
    wtmodule_name, 
    wtpoint_position, 
    wtmember, 
    wtgoods_no, 
    wtlogin_status, 
    wtnext_pageid, 
    wtnext_url, 
    wtnext_pagename, 
    wtcomponent_id, 
    user_agent, 
    referer, 
    mobile, 
    session_id, 
    wtinput_sfz, 
    environment, 
    ip_prov,
    code
 from ham.ads_rpt_hachi_40001_all 
 where code = '$1' FORMAT CSV"