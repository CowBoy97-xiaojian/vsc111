
离线逻辑
1、安卓、ios全部下发

2、其他渠道
attributes['area_id'] is null and 
attributes['type'] is null 

新增
or
et != imp


实时逻辑
if(attributesJson.containsKey("area_id") && attributesJson.containsKey("type")){
    if("CUSTOM_EVENT".equals(sourceJson.getString("eventType"))){
        return true;
    }
}


10
1