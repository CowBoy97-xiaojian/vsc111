case when match(mobile, '^[0-9]{11}$') then mobile when match(mobile, '.*==$') then aes_function(mobile) end AS mobile


,case when match(mobile, '^[0-9]{11}$') then mobile when match(mobile, '.*==$') then decrypt('aes-128-ecb',base64Decode(mobile), '9e5702ead4d643fd') end AS mobile



select base64Encode(encrypt('aes-128-ecb','18951846665', '9e5702ead4d643fd')); --加密

select decrypt('aes-128-ecb',base64Decode('3WABN0KRsz2TaQKNK8gmRg=='), '9e5702ead4d643fd');--解密