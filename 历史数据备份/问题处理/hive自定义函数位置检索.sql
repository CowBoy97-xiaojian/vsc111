
SELECT jar
FROM (
    select *
    FROM  "SDS" s
    JOIN  "FUNCS" f
    ON    s.SD_ID = f.SD_ID
    WHERE f.NAME = 'decode_input'  -- 替换为你的UDF名称
) tmp
JOIN  "SD_PARAMS" sdparams
ON    tmp.SD_ID = sdparams.SD_ID
AND   sdparams.PARAM_KEY = 'jar'


SELECT *
FROM  "SDS" s
JOIN  "FUNCS" f
ON    s."SD_ID" = f."DB_ID"
WHERE f."FUNC_NAME" = 'decode_input'  -- 替换为你的UDF名称

 FUNC_ID |           CLASS_NAME            | CREATE_TIME | DB_ID |       FUNC_NAME       | FUNC_TYPE | OWNER_NAME | 
OWNER_TYPE 
---------+---------------------------------+-------------+-------+-----------------------+-----------+------------+-
-----------
       7 | com.udbac.ham.UTFThenIf         |  1585894836 |   578 | thenif                |         1 | hive       | 
USER
      26 | com.udbac.ham.GenericUTFParseIP |  1589972516 |     1 | parse_ip              |         1 |            | 
USER
      27 | com.udbac.ham.UTFDecodeUrl      |  1589972753 |     1 | decode_url            |         1 |            | 
USER
      28 | com.udbac.ham.UTFNullIf         |  1589972753 |     1 | nullif                |         1 |            | 
USER
      46 | com.udbac.ham.UDFUuid           |  1605508219 |   578 | uuid                  |         1 | udbac      | 
USER
      48 | com.udbac.ham.GenericUDFParseUA |  1605508411 |   578 | parse_ua              |         1 | udbac      | 
USER
      49 | com.udbac.ham.UDFDecodeInput    |  1605508448 |   578 | decode_input          |         1 | udbac      | 
USER
      50 | com.udbac.ham.UDFDecodeUrl      |  1605508488 |   578 | decode_url            |         1 | udbac      | 
USER
      51 | com.udbac.ham.UDFNullIf         |  1605508519 |   578 | nullif                |         1 | udbac      | 
USER
      56 | com.udbac.ham.GenericUDFParseIP |  1622448979 |   578 | parse_ip              |         1 | udbac      | 
USER
      64 | com.udf.ExtractJsonUDF          |  1688957615 |   578 | extract_xy            |         1 | udbac      | 
USER
      65 | com.udf.AesUtil                 |  1689039110 |   578 | aes_function          |         1 | udbac      | 
USER
      66 | com.udf.EncryptMobile           |  1689039120 |   578 | encrypt_function      |         1 | udbac      | 
USER
      72 | com.udf.ExtractJsonUDFYS        |  1691719070 |   578 | parsedata_xy_ys       |         1 | udbac      | 
USER
      76 | com.udf.AddXYUDF                |  1692347714 |   578 | adddataxy             |         1 | udbac      | 
USER
      81 | com.udf2.ExtractJsonUDF2        |  1697700465 |   578 | extract_xy_no_lag_lat |         1 | udbac      | 
USER
      86 | com.udf.ExtractJsonUDFYS        |  1700627662 |   578 | parsedata_xy_ys2      |         1 | udbac      | 
USER
      91 | com.udf.UserAgentParser         |  1704364758 |   578 | useragentparser_test  |         1 | udbac      | 
USER
      92 | com.udf.UserAgentParser         |  1704371670 |   578 | useragentparser_test2 |         1 | udbac      | 
USER
      93 | com.udf.UserAgentParser         |  1704422264 |   578 | useragentparser_test3 |         1 | udbac      | 
USER
      94 | com.udf.UserAgentParser         |  1704423710 |   578 | useragentparser_test4 |         1 | udbac      | 
USER
      95 | com.udf.UserAgentParser         |  1704461852 |   578 | useragentparser_test5 |         1 | udbac      | 
USER
      96 | com.udf.UserAgentParser2        |  1704872451 |   578 | useragentparser_test6 |         1 | udbac      | 
USER
(23 rows)



SELECT "INPUT_FORMAT" FROM "SDS" WHERE "SD_ID" IN (
  SELECT "SD_ID" FROM "SDS" WHERE "CD_ID" IN (
    SELECT "CD_ID" FROM "COLUMNS_V2"
  )
) AND "INPUT_FORMAT" like '%com.udbac%';