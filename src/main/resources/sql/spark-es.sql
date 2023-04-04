create temporary function esprocess as 'cn.com.servyou.pubdata.datark.udf.EsProcessUDF' ;

CREATE TEMPORARY VIEW aiii_4 USING org.elasticsearch.spark.sql OPTIONS (
  resource 'mysearch-mirror-ads_iii_dev-v3/_doc',
  nodes '10.199.151.14',
  port '9200',
  net.http.auth.user 'mysearch',
  net.http.auth.pass 'mysearch_es_pass',
  net.http.auth.pass.encrypted 'true',
  field.read.empty.as.null 'no',
  security.vault.appcode 'datark',
  security.vault.gateway.url 'http://jupiter-gateway.servyou-stable.sit.91lyd.com',
  security.vault.appkey 'NTBENDA1ODc4MTNFNDlFRkE1QUJEMTgyNjlFOTM5Rjc=',
  security.vault.appsecret 'yihh+ahidJSH4gT0mUMpZw=='
);

CREATE TEMPORARY VIEW aiii_4_view  as
select * from
    (select * ,row_number() over( partition by `institution_id`,`institution_type` order by `@mt` desc) as rowNumber  from aiii_4 where `@del` = false) z where z.rowNumber <= 1;

--创建连接外部es表的临时视图
CREATE TEMPORARY VIEW mcbbi_3 USING org.elasticsearch.spark.sql OPTIONS (
  resource 'mysearch-mirror-md_cbb_info_dev-v6/_doc',
  nodes '10.199.151.14',
  port '9200',
  net.http.auth.user 'mysearch',
  net.http.auth.pass 'mysearch_es_pass',
  net.http.auth.pass.encrypted 'true',
  field.read.empty.as.null 'no',
  security.vault.appcode 'datark',
  security.vault.gateway.url 'http://jupiter-gateway.servyou-stable.sit.91lyd.com',
  security.vault.appkey 'NTBENDA1ODc4MTNFNDlFRkE1QUJEMTgyNjlFOTM5Rjc=',
  security.vault.appsecret 'yihh+ahidJSH4gT0mUMpZw=='
);

--根节点的node只需要过滤@del不为false的数据即可
CREATE TEMPORARY VIEW mcbbi_3_view as
select *  from mcbbi_3 where `@del` = false;

--创建spark es scene 临时视图表
CREATE TEMPORARY VIEW scene(
  `mcbbi_3@@id` string,
  `mcbbi_3@@del` string,
  `mcbbi_3@@mt` timestamp,
  `mcbbi_3@id` long,
  `mcbbi_3@customer_id` string,
  `mcbbi_3@customer_type` string,
  `mcbbi_3@create_time` timestamp,
  `mcbbi_3@operator` string,
  `mcbbi_3@business_belong` string,
  `mcbbi_3@task_json` string,
  `mcbbi_3@operator_de` string,
  `mcbbi_3@operator_low_case` string,
  `mcbbi_3@task_json_alias` string,
  `@st@mcbbi_3` string,
  `aiii_4@@id` string,
  `aiii_4@@del` string,
  `aiii_4@@mt` timestamp,
  `aiii_4@id` long,
  `aiii_4@institution_id` string,
  `aiii_4@institution_type` string,
  `aiii_4@mobile` string,
  `aiii_4@create_time` timestamp,
  `aiii_4@renewal_rate` double,
  `aiii_4@mobile_en` string,
  `aiii_4@institution_flag` string,
  `@st@aiii_4` string,
  `@timestamp` timestamp
) USING org.elasticsearch.spark.sql OPTIONS (
  resource 'mysearch-scene-multi_join_wsk_test_0407001-v18/_doc',
  es.mapping.id 'mcbbi_3@@id',
  nodes '10.199.151.14',
  port '9200',
  net.http.auth.user 'mysearch',
  net.http.auth.pass 'mysearch_es_pass',
  net.http.auth.pass.encrypted 'true',
  field.read.empty.as.null 'no',
  security.vault.appcode 'datark',
  security.vault.gateway.url 'http://jupiter-gateway.servyou-stable.sit.91lyd.com',
  security.vault.appkey 'NTBENDA1ODc4MTNFNDlFRkE1QUJEMTgyNjlFOTM5Rjc=',
  security.vault.appsecret 'yihh+ahidJSH4gT0mUMpZw=='
);

insert into
    scene
select
    /*+ REPARTITION(5, d_id) */
    cast(mcbbi_3.`@id`  as string)  as d_id,
    cast(mcbbi_3.`@del`  as string),
    cast(mcbbi_3.`@mt`  as timestamp),
    cast(get_json_object(string(mcbbi_3.`@data`), '$.id') as long) ,
    cast(get_json_object(string(mcbbi_3.`@data`), '$.customer_id') as string) ,
    cast(get_json_object(string(mcbbi_3.`@data`), '$.customer_type') as string) ,
    timestamp_millis(cast(get_json_object(string(mcbbi_3.`@data`), '$.create_time') as long)) ,
    cast(get_json_object(string(mcbbi_3.`@data`), '$.operator') as string) ,
    cast(get_json_object(string(mcbbi_3.`@data`), '$.business_belong') as string) ,
    cast(get_json_object(string(mcbbi_3.`@data`), '$.task_json') as string) ,
    cast(esprocess('cn.com.servyou.mysearch.core.process.processor.MVELFunctionUtils.vaultDecrypt("c20e1f58-5711-4900-882f-b154ad481d44","xqy-nt-tax","XQeUQJzI/HBIV4/rK5J/bXmXbH05aKRWuZjgGV3bGSA=",?this[\'operator\'])',string(mcbbi_3.`@data`)) as string) ,
    cast(esprocess('?this[\'operator\'].toLowerCase()',string(mcbbi_3.`@data`)) as string) ,
    cast(esprocess('com.alibaba.fastjson.JSON.parseObject(?this[\'task_json\']).getString("alias")',string(mcbbi_3.`@data`)) as string) ,
    if(mcbbi_3.`@id` IS NULL, 'NULL', 'LATEST'),
    cast(aiii_4.`@id`  as string),
    cast(aiii_4.`@del`  as string),
    cast(aiii_4.`@mt`  as timestamp),
    cast(get_json_object(string(aiii_4.`@data`), '$.id') as long) ,
    cast(get_json_object(string(aiii_4.`@data`), '$.institution_id') as string) ,
    cast(get_json_object(string(aiii_4.`@data`), '$.institution_type') as string) ,
    cast(get_json_object(string(aiii_4.`@data`), '$.mobile') as string) ,
    timestamp_millis(cast(get_json_object(string(aiii_4.`@data`), '$.create_time') as long)) ,
    cast(get_json_object(string(aiii_4.`@data`), '$.renewal_rate') as double) ,
    cast(esprocess("""cn.com.servyou.mysearch.core.process.processor.MVELFunctionUtils.vaultEncrypt(\"c20e1f58-5711-4900-882f-b154ad481d44\",\"xqy-nt-tax\",\"XQeUQJzI/HBIV4/rK5J/bXmXbH05aKRWuZjgGV3bGSA=\",this['mobile'])""",string(aiii_4.`@data`)) as string) ,
    cast(esprocess(""" this['mobile'] == null ? '0':'1' """,string(aiii_4.`@data`)) as string) ,
    if(aiii_4.`@id` IS NULL, 'NULL', 'LATEST'),
    now()
from
    mcbbi_3_view mcbbi_3
        left join aiii_4_view aiii_4 on mcbbi_3.`customer_id` = aiii_4.`institution_id` and mcbbi_3.`customer_type` = aiii_4.`institution_type`
