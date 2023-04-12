create temporary function esprocess as 'cn.com.servyou.pubdata.datark.udf.EsProcessUDF' ;

CREATE TEMPORARY VIEW ici_1 USING org.elasticsearch.spark.sql OPTIONS (
  resource 'mysearch-mirror-itcrm_chance_info_zjl-v1/_doc',
  nodes '10.199.151.14,10.199.156.10',
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


select
    ici_1.* ,cast(get_json_object(string(ici_1.`@data`), '$.stage_threshold') as string) ,
    cast(esprocess(""" if ( this['stage_threshold'] != null && this['stage_threshold']  == \"100%\" ) {
    \"998\"
} else if ( this['stage_threshold'] != null && this['stage_threshold']  == \"-\" ) {
    \"999\"
} else if ( this['stage_threshold'] != null && this['stage_threshold']  == \"-1%\" ) {
    \"999\"
} else {
    this['stage_threshold']
}
""",string(ici_1.`@data`)) as string)
from
    ici_1 ici_1
