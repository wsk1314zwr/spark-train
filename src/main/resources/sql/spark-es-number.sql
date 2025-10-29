CREATE TEMPORARY VIEW v1 USING org.elasticsearch.spark.sql OPTIONS (
  resource 'taxcore-warden-index-20251029/logs',
  nodes '10.199.151.10:9200',
  port '9200',
  net.http.auth.user 'warden',
  net.http.auth.pass 'ac8f14c5eaef4780bb44b3f1eefe6f9c',
  net.http.auth.pass.encrypted 'true',
  security.vault.appcode 'datark',
  security.vault.gateway.url 'http://jupiter-gateway.servyou-stable.sit.91lyd.com',
  security.vault.appkey 'NTBENDA1ODc4MTNFNDlFRkE1QUJEMTgyNjlFOTM5Rjc=',
  security.vault.appsecret 'yihh+ahidJSH4gT0mUMpZw==',
  es.read.metadata      'true',          -- 关键：开启元数据读取
  es.read.metadata.field '_metadata'     -- 可选：把所有元字段装进一个
);

SELECT * from v1;