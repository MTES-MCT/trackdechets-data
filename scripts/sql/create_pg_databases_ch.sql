-- Prod dabatabase
CREATE DATABASE pg_trackdechets_production
ENGINE = PostgreSQL('host.docker.internal:10001', 'database_name', 'postgres', 'mysecretpassword', 'data_eng',1);

CREATE DATABASE pg_trackdechets_gerico
ENGINE = PostgreSQL('host.docker.internal:10003', 'database_name', 'postgres', 'mysecretpassword', 'public',1);

CREATE DATABASE pg_dwh_raw_zone
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone',1);

CREATE DATABASE pg_dwh_raw_zone_gerep
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone_gerep',1);

CREATE DATABASE pg_dwh_raw_zone_gsheet
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone_gsheet',1);

CREATE DATABASE pg_dwh_raw_zone_icpe
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone_icpe',1);

CREATE DATABASE pg_dwh_raw_zone_rndts
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone_rndts_dlt',1);
