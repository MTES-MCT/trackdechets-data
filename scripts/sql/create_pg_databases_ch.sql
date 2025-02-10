-- Prod dabatabase

CREATE DATABASE pg_trackdechets_production
ENGINE = PostgreSQL('host.docker.internal:10001', 'database_name', 'postgres', 'mysecretpassword', 'default$default',1);

CREATE DATABASE pg_dwh_raw_zone
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone',1);

CREATE DATABASE pg_dwh_raw_zone_gerep
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone_gerep',1);

CREATE DATABASE pg_dwh_raw_zone_gsheet
ENGINE = PostgreSQL('host.docker.internal:5432', 'trackdechets-datawarehouse', 'postgres', 'mysecretpassword', 'raw_zone_gsheet',1);

d