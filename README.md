Hello easydb

create user postgres with password '1988lm';
alter user postgres with password '1988lm';
GRANT ALL PRIVILEGES ON DATABASE test to postgres;
alter role postgres with Superuser;

# easydb
postgress wrapper to json
