#!/bin/bash

set -e
set -x

schema=${1}
dbname=${2}

PSQLCMD="psql -X -U jevans --set ON_ERROR_STOP=on --single-transaction -d $dbname"

$PSQLCMD <<SQL

set search_path to '$schema';

-- Migrate the service_type_id column in the service log table to an
-- enum type.
create type svc_type_enum as ENUM('MapServer', 'ImageServer', 'FeatureServer');

alter table service_lut add service_type svc_type_enum;

update service_lut s
set service_type = (
    select name::svc_type_enum
    from service_type_lut
    where id = s.service_type_id
);

alter table service_lut drop service_type_id;
drop table service_type_lut;
SQL
