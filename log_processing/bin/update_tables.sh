#!/usr/bin/env bash

# Update the tables to support cascading delete.

set -x
set -e

function update_tables()
{
    local project=${1}

#------------------------------------------------------------------------------
# ip_address
sqlite3 $HOME/Documents/arcgis_apache_logs/arcgis_apache_"$project".db << END_SQL

PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

ALTER TABLE ip_address_logs RENAME TO _ip_address_logs_old;

CREATE TABLE ip_address_logs (
    date integer,
    id integer,
    hits integer,
    errors integer,
    nbytes integer,
    CONSTRAINT fk_known_ip_address_id
        FOREIGN KEY (id)
        REFERENCES known_ip_addresses(id)
	ON DELETE CASCADE
);

INSERT INTO ip_address_logs SELECT * FROM _ip_address_logs_old;

DROP TABLE _ip_address_logs_old;

COMMIT;

PRAGMA foreign_keys=on;

SELECT name from sqlite_master where type='table' and name not like 'sqlite_%';

END_SQL

#------------------------------------------------------------------------------
# referer
sqlite3 $HOME/Documents/arcgis_apache_logs/arcgis_apache_"$project".db << END_SQL

PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

ALTER TABLE referer_logs RENAME TO _referer_logs_old;

CREATE TABLE referer_logs (
    date integer,
    id integer,
    hits integer,
    errors integer,
    nbytes integer,
    CONSTRAINT fk_known_referers_id
        FOREIGN KEY (id)
	REFERENCES known_referers(id)
	ON DELETE CASCADE
);

INSERT INTO referer_logs SELECT * FROM _referer_logs_old;

DROP TABLE _referer_logs_old;

COMMIT;

PRAGMA foreign_keys=on;

SELECT name from sqlite_master where type='table' and name not like 'sqlite_%';

END_SQL

#------------------------------------------------------------------------------
# services
sqlite3 $HOME/Documents/arcgis_apache_logs/arcgis_apache_"$project".db << END_SQL

PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

ALTER TABLE service_logs RENAME TO _service_logs_old;

CREATE TABLE service_logs (                                          
    date integer,                                                    
    id integer,                                                      
    hits integer,                                                    
    errors integer,                                                  
    nbytes integer,                                                  
    CONSTRAINT fk_known_services_id
        FOREIGN KEY (id)
        REFERENCES known_services(id)                   
	ON DELETE CASCADE
);

INSERT INTO service_logs SELECT * FROM _service_logs_old;

DROP TABLE _service_logs_old;

SELECT name from sqlite_master where type='table' and name not like 'sqlite_%';

COMMIT;

PRAGMA foreign_keys=on;

END_SQL

#------------------------------------------------------------------------------
# user agent
sqlite3 $HOME/Documents/arcgis_apache_logs/arcgis_apache_"$project".db << END_SQL

PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

ALTER TABLE user_agent_logs RENAME TO _user_agent_logs_old;

CREATE TABLE user_agent_logs (
    date integer,
    id integer,
    hits integer,
    errors integer,
    nbytes integer,
    CONSTRAINT fk_known_user_agents_id
        FOREIGN KEY (id)
        REFERENCES known_user_agents(id)
	ON DELETE CASCADE
);

INSERT INTO user_agent_logs SELECT * FROM _user_agent_logs_old;

DROP TABLE _user_agent_logs_old;

SELECT name from sqlite_master where type='table' and name not like 'sqlite_%';

COMMIT;

PRAGMA foreign_keys=on;

END_SQL
}

for project in nowcoast idpgis
do
    update_tables $project
done
