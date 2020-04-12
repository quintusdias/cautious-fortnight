CREATE TABLE burst (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);

CREATE TABLE folder_lut (
    id integer NOT NULL,
    folder text UNIQUE,
    CONSTRAINT folder_lut_pkey PRIMARY KEY (id)
);

CREATE TABLE ip_address_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    CONSTRAINT ip_address_logs_id_fkey FOREIGN KEY (id) REFERENCES ip_address_lut(id) ON DELETE CASCADE
);

CREATE TABLE ip_address_lut (
    id integer NOT NULL,
    ip_address inet,
    CONSTRAINT ip_address_exists UNIQUE (ip_address),
    CONSTRAINT ip_address_lut_pkey PRIMARY KEY (id)
);

CREATE TABLE referer_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    CONSTRAINT referer_logs_id_fkey FOREIGN KEY (id) REFERENCES referer_lut(id) ON DELETE CASCADE
);

CREATE TABLE referer_lut (
    id integer NOT NULL,
    name text,
    CONSTRAINT referer_exists UNIQUE (name),
    CONSTRAINT referer_lut_pkey PRIMARY KEY (id)
);

CREATE TABLE service_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    export_mapdraws bigint,
    wms_mapdraws bigint,
    CONSTRAINT service_logs_id_fkey FOREIGN KEY (id) REFERENCES service_lut(id) ON DELETE CASCADE
);

CREATE TABLE service_lut (
    id integer NOT NULL,
    active boolean DEFAULT true,
    service text,
    folder_id integer,
    service_type_id integer,
    CONSTRAINT service_exists UNIQUE (folder_id, service, service_type_id),
    CONSTRAINT service_lut_pkey PRIMARY KEY (id),
    CONSTRAINT service_lut_folder_id_fkey FOREIGN KEY (folder_id) REFERENCES folder_lut(id) ON DELETE CASCADE
);

CREATE TABLE summary (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    mapdraws bigint
);

CREATE TABLE user_agent_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    CONSTRAINT user_agent_logs_id_date_key UNIQUE (id, date),
    CONSTRAINT user_agent_logs_id_fkey FOREIGN KEY (id) REFERENCES user_agent_lut(id) ON DELETE CASCADE
);

CREATE TABLE user_agent_lut (
    id integer NOT NULL,
    name text,
    CONSTRAINT user_agent_exists UNIQUE (name),
    CONSTRAINT user_agent_lut_pkey PRIMARY KEY (id)
);

CREATE INDEX burst_date_idx ON burst(date);
