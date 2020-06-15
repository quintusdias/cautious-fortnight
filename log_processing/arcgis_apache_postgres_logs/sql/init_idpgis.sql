--
-- PostgreSQL database dump
--

-- Dumped from database version 11.7
-- Dumped by pg_dump version 11.7

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: idpgis; Type: SCHEMA; Schema: -; Owner: jevans
--

CREATE SCHEMA idpgis;


ALTER SCHEMA idpgis OWNER TO jevans;

--
-- Name: svc_type_enum; Type: TYPE; Schema: idpgis; Owner: jevans
--

CREATE TYPE idpgis.svc_type_enum AS ENUM (
    'MapServer',
    'ImageServer',
    'FeatureServer'
);


ALTER TYPE idpgis.svc_type_enum OWNER TO jevans;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: burst; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.burst (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE idpgis.burst OWNER TO jevans;

--
-- Name: folder_lut; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.folder_lut (
    id integer NOT NULL,
    folder text
);


ALTER TABLE idpgis.folder_lut OWNER TO jevans;

--
-- Name: folder_lut_id_seq; Type: SEQUENCE; Schema: idpgis; Owner: jevans
--

CREATE SEQUENCE idpgis.folder_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE idpgis.folder_lut_id_seq OWNER TO jevans;

--
-- Name: folder_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: idpgis; Owner: jevans
--

ALTER SEQUENCE idpgis.folder_lut_id_seq OWNED BY idpgis.folder_lut.id;


--
-- Name: ip_address_logs; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.ip_address_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE idpgis.ip_address_logs OWNER TO jevans;

--
-- Name: TABLE ip_address_logs; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON TABLE idpgis.ip_address_logs IS 'An IP address cannot have a summarizing set of statistics at the same time.';


--
-- Name: COLUMN ip_address_logs.id; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON COLUMN idpgis.ip_address_logs.id IS 'identifies IP address in lookup table';


--
-- Name: ip_address_lut; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.ip_address_lut (
    id integer NOT NULL,
    ip_address inet
);


ALTER TABLE idpgis.ip_address_lut OWNER TO jevans;

--
-- Name: ip_address_lut_id_seq; Type: SEQUENCE; Schema: idpgis; Owner: jevans
--

CREATE SEQUENCE idpgis.ip_address_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE idpgis.ip_address_lut_id_seq OWNER TO jevans;

--
-- Name: ip_address_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: idpgis; Owner: jevans
--

ALTER SEQUENCE idpgis.ip_address_lut_id_seq OWNED BY idpgis.ip_address_lut.id;


--
-- Name: referer_logs; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.referer_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE idpgis.referer_logs OWNER TO jevans;

--
-- Name: TABLE referer_logs; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON TABLE idpgis.referer_logs IS 'A referer cannot have a summarizing set of statistics at the same time.';


--
-- Name: COLUMN referer_logs.id; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON COLUMN idpgis.referer_logs.id IS 'identifies referer in lookup table';


--
-- Name: referer_lut; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.referer_lut (
    id integer NOT NULL,
    name text
);


ALTER TABLE idpgis.referer_lut OWNER TO jevans;

--
-- Name: referer_lut_id_seq; Type: SEQUENCE; Schema: idpgis; Owner: jevans
--

CREATE SEQUENCE idpgis.referer_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE idpgis.referer_lut_id_seq OWNER TO jevans;

--
-- Name: referer_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: idpgis; Owner: jevans
--

ALTER SEQUENCE idpgis.referer_lut_id_seq OWNED BY idpgis.referer_lut.id;


--
-- Name: service_logs; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.service_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    export_mapdraws bigint,
    wms_mapdraws bigint
);


ALTER TABLE idpgis.service_logs OWNER TO jevans;

--
-- Name: TABLE service_logs; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON TABLE idpgis.service_logs IS 'Aggregated summary statistics';

CREATE INDEX service_logs_date_idx ON idpgis.service_logs USING btree (date);

--
-- Name: COLUMN service_logs.hits; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON COLUMN idpgis.service_logs.hits IS 'Number of hits aggregated over a set time period (one hour?)';


--
-- Name: service_lut; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.service_lut (
    id integer NOT NULL,
    service text,
    folder_id integer,
    active boolean DEFAULT true,
    service_type idpgis.svc_type_enum
);


ALTER TABLE idpgis.service_lut OWNER TO jevans;

--
-- Name: TABLE service_lut; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON TABLE idpgis.service_lut IS 'This table should not vary unless there is a new release at NCEP';


--
-- Name: COLUMN service_lut.active; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON COLUMN idpgis.service_lut.active IS 'False if a service has been retired.';


--
-- Name: service_lut_id_seq; Type: SEQUENCE; Schema: idpgis; Owner: jevans
--

CREATE SEQUENCE idpgis.service_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE idpgis.service_lut_id_seq OWNER TO jevans;

--
-- Name: service_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: idpgis; Owner: jevans
--

ALTER SEQUENCE idpgis.service_lut_id_seq OWNED BY idpgis.service_lut.id;


--
-- Name: summary; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.summary (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    mapdraws bigint
);


ALTER TABLE idpgis.summary OWNER TO jevans;

--
-- Name: user_agent_logs; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.user_agent_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE idpgis.user_agent_logs OWNER TO jevans;

--
-- Name: COLUMN user_agent_logs.id; Type: COMMENT; Schema: idpgis; Owner: jevans
--

COMMENT ON COLUMN idpgis.user_agent_logs.id IS 'identifies user agent in lookup table';


--
-- Name: user_agent_lut; Type: TABLE; Schema: idpgis; Owner: jevans
--

CREATE TABLE idpgis.user_agent_lut (
    id integer NOT NULL,
    name text
);


ALTER TABLE idpgis.user_agent_lut OWNER TO jevans;

--
-- Name: user_agent_lut_id_seq; Type: SEQUENCE; Schema: idpgis; Owner: jevans
--

CREATE SEQUENCE idpgis.user_agent_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE idpgis.user_agent_lut_id_seq OWNER TO jevans;

--
-- Name: user_agent_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: idpgis; Owner: jevans
--

ALTER SEQUENCE idpgis.user_agent_lut_id_seq OWNED BY idpgis.user_agent_lut.id;


--
-- Name: folder_lut id; Type: DEFAULT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.folder_lut ALTER COLUMN id SET DEFAULT nextval('idpgis.folder_lut_id_seq'::regclass);


--
-- Name: ip_address_lut id; Type: DEFAULT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.ip_address_lut ALTER COLUMN id SET DEFAULT nextval('idpgis.ip_address_lut_id_seq'::regclass);


--
-- Name: referer_lut id; Type: DEFAULT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.referer_lut ALTER COLUMN id SET DEFAULT nextval('idpgis.referer_lut_id_seq'::regclass);


--
-- Name: service_lut id; Type: DEFAULT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.service_lut ALTER COLUMN id SET DEFAULT nextval('idpgis.service_lut_id_seq'::regclass);


--
-- Name: user_agent_lut id; Type: DEFAULT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.user_agent_lut ALTER COLUMN id SET DEFAULT nextval('idpgis.user_agent_lut_id_seq'::regclass);


--
-- Name: folder_lut folder_exists; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.folder_lut
    ADD CONSTRAINT folder_exists UNIQUE (folder);


--
-- Name: folder_lut folder_lut_pkey; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.folder_lut
    ADD CONSTRAINT folder_lut_pkey PRIMARY KEY (id);


--
-- Name: ip_address_lut ip_address_exists; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.ip_address_lut
    ADD CONSTRAINT ip_address_exists UNIQUE (ip_address);


--
-- Name: ip_address_lut ip_address_lut_pkey; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.ip_address_lut
    ADD CONSTRAINT ip_address_lut_pkey PRIMARY KEY (id);


--
-- Name: referer_lut referer_exists; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.referer_lut
    ADD CONSTRAINT referer_exists UNIQUE (name);


--
-- Name: referer_lut referer_lut_pkey; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.referer_lut
    ADD CONSTRAINT referer_lut_pkey PRIMARY KEY (id);


--
-- Name: service_lut service_exists; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.service_lut
    ADD CONSTRAINT service_exists UNIQUE (folder_id, service, service_type);


--
-- Name: service_lut service_lut_pkey; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.service_lut
    ADD CONSTRAINT service_lut_pkey PRIMARY KEY (id);


--
-- Name: user_agent_lut user_agent_exists; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.user_agent_lut
    ADD CONSTRAINT user_agent_exists UNIQUE (name);


--
-- Name: user_agent_lut user_agent_lut_pkey; Type: CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.user_agent_lut
    ADD CONSTRAINT user_agent_lut_pkey PRIMARY KEY (id);


--
-- Name: burst_date_idx; Type: INDEX; Schema: idpgis; Owner: jevans
--

CREATE INDEX burst_date_idx ON idpgis.burst USING btree (date);


--
-- Name: ip_address_logs_date_idx; Type: INDEX; Schema: idpgis; Owner: jevans
--

CREATE INDEX ip_address_logs_date_idx ON idpgis.ip_address_logs USING btree (date);


--
-- Name: ip_address_logs_id_idx; Type: INDEX; Schema: idpgis; Owner: jevans
--

CREATE INDEX ip_address_logs_id_idx ON idpgis.ip_address_logs USING btree (id);


--
-- Name: referer_logs_date_idx; Type: INDEX; Schema: idpgis; Owner: jevans
--

CREATE INDEX referer_logs_date_idx ON idpgis.referer_logs USING btree (date);


--
-- Name: referer_logs_id_idx; Type: INDEX; Schema: idpgis; Owner: jevans
--

CREATE INDEX referer_logs_id_idx ON idpgis.referer_logs USING btree (id);


--
-- Name: user_agent_logs_date_idx; Type: INDEX; Schema: idpgis; Owner: jevans
--

CREATE INDEX user_agent_logs_date_idx ON idpgis.user_agent_logs USING btree (date);


--
-- Name: user_agent_logs_id_idx; Type: INDEX; Schema: idpgis; Owner: jevans
--

CREATE INDEX user_agent_logs_id_idx ON idpgis.user_agent_logs USING btree (id);


--
-- Name: ip_address_logs ip_address_logs_id_fkey; Type: FK CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.ip_address_logs
    ADD CONSTRAINT ip_address_logs_id_fkey FOREIGN KEY (id) REFERENCES idpgis.ip_address_lut(id) ON DELETE CASCADE;


--
-- Name: referer_logs referer_logs_id_fkey; Type: FK CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.referer_logs
    ADD CONSTRAINT referer_logs_id_fkey FOREIGN KEY (id) REFERENCES idpgis.referer_lut(id) ON DELETE CASCADE;


--
-- Name: service_logs service_logs_id_fkey; Type: FK CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.service_logs
    ADD CONSTRAINT service_logs_id_fkey FOREIGN KEY (id) REFERENCES idpgis.service_lut(id) ON DELETE CASCADE;


--
-- Name: service_lut service_lut_folder_id_fkey; Type: FK CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.service_lut
    ADD CONSTRAINT service_lut_folder_id_fkey FOREIGN KEY (folder_id) REFERENCES idpgis.folder_lut(id) ON DELETE CASCADE;


--
-- Name: user_agent_logs user_agent_logs_id_fkey; Type: FK CONSTRAINT; Schema: idpgis; Owner: jevans
--

ALTER TABLE ONLY idpgis.user_agent_logs
    ADD CONSTRAINT user_agent_logs_id_fkey FOREIGN KEY (id) REFERENCES idpgis.user_agent_lut(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

