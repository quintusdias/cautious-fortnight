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
-- Name: {myschema}; Type: SCHEMA; Schema: -; Owner: jevans
--

DROP SCHEMA IF EXISTS {myschema} CASCADE;
CREATE SCHEMA {myschema};


ALTER SCHEMA {myschema} OWNER TO jevans;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: burst; Type: TABLE; Schema: {myschema}; Owner: jevans
--

create type {myschema}.svc_type_enum as ENUM('MapServer', 'ImageServer', 'FeatureServer');

CREATE TABLE {myschema}.burst (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE {myschema}.burst OWNER TO jevans;

--
-- Name: folder_lut; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.folder_lut (
    id integer NOT NULL,
    folder text
);


ALTER TABLE {myschema}.folder_lut OWNER TO jevans;

--
-- Name: TABLE folder_lut; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON TABLE {myschema}.folder_lut IS 'This table should not vary unless there is a new release at NCEP, and usually not even then...';


--
-- Name: folder_lut_id_seq; Type: SEQUENCE; Schema: {myschema}; Owner: jevans
--

CREATE SEQUENCE {myschema}.folder_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE {myschema}.folder_lut_id_seq OWNER TO jevans;

--
-- Name: folder_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: {myschema}; Owner: jevans
--

ALTER SEQUENCE {myschema}.folder_lut_id_seq OWNED BY {myschema}.folder_lut.id;


--
-- Name: ip_address_logs; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.ip_address_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE {myschema}.ip_address_logs OWNER TO jevans;

--
-- Name: TABLE ip_address_logs; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON TABLE {myschema}.ip_address_logs IS 'An IP address cannot have a summarizing set of statistics at the same time.';


--
-- Name: COLUMN ip_address_logs.id; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON COLUMN {myschema}.ip_address_logs.id IS 'identifies referer in lookup table';


--
-- Name: ip_address_lut; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.ip_address_lut (
    id integer NOT NULL,
    ip_address inet
);


ALTER TABLE {myschema}.ip_address_lut OWNER TO jevans;

--
-- Name: ip_address_lut_id_seq; Type: SEQUENCE; Schema: {myschema}; Owner: jevans
--

CREATE SEQUENCE {myschema}.ip_address_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE {myschema}.ip_address_lut_id_seq OWNER TO jevans;

--
-- Name: ip_address_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: {myschema}; Owner: jevans
--

ALTER SEQUENCE {myschema}.ip_address_lut_id_seq OWNED BY {myschema}.ip_address_lut.id;


--
-- Name: referer_logs; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.referer_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE {myschema}.referer_logs OWNER TO jevans;

--
-- Name: TABLE referer_logs; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON TABLE {myschema}.referer_logs IS 'A referer cannot have a summarizing set of statistics at the same time.';


--
-- Name: COLUMN referer_logs.id; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON COLUMN {myschema}.referer_logs.id IS 'identifies referer in lookup table';


--
-- Name: referer_lut; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.referer_lut (
    id integer NOT NULL,
    name text
);


ALTER TABLE {myschema}.referer_lut OWNER TO jevans;

--
-- Name: referer_lut_id_seq; Type: SEQUENCE; Schema: {myschema}; Owner: jevans
--

CREATE SEQUENCE {myschema}.referer_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE {myschema}.referer_lut_id_seq OWNER TO jevans;

--
-- Name: referer_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: {myschema}; Owner: jevans
--

ALTER SEQUENCE {myschema}.referer_lut_id_seq OWNED BY {myschema}.referer_lut.id;


--
-- Name: service_logs; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.service_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    export_mapdraws bigint,
    wms_mapdraws bigint
);


ALTER TABLE {myschema}.service_logs OWNER TO jevans;

--
-- Name: TABLE service_logs; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON TABLE {myschema}.service_logs IS 'Aggregated summary statistics';


--
-- Name: COLUMN service_logs.hits; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON COLUMN {myschema}.service_logs.hits IS 'Number of hits aggregated over a set time period (one hour?)';


--
-- Name: service_lut; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.service_lut (
    id integer NOT NULL,
    active boolean DEFAULT true,
    service text,
    folder_id integer,
    service_type_id {myschema}.svc_type_enum
);


ALTER TABLE {myschema}.service_lut OWNER TO jevans;

--
-- Name: TABLE service_lut; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON TABLE {myschema}.service_lut IS 'This table should not vary unless there is a new release at NCEP';


--
-- Name: COLUMN service_lut.active; Type: COMMENT; Schema: {myschema}; Owner: jevans
--

COMMENT ON COLUMN {myschema}.service_lut.active IS 'False if a service has been retired.';


--
-- Name: service_lut_id_seq; Type: SEQUENCE; Schema: {myschema}; Owner: jevans
--

CREATE SEQUENCE {myschema}.service_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE {myschema}.service_lut_id_seq OWNER TO jevans;

--
-- Name: service_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: {myschema}; Owner: jevans
--

ALTER SEQUENCE {myschema}.service_lut_id_seq OWNED BY {myschema}.service_lut.id;


--
-- Name: summary; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.summary (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    mapdraws bigint
);


ALTER TABLE {myschema}.summary OWNER TO jevans;

--
-- Name: user_agent_logs; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.user_agent_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE {myschema}.user_agent_logs OWNER TO jevans;

--
-- Name: user_agent_lut; Type: TABLE; Schema: {myschema}; Owner: jevans
--

CREATE TABLE {myschema}.user_agent_lut (
    id integer NOT NULL,
    name text
);


ALTER TABLE {myschema}.user_agent_lut OWNER TO jevans;

--
-- Name: user_agent_lut_id_seq; Type: SEQUENCE; Schema: {myschema}; Owner: jevans
--

CREATE SEQUENCE {myschema}.user_agent_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE {myschema}.user_agent_lut_id_seq OWNER TO jevans;

--
-- Name: user_agent_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: {myschema}; Owner: jevans
--

ALTER SEQUENCE {myschema}.user_agent_lut_id_seq OWNED BY {myschema}.user_agent_lut.id;


--
-- Name: folder_lut id; Type: DEFAULT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.folder_lut ALTER COLUMN id SET DEFAULT nextval('{myschema}.folder_lut_id_seq'::regclass);


--
-- Name: ip_address_lut id; Type: DEFAULT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.ip_address_lut ALTER COLUMN id SET DEFAULT nextval('{myschema}.ip_address_lut_id_seq'::regclass);


--
-- Name: referer_lut id; Type: DEFAULT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.referer_lut ALTER COLUMN id SET DEFAULT nextval('{myschema}.referer_lut_id_seq'::regclass);


--
-- Name: service_lut id; Type: DEFAULT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.service_lut ALTER COLUMN id SET DEFAULT nextval('{myschema}.service_lut_id_seq'::regclass);


--
-- Name: user_agent_lut id; Type: DEFAULT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.user_agent_lut ALTER COLUMN id SET DEFAULT nextval('{myschema}.user_agent_lut_id_seq'::regclass);


--
-- Name: folder_lut folder_exists; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.folder_lut
    ADD CONSTRAINT folder_exists UNIQUE (folder);


--
-- Name: folder_lut folder_lut_pkey; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.folder_lut
    ADD CONSTRAINT folder_lut_pkey PRIMARY KEY (id);


--
-- Name: ip_address_lut ip_address_exists; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.ip_address_lut
    ADD CONSTRAINT ip_address_exists UNIQUE (ip_address);


--
-- Name: ip_address_lut ip_address_lut_pkey; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.ip_address_lut
    ADD CONSTRAINT ip_address_lut_pkey PRIMARY KEY (id);


--
-- Name: referer_lut referer_exists; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.referer_lut
    ADD CONSTRAINT referer_exists UNIQUE (name);


--
-- Name: referer_lut referer_lut_pkey; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.referer_lut
    ADD CONSTRAINT referer_lut_pkey PRIMARY KEY (id);


--
-- Name: service_lut service_exists; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.service_lut
    ADD CONSTRAINT service_exists UNIQUE (folder_id, service, service_type_id);


--
-- Name: service_lut service_lut_pkey; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.service_lut
    ADD CONSTRAINT service_lut_pkey PRIMARY KEY (id);


--
-- Name: user_agent_lut user_agent_exists; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.user_agent_lut
    ADD CONSTRAINT user_agent_exists UNIQUE (name);


--
-- Name: user_agent_lut user_agent_lut_pkey; Type: CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.user_agent_lut
    ADD CONSTRAINT user_agent_lut_pkey PRIMARY KEY (id);


--
-- Name: burst_date_idx; Type: INDEX; Schema: {myschema}; Owner: jevans
--

CREATE INDEX burst_date_idx ON {myschema}.burst USING btree (date);


--
-- Name: ip_address_logs ip_address_logs_id_fkey; Type: FK CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.ip_address_logs
    ADD CONSTRAINT ip_address_logs_id_fkey FOREIGN KEY (id) REFERENCES {myschema}.ip_address_lut(id) ON DELETE CASCADE;


--
-- Name: referer_logs referer_logs_id_fkey; Type: FK CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.referer_logs
    ADD CONSTRAINT referer_logs_id_fkey FOREIGN KEY (id) REFERENCES {myschema}.referer_lut(id) ON DELETE CASCADE;


--
-- Name: service_logs service_logs_id_fkey; Type: FK CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.service_logs
    ADD CONSTRAINT service_logs_id_fkey FOREIGN KEY (id) REFERENCES {myschema}.service_lut(id) ON DELETE CASCADE;


--
-- Name: service_lut service_lut_folder_id_fkey; Type: FK CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.service_lut
    ADD CONSTRAINT service_lut_folder_id_fkey FOREIGN KEY (folder_id) REFERENCES {myschema}.folder_lut(id) ON DELETE CASCADE;


--
-- Name: user_agent_logs user_agent_logs_id_fkey; Type: FK CONSTRAINT; Schema: {myschema}; Owner: jevans
--

ALTER TABLE ONLY {myschema}.user_agent_logs
    ADD CONSTRAINT user_agent_logs_id_fkey FOREIGN KEY (id) REFERENCES {myschema}.user_agent_lut(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

