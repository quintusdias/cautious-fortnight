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
-- Name: nowcoast; Type: SCHEMA; Schema: -; Owner: jevans
--

CREATE SCHEMA nowcoast;


ALTER SCHEMA nowcoast OWNER TO jevans;

--
-- Name: svc_type_enum; Type: TYPE; Schema: nowcoast; Owner: jevans
--

CREATE TYPE nowcoast.svc_type_enum AS ENUM (
    'MapServer',
    'ImageServer',
    'FeatureServer'
);


ALTER TYPE nowcoast.svc_type_enum OWNER TO jevans;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: burst; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.burst (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE nowcoast.burst OWNER TO jevans;

--
-- Name: folder_lut; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.folder_lut (
    id integer NOT NULL,
    folder text
);


ALTER TABLE nowcoast.folder_lut OWNER TO jevans;

--
-- Name: folder_lut_id_seq; Type: SEQUENCE; Schema: nowcoast; Owner: jevans
--

CREATE SEQUENCE nowcoast.folder_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE nowcoast.folder_lut_id_seq OWNER TO jevans;

--
-- Name: folder_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: nowcoast; Owner: jevans
--

ALTER SEQUENCE nowcoast.folder_lut_id_seq OWNED BY nowcoast.folder_lut.id;


--
-- Name: ip_address_logs; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.ip_address_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE nowcoast.ip_address_logs OWNER TO jevans;

--
-- Name: TABLE ip_address_logs; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON TABLE nowcoast.ip_address_logs IS 'An IP address cannot have a summarizing set of statistics at the same time.';


--
-- Name: COLUMN ip_address_logs.id; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON COLUMN nowcoast.ip_address_logs.id IS 'identifies IP address in lookup table';


--
-- Name: ip_address_lut; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.ip_address_lut (
    id integer NOT NULL,
    ip_address inet
);


ALTER TABLE nowcoast.ip_address_lut OWNER TO jevans;

--
-- Name: ip_address_lut_id_seq; Type: SEQUENCE; Schema: nowcoast; Owner: jevans
--

CREATE SEQUENCE nowcoast.ip_address_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE nowcoast.ip_address_lut_id_seq OWNER TO jevans;

--
-- Name: ip_address_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: nowcoast; Owner: jevans
--

ALTER SEQUENCE nowcoast.ip_address_lut_id_seq OWNED BY nowcoast.ip_address_lut.id;


--
-- Name: referer_logs; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.referer_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE nowcoast.referer_logs OWNER TO jevans;

--
-- Name: TABLE referer_logs; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON TABLE nowcoast.referer_logs IS 'A referer cannot have a summarizing set of statistics at the same time.';


--
-- Name: COLUMN referer_logs.id; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON COLUMN nowcoast.referer_logs.id IS 'identifies referer in lookup table';


--
-- Name: referer_lut; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.referer_lut (
    id integer NOT NULL,
    name text
);


ALTER TABLE nowcoast.referer_lut OWNER TO jevans;

--
-- Name: referer_lut_id_seq; Type: SEQUENCE; Schema: nowcoast; Owner: jevans
--

CREATE SEQUENCE nowcoast.referer_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE nowcoast.referer_lut_id_seq OWNER TO jevans;

--
-- Name: referer_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: nowcoast; Owner: jevans
--

ALTER SEQUENCE nowcoast.referer_lut_id_seq OWNED BY nowcoast.referer_lut.id;


--
-- Name: service_logs; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.service_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    export_mapdraws bigint,
    wms_mapdraws bigint
);


ALTER TABLE nowcoast.service_logs OWNER TO jevans;

--
-- Name: TABLE service_logs; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON TABLE nowcoast.service_logs IS 'Aggregated summary statistics';

CREATE INDEX service_logs_date_idx ON nowcoast.service_logs USING btree (date);

--
-- Name: COLUMN service_logs.hits; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON COLUMN nowcoast.service_logs.hits IS 'Number of hits aggregated over a set time period (one hour?)';


--
-- Name: service_lut; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.service_lut (
    id integer NOT NULL,
    service text,
    folder_id integer,
    active boolean DEFAULT true,
    service_type nowcoast.svc_type_enum
);


ALTER TABLE nowcoast.service_lut OWNER TO jevans;

--
-- Name: TABLE service_lut; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON TABLE nowcoast.service_lut IS 'This table should not vary unless there is a new release at NCEP';


--
-- Name: COLUMN service_lut.active; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON COLUMN nowcoast.service_lut.active IS 'False if a service has been retired.';


--
-- Name: service_lut_id_seq; Type: SEQUENCE; Schema: nowcoast; Owner: jevans
--

CREATE SEQUENCE nowcoast.service_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE nowcoast.service_lut_id_seq OWNER TO jevans;

--
-- Name: service_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: nowcoast; Owner: jevans
--

ALTER SEQUENCE nowcoast.service_lut_id_seq OWNED BY nowcoast.service_lut.id;


--
-- Name: summary; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.summary (
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint,
    mapdraws bigint
);


ALTER TABLE nowcoast.summary OWNER TO jevans;

--
-- Name: user_agent_logs; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.user_agent_logs (
    id bigint,
    date timestamp with time zone,
    hits bigint,
    errors bigint,
    nbytes bigint
);


ALTER TABLE nowcoast.user_agent_logs OWNER TO jevans;

--
-- Name: COLUMN user_agent_logs.id; Type: COMMENT; Schema: nowcoast; Owner: jevans
--

COMMENT ON COLUMN nowcoast.user_agent_logs.id IS 'identifies user agent in lookup table';


--
-- Name: user_agent_lut; Type: TABLE; Schema: nowcoast; Owner: jevans
--

CREATE TABLE nowcoast.user_agent_lut (
    id integer NOT NULL,
    name text
);


ALTER TABLE nowcoast.user_agent_lut OWNER TO jevans;

--
-- Name: user_agent_lut_id_seq; Type: SEQUENCE; Schema: nowcoast; Owner: jevans
--

CREATE SEQUENCE nowcoast.user_agent_lut_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE nowcoast.user_agent_lut_id_seq OWNER TO jevans;

--
-- Name: user_agent_lut_id_seq; Type: SEQUENCE OWNED BY; Schema: nowcoast; Owner: jevans
--

ALTER SEQUENCE nowcoast.user_agent_lut_id_seq OWNED BY nowcoast.user_agent_lut.id;


--
-- Name: folder_lut id; Type: DEFAULT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.folder_lut ALTER COLUMN id SET DEFAULT nextval('nowcoast.folder_lut_id_seq'::regclass);


--
-- Name: ip_address_lut id; Type: DEFAULT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.ip_address_lut ALTER COLUMN id SET DEFAULT nextval('nowcoast.ip_address_lut_id_seq'::regclass);


--
-- Name: referer_lut id; Type: DEFAULT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.referer_lut ALTER COLUMN id SET DEFAULT nextval('nowcoast.referer_lut_id_seq'::regclass);


--
-- Name: service_lut id; Type: DEFAULT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.service_lut ALTER COLUMN id SET DEFAULT nextval('nowcoast.service_lut_id_seq'::regclass);


--
-- Name: user_agent_lut id; Type: DEFAULT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.user_agent_lut ALTER COLUMN id SET DEFAULT nextval('nowcoast.user_agent_lut_id_seq'::regclass);


--
-- Name: folder_lut folder_exists; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.folder_lut
    ADD CONSTRAINT folder_exists UNIQUE (folder);


--
-- Name: folder_lut folder_lut_pkey; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.folder_lut
    ADD CONSTRAINT folder_lut_pkey PRIMARY KEY (id);


--
-- Name: ip_address_lut ip_address_exists; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.ip_address_lut
    ADD CONSTRAINT ip_address_exists UNIQUE (ip_address);


--
-- Name: ip_address_lut ip_address_lut_pkey; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.ip_address_lut
    ADD CONSTRAINT ip_address_lut_pkey PRIMARY KEY (id);


--
-- Name: referer_lut referer_exists; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.referer_lut
    ADD CONSTRAINT referer_exists UNIQUE (name);


--
-- Name: referer_lut referer_lut_pkey; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.referer_lut
    ADD CONSTRAINT referer_lut_pkey PRIMARY KEY (id);


--
-- Name: service_lut service_exists; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.service_lut
    ADD CONSTRAINT service_exists UNIQUE (folder_id, service, service_type);


--
-- Name: service_lut service_lut_pkey; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.service_lut
    ADD CONSTRAINT service_lut_pkey PRIMARY KEY (id);


--
-- Name: user_agent_lut user_agent_exists; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.user_agent_lut
    ADD CONSTRAINT user_agent_exists UNIQUE (name);


--
-- Name: user_agent_lut user_agent_lut_pkey; Type: CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.user_agent_lut
    ADD CONSTRAINT user_agent_lut_pkey PRIMARY KEY (id);


--
-- Name: burst_date_idx; Type: INDEX; Schema: nowcoast; Owner: jevans
--

CREATE INDEX burst_date_idx ON nowcoast.burst USING btree (date);


--
-- Name: ip_address_logs_date_idx; Type: INDEX; Schema: nowcoast; Owner: jevans
--

CREATE INDEX ip_address_logs_date_idx ON nowcoast.ip_address_logs USING btree (date);


--
-- Name: ip_address_logs_id_idx; Type: INDEX; Schema: nowcoast; Owner: jevans
--

CREATE INDEX ip_address_logs_id_idx ON nowcoast.ip_address_logs USING btree (id);


--
-- Name: referer_logs_date_idx; Type: INDEX; Schema: nowcoast; Owner: jevans
--

CREATE INDEX referer_logs_date_idx ON nowcoast.referer_logs USING btree (date);


--
-- Name: referer_logs_id_idx; Type: INDEX; Schema: nowcoast; Owner: jevans
--

CREATE INDEX referer_logs_id_idx ON nowcoast.referer_logs USING btree (id);


--
-- Name: user_agent_logs_date_idx; Type: INDEX; Schema: nowcoast; Owner: jevans
--

CREATE INDEX user_agent_logs_date_idx ON nowcoast.user_agent_logs USING btree (date);


--
-- Name: user_agent_logs_id_idx; Type: INDEX; Schema: nowcoast; Owner: jevans
--

CREATE INDEX user_agent_logs_id_idx ON nowcoast.user_agent_logs USING btree (id);


--
-- Name: ip_address_logs ip_address_logs_id_fkey; Type: FK CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.ip_address_logs
    ADD CONSTRAINT ip_address_logs_id_fkey FOREIGN KEY (id) REFERENCES nowcoast.ip_address_lut(id) ON DELETE CASCADE;


--
-- Name: referer_logs referer_logs_id_fkey; Type: FK CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.referer_logs
    ADD CONSTRAINT referer_logs_id_fkey FOREIGN KEY (id) REFERENCES nowcoast.referer_lut(id) ON DELETE CASCADE;


--
-- Name: service_logs service_logs_id_fkey; Type: FK CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.service_logs
    ADD CONSTRAINT service_logs_id_fkey FOREIGN KEY (id) REFERENCES nowcoast.service_lut(id) ON DELETE CASCADE;


--
-- Name: service_lut service_lut_folder_id_fkey; Type: FK CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.service_lut
    ADD CONSTRAINT service_lut_folder_id_fkey FOREIGN KEY (folder_id) REFERENCES nowcoast.folder_lut(id) ON DELETE CASCADE;


--
-- Name: user_agent_logs user_agent_logs_id_fkey; Type: FK CONSTRAINT; Schema: nowcoast; Owner: jevans
--

ALTER TABLE ONLY nowcoast.user_agent_logs
    ADD CONSTRAINT user_agent_logs_id_fkey FOREIGN KEY (id) REFERENCES nowcoast.user_agent_lut(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

