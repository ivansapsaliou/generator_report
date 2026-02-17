CREATE TABLE public.rul_debug_log (
    debug_log_id bigint DEFAULT nextval('rul_debug_log_debug_log_id_seq'::regclass) NOT NULL,
    debug_user_id character varying(64),
    debug_module character varying(64),
    debug_message character varying(512),
    debug_time timestamp without time zone DEFAULT (CURRENT_TIMESTAMP)::timestamp without time zone
    ,
    CONSTRAINT rul_debug_log_pkey PRIMARY KEY (debug_log_id)
);
