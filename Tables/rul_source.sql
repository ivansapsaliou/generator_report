CREATE TABLE public.rul_source (
    source_id bigint DEFAULT nextval('rul_source_source_id_seq'::regclass) NOT NULL,
    source_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_source_pkey PRIMARY KEY (source_id)
);
