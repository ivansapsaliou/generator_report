CREATE TABLE public.rul_user_status (
    user_status_id bigint DEFAULT nextval('rul_user_status_user_status_id_seq'::regclass) NOT NULL,
    user_status_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_user_status_pkey PRIMARY KEY (user_status_id)
);
