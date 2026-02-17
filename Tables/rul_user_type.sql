CREATE TABLE public.rul_user_type (
    user_type_id bigint DEFAULT nextval('rul_user_type_user_type_id_seq'::regclass) NOT NULL,
    user_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_user_type_pkey PRIMARY KEY (user_type_id)
);
