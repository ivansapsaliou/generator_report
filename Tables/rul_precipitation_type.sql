CREATE TABLE public.rul_precipitation_type (
    precipitation_type_id bigint DEFAULT nextval('rul_precipitation_type_precipitation_type_id_seq'::regclass) NOT NULL,
    precipitation_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_precipitation_type_pkey PRIMARY KEY (precipitation_type_id)
);
