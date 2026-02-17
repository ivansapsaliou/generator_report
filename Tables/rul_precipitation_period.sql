CREATE TABLE public.rul_precipitation_period (
    precipitation_period_id bigint DEFAULT nextval('rul_precipitation_period_precipitation_period_id_seq'::regclass) NOT NULL,
    precipitation_period_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_precipitation_period_pkey PRIMARY KEY (precipitation_period_id)
);
