CREATE TABLE public.rul_observation_period (
    observation_period_id bigint DEFAULT nextval('rul_observation_period_observation_period_id_seq'::regclass) NOT NULL,
    observation_period_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_observation_period_pkey PRIMARY KEY (observation_period_id)
);
