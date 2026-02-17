CREATE TABLE public.rul_observation_type (
    observation_type_id bigint DEFAULT nextval('rul_observation_type_observation_type_id_seq'::regclass) NOT NULL,
    observation_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_observation_type_pkey PRIMARY KEY (observation_type_id)
);
