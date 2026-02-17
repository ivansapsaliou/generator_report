CREATE TABLE public.rul_penalty_type (
    penalty_type_id bigint DEFAULT nextval('rul_penalty_type_penalty_type_id_seq'::regclass) NOT NULL,
    penalty_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_penalty_type_pkey PRIMARY KEY (penalty_type_id)
);

COMMENT ON COLUMN public.rul_penalty_type.penalty_type_name IS 'Тип пени';
