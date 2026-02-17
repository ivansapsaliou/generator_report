CREATE TABLE public.rul_target_use (
    target_use_id bigint DEFAULT nextval('rul_target_use_target_use_id_seq'::regclass) NOT NULL,
    target_use_name character varying(1024),
    description character varying(1024),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_target_use_pkey PRIMARY KEY (target_use_id)
);

COMMENT ON COLUMN public.rul_target_use.target_use_name IS 'Назначение энергоресурса';
COMMENT ON COLUMN public.rul_target_use.description IS 'Описание назначения';
