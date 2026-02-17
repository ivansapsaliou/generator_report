CREATE TABLE public.rul_construction (
    construction_id bigint DEFAULT nextval('rul_construction_construction_id_seq'::regclass) NOT NULL,
    construction_name character varying(128),
    parameter_target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_construction_pkey PRIMARY KEY (construction_id),
    CONSTRAINT fk_parameter_target_use_id FOREIGN KEY (parameter_target_use_id) REFERENCES rul_parameter_target_use(parameter_target_use_id)
);

COMMENT ON COLUMN public.rul_construction.construction_name IS 'Название конструкции';
COMMENT ON COLUMN public.rul_construction.parameter_target_use_id IS 'Ссылка на назначение';
