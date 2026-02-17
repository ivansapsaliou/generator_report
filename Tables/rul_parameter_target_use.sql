CREATE TABLE public.rul_parameter_target_use (
    parameter_target_use_id bigint DEFAULT nextval('rul_parameter_target_use_parameter_target_use_id_seq'::regclass) NOT NULL,
    parameter_id bigint,
    target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_parameter_target_use_pkey PRIMARY KEY (parameter_target_use_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_target_use_id FOREIGN KEY (target_use_id) REFERENCES rul_target_use(target_use_id)
);

COMMENT ON COLUMN public.rul_parameter_target_use.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_parameter_target_use.target_use_id IS 'Ссылка на назначение';
