CREATE TABLE public.rul_method_tubing (
    method_tubing_id bigint DEFAULT nextval('rul_method_tubing_method_tubing_id_seq'::regclass) NOT NULL,
    method_tubing_name character varying(128),
    parameter_target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_method_tubing_pkey PRIMARY KEY (method_tubing_id),
    CONSTRAINT fk_parameter_target_use_id FOREIGN KEY (parameter_target_use_id) REFERENCES rul_parameter_target_use(parameter_target_use_id)
);

COMMENT ON COLUMN public.rul_method_tubing.method_tubing_name IS 'Название способа прокладки';
COMMENT ON COLUMN public.rul_method_tubing.parameter_target_use_id IS 'Ссылка на назначение';
