CREATE TABLE public.rul_load_standard_value (
    load_standard_value_id bigint DEFAULT nextval('rul_load_standard_value_load_standard_value_id_seq'::regclass) NOT NULL,
    argument_formula_id bigint,
    value numeric,
    version_load_standard_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_load_standard_value_pkey PRIMARY KEY (load_standard_value_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_version_load_standard_id FOREIGN KEY (version_load_standard_id) REFERENCES rul_version_load_standard(version_load_standard_id)
);

COMMENT ON COLUMN public.rul_load_standard_value.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_load_standard_value.value IS 'Значение норматива/нагрузки';
COMMENT ON COLUMN public.rul_load_standard_value.version_load_standard_id IS 'Ссылка на версию показателя';
