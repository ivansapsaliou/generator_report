CREATE TABLE public.rul_constant_value (
    constant_value_id bigint DEFAULT nextval('rul_constant_value_constant_value_id_seq'::regclass) NOT NULL,
    argument_formula_id bigint,
    version_constant_id bigint,
    value numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_constant_value_pkey PRIMARY KEY (constant_value_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_version_constant_id FOREIGN KEY (version_constant_id) REFERENCES rul_version_constant(version_constant_id)
);

COMMENT ON COLUMN public.rul_constant_value.argument_formula_id IS 'Ссылка аргумент формулы';
COMMENT ON COLUMN public.rul_constant_value.version_constant_id IS 'Ссылка на версию константы';
COMMENT ON COLUMN public.rul_constant_value.value IS 'Значение';
