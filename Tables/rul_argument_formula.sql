CREATE TABLE public.rul_argument_formula (
    argument_formula_id bigint DEFAULT nextval('rul_argument_formula_argument_formula_id_seq'::regclass) NOT NULL,
    argument_formula_name character varying(256),
    argument_formula_code character varying(16),
    formula_id bigint,
    parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    argument_type_id bigint,
    argument_class_id bigint,
    unit_id bigint
    ,
    CONSTRAINT rul_argument_formula_pkey PRIMARY KEY (argument_formula_id),
    CONSTRAINT fk_argument_class_id FOREIGN KEY (argument_class_id) REFERENCES rul_argument_class(argument_class_id),
    CONSTRAINT fk_argument_type_id FOREIGN KEY (argument_type_id) REFERENCES rul_argument_type(argument_type_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_unit_id FOREIGN KEY (unit_id) REFERENCES rul_unit(unit_id)
);

COMMENT ON COLUMN public.rul_argument_formula.argument_formula_name IS 'Название аргумента формулы';
COMMENT ON COLUMN public.rul_argument_formula.argument_formula_code IS 'Код аргумента формулы';
COMMENT ON COLUMN public.rul_argument_formula.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_argument_formula.parameter_id IS 'Ссылка на Входной параметр(Так понимаю, тот по которому ищутся показания)';
COMMENT ON COLUMN public.rul_argument_formula.argument_type_id IS 'Ссылка на тип аргумента';
COMMENT ON COLUMN public.rul_argument_formula.argument_class_id IS 'Ссылка на класс аргумента';
COMMENT ON COLUMN public.rul_argument_formula.unit_id IS 'Ссылка на Единицу измерения';
