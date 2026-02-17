CREATE TABLE public.rul_average_value_argument (
    average_value_argument_id bigint DEFAULT nextval('rul_average_value_argument_average_value_argument_id_seq'::regclass) NOT NULL,
    argument_formula_id bigint,
    value numeric,
    average_value_id bigint,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_average_value_argument_pkey PRIMARY KEY (average_value_argument_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_average_value_id FOREIGN KEY (average_value_id) REFERENCES rul_average_value(average_value_id),
    CONSTRAINT fk_op_user_id FOREIGN KEY (op_user_id) REFERENCES rul_user(user_id)
);

COMMENT ON COLUMN public.rul_average_value_argument.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_average_value_argument.value IS 'Значение аргумента';
COMMENT ON COLUMN public.rul_average_value_argument.average_value_id IS 'Ссылка на расход по среднему';
