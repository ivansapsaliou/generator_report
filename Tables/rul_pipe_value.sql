CREATE TABLE public.rul_pipe_value (
    pipe_value_id bigint DEFAULT nextval('rul_pipe_value_pipe_value_id_seq'::regclass) NOT NULL,
    value numeric,
    argument_formula_id bigint,
    accounting_type_node_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_pipe_value_pkey PRIMARY KEY (pipe_value_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id)
);

COMMENT ON COLUMN public.rul_pipe_value.value IS 'Значение аргумента по способу учета "по сечению"';
COMMENT ON COLUMN public.rul_pipe_value.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_pipe_value.accounting_type_node_id IS 'Ссылка на способ учета';
