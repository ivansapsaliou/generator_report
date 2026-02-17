CREATE TABLE public.rul_accounting_type_node (
    accounting_type_node_id bigint DEFAULT nextval('rul_accounting_type_node_accounting_type_node_id_seq'::regclass) NOT NULL,
    accounting_type_id bigint,
    node_calculate_parameter_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    formula_id bigint
    ,
    CONSTRAINT rul_accounting_type_node_pkey PRIMARY KEY (accounting_type_node_id),
    CONSTRAINT fk_accounting_type_id FOREIGN KEY (accounting_type_id) REFERENCES rul_accounting_type(accounting_type_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id)
);

COMMENT ON COLUMN public.rul_accounting_type_node.accounting_type_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_accounting_type_node.node_calculate_parameter_id IS 'Ссылка на рассчетный параметр в узле';
COMMENT ON COLUMN public.rul_accounting_type_node.formula_id IS 'Ссылка на формулу';
