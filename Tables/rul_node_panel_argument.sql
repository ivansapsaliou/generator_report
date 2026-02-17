CREATE TABLE public.rul_node_panel_argument (
    node_panel_argument_id bigint DEFAULT nextval('rul_node_panel_argument_node_panel_argument_id_seq'::regclass) NOT NULL,
    node_panel_id bigint,
    argument_formula_id bigint,
    conversion_factor numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    accounting_type_node_id bigint
    ,
    CONSTRAINT rul_node_panel_argument_pkey PRIMARY KEY (node_panel_argument_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_node_panel_id FOREIGN KEY (node_panel_id) REFERENCES rul_node_panel(node_panel_id)
);

COMMENT ON COLUMN public.rul_node_panel_argument.node_panel_id IS 'Ссылка на параметр измеряемый в узле';
COMMENT ON COLUMN public.rul_node_panel_argument.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_node_panel_argument.conversion_factor IS 'переводной коэффициент';
COMMENT ON COLUMN public.rul_node_panel_argument.accounting_type_node_id IS 'Ссылка на конкретный способ учета в узле';
