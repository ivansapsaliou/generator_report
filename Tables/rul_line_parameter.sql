CREATE TABLE public.rul_line_parameter (
    line_parameter_id bigint DEFAULT nextval('rul_line_parameter_line_parameter_id_seq'::regclass) NOT NULL,
    line_id bigint,
    parameter_id bigint,
    node_calculate_parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    formula_id bigint
    ,
    CONSTRAINT rul_line_parameter_pkey PRIMARY KEY (line_parameter_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_line_id FOREIGN KEY (line_id) REFERENCES rul_line(line_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);

COMMENT ON COLUMN public.rul_line_parameter.line_id IS 'Ссылка на линию';
COMMENT ON COLUMN public.rul_line_parameter.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_line_parameter.node_calculate_parameter_id IS 'Ссылка на расчетный параметр, от которого идут "листья"';
COMMENT ON COLUMN public.rul_line_parameter.formula_id IS 'Ссылка на формулу расчета потерь';
