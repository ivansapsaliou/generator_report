CREATE TABLE public.rul_line_parameter_child (
    line_parameter_child_id bigint DEFAULT nextval('rul_line_parameter_child_line_parameter_child_id_seq'::regclass) NOT NULL,
    line_parameter_id bigint,
    node_calculate_parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_line_parameter_child_pkey PRIMARY KEY (line_parameter_child_id),
    CONSTRAINT fk_line_parameter_id FOREIGN KEY (line_parameter_id) REFERENCES rul_line_parameter(line_parameter_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id)
);

COMMENT ON COLUMN public.rul_line_parameter_child.line_parameter_id IS 'Ссылка на верхний узел(расчетный параметр) из дерева';
COMMENT ON COLUMN public.rul_line_parameter_child.node_calculate_parameter_id IS 'Ссылка на дочерний расчетный параметр';
