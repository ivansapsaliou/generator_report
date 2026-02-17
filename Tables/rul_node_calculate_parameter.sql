CREATE TABLE public.rul_node_calculate_parameter (
    node_calculate_parameter_id bigint DEFAULT nextval('rul_node_calculate_parameter_node_calculate_parameter_id_seq'::regclass) NOT NULL,
    parameter_id bigint,
    node_id bigint,
    target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    balancing_node_calculate_parameter_id bigint,
    commercial_node_calculate_parameter_id bigint
    ,
    CONSTRAINT rul_node_calculate_parameter_pkey PRIMARY KEY (node_calculate_parameter_id),
    CONSTRAINT fk_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_target_use_id FOREIGN KEY (target_use_id) REFERENCES rul_target_use(target_use_id)
);

COMMENT ON COLUMN public.rul_node_calculate_parameter.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_node_calculate_parameter.node_id IS 'Ссылка на узел учета';
COMMENT ON COLUMN public.rul_node_calculate_parameter.target_use_id IS 'Ссылка на назначение';
COMMENT ON COLUMN public.rul_node_calculate_parameter.balancing_node_calculate_parameter_id IS 'Ссылка на баланскный узел черз РП';
COMMENT ON COLUMN public.rul_node_calculate_parameter.commercial_node_calculate_parameter_id IS 'Указывает на верхний коммерческий узел';
