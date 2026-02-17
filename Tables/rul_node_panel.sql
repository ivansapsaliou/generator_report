CREATE TABLE public.rul_node_panel (
    node_panel_id bigint DEFAULT nextval('rul_node_panel_node_panel_id_seq'::regclass) NOT NULL,
    panel_id bigint,
    parameter_id bigint,
    node_meter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_node_panel_pkey PRIMARY KEY (node_panel_id),
    CONSTRAINT fk_node_meter_id FOREIGN KEY (node_meter_id) REFERENCES rul_node_meter(node_meter_id),
    CONSTRAINT fk_panel_id FOREIGN KEY (panel_id) REFERENCES rul_panel(panel_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);

COMMENT ON COLUMN public.rul_node_panel.panel_id IS 'Ссылка на панель прибора учета';
COMMENT ON COLUMN public.rul_node_panel.parameter_id IS 'Ссылка на измеряемый параметр';
COMMENT ON COLUMN public.rul_node_panel.node_meter_id IS 'Ссылка на размещение счетчика в узле';
