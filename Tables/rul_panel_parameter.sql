CREATE TABLE public.rul_panel_parameter (
    panel_parameter_id bigint DEFAULT nextval('rul_panel_parameter_panel_parameter_id_seq'::regclass) NOT NULL,
    panel_id bigint,
    parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_panel_parameter_pkey PRIMARY KEY (panel_parameter_id),
    CONSTRAINT fk_panel_id FOREIGN KEY (panel_id) REFERENCES rul_panel(panel_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);

COMMENT ON COLUMN public.rul_panel_parameter.panel_id IS 'Ссылка на панель бренда';
COMMENT ON COLUMN public.rul_panel_parameter.parameter_id IS 'Ссылка на параметр';
