CREATE TABLE public.rul_first_indication (
    first_indication_id bigint DEFAULT nextval('rul_first_indication_first_indication_id_seq'::regclass) NOT NULL,
    value numeric,
    parameter_id bigint,
    node_meter_id bigint,
    check_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_first_indication_pkey PRIMARY KEY (first_indication_id),
    CONSTRAINT fk_node_meter_id FOREIGN KEY (node_meter_id) REFERENCES rul_node_meter(node_meter_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);

COMMENT ON COLUMN public.rul_first_indication.value IS 'Показание';
COMMENT ON COLUMN public.rul_first_indication.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_first_indication.node_meter_id IS 'Ссылка на размещение прибора учета';
