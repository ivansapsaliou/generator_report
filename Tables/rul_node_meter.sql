CREATE TABLE public.rul_node_meter (
    node_meter_id bigint DEFAULT nextval('rul_node_meter_node_meter_id_seq'::regclass) NOT NULL,
    meter_id bigint,
    node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_node_meter_pkey PRIMARY KEY (node_meter_id),
    CONSTRAINT fk_meter_id FOREIGN KEY (meter_id) REFERENCES rul_meter(meter_id),
    CONSTRAINT fk_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id)
);

COMMENT ON COLUMN public.rul_node_meter.meter_id IS 'Ссылка на прибор учета';
COMMENT ON COLUMN public.rul_node_meter.node_id IS 'Ссылка на узел установки';
COMMENT ON COLUMN public.rul_node_meter.start_date IS 'Начало даты действия';
COMMENT ON COLUMN public.rul_node_meter.end_date IS 'Конец даты действия';
