CREATE TABLE public.rul_consumption (
    consumption_id bigint DEFAULT nextval('rul_consumption_consumption_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    value numeric,
    is_unbalanced numeric(1,0) DEFAULT 0 NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    node_panel_id bigint,
    node_panel_argument_id bigint,
    avg_day_value numeric,
    connection_id bigint,
    note character varying(256)
    ,
    CONSTRAINT rul_consumption_pkey PRIMARY KEY (consumption_id)
);

COMMENT ON COLUMN public.rul_consumption.accounting_type_node_id IS 'Ссылка на параметр спочобо учета в узле';
COMMENT ON COLUMN public.rul_consumption.start_date IS 'Дата с (расхода)';
COMMENT ON COLUMN public.rul_consumption.end_date IS 'Дата по (расход)';
COMMENT ON COLUMN public.rul_consumption.value IS 'Сам расход';
COMMENT ON COLUMN public.rul_consumption.node_panel_id IS 'Ссылка на параметр измеряемый узлом';
COMMENT ON COLUMN public.rul_consumption.avg_day_value IS 'Средний расход за день';
COMMENT ON COLUMN public.rul_consumption.connection_id IS '28.11. Устаревшее, сейчес не использую. Ссылка на подключение';
COMMENT ON COLUMN public.rul_consumption.note IS 'Обоснование';
