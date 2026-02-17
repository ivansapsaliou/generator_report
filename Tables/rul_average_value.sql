CREATE TABLE public.rul_average_value (
    average_value_id bigint DEFAULT nextval('rul_average_value_average_value_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    indication_start_date timestamp without time zone,
    indication_end_date timestamp without time zone
    ,
    CONSTRAINT rul_average_value_pkey PRIMARY KEY (average_value_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id)
);

COMMENT ON COLUMN public.rul_average_value.accounting_type_node_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_average_value.start_date IS 'Дата с (с которой начинает расчитываться кэф по среднему)';
COMMENT ON COLUMN public.rul_average_value.end_date IS 'Дата по (которой заканчивается расчет кэфа по среднему)';
COMMENT ON COLUMN public.rul_average_value.indication_start_date IS 'Дата, которая преобразована методом расчета по среднему';
COMMENT ON COLUMN public.rul_average_value.indication_end_date IS 'Дата, которая преобразована методом расчета по среднему';
