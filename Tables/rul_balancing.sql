CREATE TABLE public.rul_balancing (
    balancing_id bigint DEFAULT nextval('rul_balancing_balancing_id_seq'::regclass) NOT NULL,
    node_calculate_parameter_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    balancing_coefficient numeric,
    balancing_indication numeric,
    sum_losses_balance numeric,
    realize_consumption numeric,
    sum_losses_unbilled numeric
    ,
    CONSTRAINT rul_balancing_pkey PRIMARY KEY (balancing_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id)
);

COMMENT ON COLUMN public.rul_balancing.node_calculate_parameter_id IS 'Ссылка на расчетный параметр';
COMMENT ON COLUMN public.rul_balancing.start_date IS 'Дата начала периода';
COMMENT ON COLUMN public.rul_balancing.end_date IS 'Дата завершения периода';
COMMENT ON COLUMN public.rul_balancing.balancing_coefficient IS 'Коэффициент небаланса';
COMMENT ON COLUMN public.rul_balancing.balancing_indication IS 'Отпущено с узла';
COMMENT ON COLUMN public.rul_balancing.sum_losses_balance IS 'Потери поставщика';
COMMENT ON COLUMN public.rul_balancing.realize_consumption IS 'Реализовано';
COMMENT ON COLUMN public.rul_balancing.sum_losses_unbilled IS 'Сумма списываемых потерь';
