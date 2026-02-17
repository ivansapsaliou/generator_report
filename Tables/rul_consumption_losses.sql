CREATE TABLE public.rul_consumption_losses (
    line_id bigint,
    section_id bigint,
    v_p numeric,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    p numeric,
    g numeric,
    value numeric,
    connection_id bigint,
    theoretical_calculation boolean DEFAULT true,
    path bigint[],
    accounting_type_node_id bigint,
    coefficient numeric,
    balancing_coefficient numeric,
    balancing_id bigint,
    note character varying(256),
    is_balancing_losses numeric
);

COMMENT ON COLUMN public.rul_consumption_losses.coefficient IS 'Коэффициент расчета ГПУ';
COMMENT ON COLUMN public.rul_consumption_losses.balancing_coefficient IS 'Коэффициент Балансировки';
COMMENT ON COLUMN public.rul_consumption_losses.note IS 'Обоснование';
