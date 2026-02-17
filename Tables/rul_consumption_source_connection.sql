CREATE TABLE public.rul_consumption_source_connection (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    accounting_type_node_id bigint,
    node_calculate_parameter_id bigint,
    accounting_type_id bigint,
    value numeric,
    coefficient numeric DEFAULT 1,
    theoretical_calculation boolean DEFAULT true,
    balancing_coefficient numeric,
    balancing_id bigint,
    note character varying(256)
);

COMMENT ON COLUMN public.rul_consumption_source_connection.coefficient IS 'Коэффициент распределения';
COMMENT ON COLUMN public.rul_consumption_source_connection.theoretical_calculation IS 'Флаг указывающий на то, брать ли в начисления расход';
COMMENT ON COLUMN public.rul_consumption_source_connection.balancing_coefficient IS 'Коэффициент балансировки';
COMMENT ON COLUMN public.rul_consumption_source_connection.note IS 'Обоснование';
