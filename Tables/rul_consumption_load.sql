CREATE TABLE public.rul_consumption_load (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    formula_connection_id bigint,
    version_load_standard_id bigint,
    value numeric,
    accounting_type_node_id bigint,
    coefficient numeric DEFAULT 1,
    theoretical_calculation boolean DEFAULT true,
    balancing_coefficient numeric,
    description character varying(2048),
    balancing_id bigint,
    note character varying(256)
);

COMMENT ON COLUMN public.rul_consumption_load.coefficient IS 'Коэффициент распределения';
COMMENT ON COLUMN public.rul_consumption_load.theoretical_calculation IS 'Флаг указывающий на то, брать ли в начисления расход';
COMMENT ON COLUMN public.rul_consumption_load.balancing_coefficient IS 'Коэфициент балансировки';
COMMENT ON COLUMN public.rul_consumption_load.note IS 'Обоснование';
