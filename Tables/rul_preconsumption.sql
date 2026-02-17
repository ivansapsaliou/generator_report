CREATE TABLE public.rul_preconsumption (
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    node_panel_id bigint,
    node_panel_argument_id bigint,
    value_number numeric
);

COMMENT ON COLUMN public.rul_preconsumption.value_number IS 'Показание, используется для обоснований';
