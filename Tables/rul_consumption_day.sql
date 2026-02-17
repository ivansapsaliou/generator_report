CREATE TABLE public.rul_consumption_day (
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    node_panel_id bigint,
    node_panel_argument_id bigint
);
