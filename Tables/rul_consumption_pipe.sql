CREATE TABLE public.rul_consumption_pipe (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    value numeric,
    accounting_type_node_id bigint,
    node_calculate_parameter_id bigint
);
