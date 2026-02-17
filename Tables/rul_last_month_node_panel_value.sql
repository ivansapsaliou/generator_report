CREATE TABLE public.rul_last_month_node_panel_value (
    node_panel_value_id bigint,
    value_number numeric,
    check_date timestamp without time zone,
    check_type_id bigint,
    node_panel_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint,
    is_correct numeric(1,0),
    changed_user_id bigint
);
