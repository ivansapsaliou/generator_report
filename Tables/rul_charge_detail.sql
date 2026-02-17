CREATE TABLE public.rul_charge_detail (
    charge_detail_id bigint DEFAULT nextval('rul_charge_detail_charge_detail_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    note character varying(2048),
    charge_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    rate_value_id bigint,
    connection_id bigint,
    note2 character varying(2048)
    ,
    CONSTRAINT rul_charge_detail_pkey PRIMARY KEY (charge_detail_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id)
);

COMMENT ON COLUMN public.rul_charge_detail.accounting_type_node_id IS 'Ссылка на параметр способ учета в узле';
COMMENT ON COLUMN public.rul_charge_detail.start_date IS 'Дата с (расхода)';
COMMENT ON COLUMN public.rul_charge_detail.end_date IS 'Дата по (расход)';
COMMENT ON COLUMN public.rul_charge_detail.consumption IS 'Сам расход';
COMMENT ON COLUMN public.rul_charge_detail.note IS 'Примечание';
COMMENT ON COLUMN public.rul_charge_detail.charge_id IS 'Ссылка на начисление';
COMMENT ON COLUMN public.rul_charge_detail.rate_value_id IS 'Ссылка на значение тарифа';
