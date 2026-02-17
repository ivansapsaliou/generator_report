CREATE TABLE public.rul_planned_consumption (
    planned_consumption_id bigint DEFAULT nextval('rul_planned_consumption_planned_consumption_id_seq'::regclass) NOT NULL,
    connection_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    planned_consumption_value numeric,
    advance_payment_percent numeric,
    payment_date timestamp without time zone,
    description character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_planned_consumption_pkey PRIMARY KEY (planned_consumption_id),
    CONSTRAINT fk_connection_id FOREIGN KEY (connection_id) REFERENCES rul_connection(connection_id)
);

COMMENT ON COLUMN public.rul_planned_consumption.connection_id IS 'Ссылка на подключение';
COMMENT ON COLUMN public.rul_planned_consumption.start_date IS 'Дата отчетного месяца';
COMMENT ON COLUMN public.rul_planned_consumption.end_date IS 'Дата отчетного месяца';
COMMENT ON COLUMN public.rul_planned_consumption.planned_consumption_value IS 'Плановое месячное потребление';
COMMENT ON COLUMN public.rul_planned_consumption.advance_payment_percent IS 'Плановый авансовый платеж';
COMMENT ON COLUMN public.rul_planned_consumption.payment_date IS 'Дата платежа';
COMMENT ON COLUMN public.rul_planned_consumption.description IS 'Обоснование';
