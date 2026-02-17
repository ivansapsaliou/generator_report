CREATE TABLE public.rul_source_consumption (
    source_consumption_id bigint DEFAULT nextval('rul_source_consumption_source_consumption_id_seq'::regclass) NOT NULL,
    source_consumption_name character varying(256),
    accounting_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_source_consumption_pkey PRIMARY KEY (source_consumption_id),
    CONSTRAINT fk_accounting_type_id FOREIGN KEY (accounting_type_id) REFERENCES rul_accounting_type(accounting_type_id)
);

COMMENT ON COLUMN public.rul_source_consumption.source_consumption_name IS 'Источник данных о расходах';
COMMENT ON COLUMN public.rul_source_consumption.accounting_type_id IS 'Ссылка на способ учета';
