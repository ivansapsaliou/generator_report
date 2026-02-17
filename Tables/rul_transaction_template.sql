CREATE TABLE public.rul_transaction_template (
    transaction_template_id bigint DEFAULT nextval('rul_transaction_template_transaction_template_id_seq'::regclass) NOT NULL,
    operation_template_id bigint,
    debit_subinvoice character varying(128),
    credit_subinvoice character varying(128),
    source_data_transaction_id bigint NOT NULL,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_transaction_template_pkey PRIMARY KEY (transaction_template_id),
    CONSTRAINT fk_operation_template_id FOREIGN KEY (operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_source_data_transaction_id FOREIGN KEY (source_data_transaction_id) REFERENCES rul_source_data_transaction(source_data_transaction_id)
);

COMMENT ON COLUMN public.rul_transaction_template.operation_template_id IS 'Ссылка на шаблон операции';
COMMENT ON COLUMN public.rul_transaction_template.debit_subinvoice IS 'СчетДебет';
COMMENT ON COLUMN public.rul_transaction_template.credit_subinvoice IS 'СчетКредит';
COMMENT ON COLUMN public.rul_transaction_template.source_data_transaction_id IS 'Источник данных о сумме проводки';
COMMENT ON COLUMN public.rul_transaction_template.description IS 'Примечание';
