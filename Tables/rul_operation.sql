CREATE TABLE public.rul_operation (
    operation_id bigint DEFAULT nextval('rul_operation_operation_id_seq'::regclass) NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    invoice_id bigint,
    operation_template_id bigint,
    operation_date timestamp without time zone
    ,
    CONSTRAINT rul_operation_pkey PRIMARY KEY (operation_id)
);

COMMENT ON COLUMN public.rul_operation.invoice_id IS 'Ссылка на счет фактуру';
COMMENT ON COLUMN public.rul_operation.operation_template_id IS 'Ссылка на шаблон операции';
COMMENT ON COLUMN public.rul_operation.operation_date IS 'Дата операции';
