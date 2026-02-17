CREATE TABLE public.rul_invoice_confirm_status (
    invoice_confirm_status_id bigint DEFAULT nextval('rul_invoice_confirm_status_invoice_confirm_status_id_seq'::regclass) NOT NULL,
    invoice_confirm_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_invoice_confirm_status_pkey PRIMARY KEY (invoice_confirm_status_id)
);

COMMENT ON TABLE public.rul_invoice_confirm_status IS 'Таблица статусов подтверждения счет-фактуры';

COMMENT ON COLUMN public.rul_invoice_confirm_status.invoice_confirm_status_id IS 'Идентификатор статуса подтверждения счет-фактуры';
COMMENT ON COLUMN public.rul_invoice_confirm_status.invoice_confirm_status_name IS 'Название статуса подтверждения счет-фактуры';
COMMENT ON COLUMN public.rul_invoice_confirm_status.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_invoice_confirm_status.deleted IS 'Признак удаления записи';
