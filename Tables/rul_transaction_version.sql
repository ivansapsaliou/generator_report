CREATE TABLE public.rul_transaction_version (
    transaction_version_id bigint DEFAULT nextval('rul_transaction_version_transaction_version_id_seq'::regclass) NOT NULL,
    payment_percent numeric,
    transaction_reversal_id bigint,
    create_date timestamp without time zone,
    month timestamp without time zone,
    transaction_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    is_actual boolean DEFAULT true
    ,
    CONSTRAINT rul_transaction_version_pkey PRIMARY KEY (transaction_version_id),
    CONSTRAINT rul_transaction_version_idx UNIQUE (month, is_actual, transaction_id, transaction_reversal_id),
    CONSTRAINT fk_transaction_id FOREIGN KEY (transaction_id) REFERENCES rul_transaction(transaction_id),
    CONSTRAINT fk_transaction_reversal_id FOREIGN KEY (transaction_reversal_id) REFERENCES rul_transaction_reversal(transaction_reversal_id)
);

COMMENT ON COLUMN public.rul_transaction_version.payment_percent IS 'Процент погашения проводки на начало месяца';
COMMENT ON COLUMN public.rul_transaction_version.transaction_reversal_id IS 'Ссылка на сторнирующую проводку из-за которой появилась версия';
COMMENT ON COLUMN public.rul_transaction_version.create_date IS 'Дата когда создана была версия (по ней актуальность определяется)';
COMMENT ON COLUMN public.rul_transaction_version.month IS 'Месяц для которого новая версия';
COMMENT ON COLUMN public.rul_transaction_version.transaction_id IS 'Ссылка на проводку';
COMMENT ON COLUMN public.rul_transaction_version.is_actual IS 'Храним знак актуальности';
