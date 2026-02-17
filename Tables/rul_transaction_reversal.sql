CREATE TABLE public.rul_transaction_reversal (
    transaction_reversal_id bigint DEFAULT nextval('rul_transaction_reversal_transaction_reversal_id_seq'::regclass) NOT NULL,
    source_correlation_transaction_id bigint,
    storn_correlation_transaction_id bigint,
    amount numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_transaction_reversal_pkey PRIMARY KEY (transaction_reversal_id)
);

COMMENT ON COLUMN public.rul_transaction_reversal.source_correlation_transaction_id IS 'Ссылка на проводки, источники по correlation_id';
COMMENT ON COLUMN public.rul_transaction_reversal.storn_correlation_transaction_id IS 'Ссылка на те проводки, которые надо сторнировать correlation_id';
COMMENT ON COLUMN public.rul_transaction_reversal.amount IS 'Сумма, возможно лишняя т.к. Сумма сторнирования есть в проводке';
