CREATE TABLE public.rul_transaction_transaction (
    transaction_transaction_id bigint DEFAULT nextval('rul_transaction_transaction_transaction_transaction_id_seq'::regclass) NOT NULL,
    credit_transaction_id bigint,
    debit_transaction_id bigint,
    amount numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    operation_date timestamp without time zone
    ,
    CONSTRAINT rul_transaction_transaction_pkey PRIMARY KEY (transaction_transaction_id),
    CONSTRAINT fk_credit_transaction_id FOREIGN KEY (credit_transaction_id) REFERENCES rul_transaction_version(transaction_version_id),
    CONSTRAINT fk_debit_transaction_id FOREIGN KEY (debit_transaction_id) REFERENCES rul_transaction_version(transaction_version_id)
);

COMMENT ON COLUMN public.rul_transaction_transaction.credit_transaction_id IS 'Ссылка на версию кредитной проводки';
COMMENT ON COLUMN public.rul_transaction_transaction.debit_transaction_id IS 'Ссылка на версию дебетовой проводки';
COMMENT ON COLUMN public.rul_transaction_transaction.amount IS 'Сумма';
