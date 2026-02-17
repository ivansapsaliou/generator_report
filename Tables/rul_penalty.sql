CREATE TABLE public.rul_penalty (
    penalty_id bigint DEFAULT nextval('rul_penalty_penalty_id_seq'::regclass) NOT NULL,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    amount numeric,
    source_invoice_id bigint,
    penalty_type_id bigint,
    penalty_value numeric,
    penalty_nds_value numeric,
    invoice_id bigint,
    transaction_transaction_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    transaction_reversal_id bigint,
    penalty numeric,
    is_clone smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_penalty_pkey PRIMARY KEY (penalty_id),
    CONSTRAINT fk_invoice_id FOREIGN KEY (invoice_id) REFERENCES rul_invoice(invoice_id),
    CONSTRAINT fk_penalty_type_id FOREIGN KEY (penalty_type_id) REFERENCES rul_penalty_type(penalty_type_id),
    CONSTRAINT fk_source_invoice_id FOREIGN KEY (source_invoice_id) REFERENCES rul_invoice(invoice_id),
    CONSTRAINT fk_transaction_transaction_id FOREIGN KEY (transaction_transaction_id) REFERENCES rul_transaction_transaction(transaction_transaction_id)
);

COMMENT ON COLUMN public.rul_penalty.start_date IS 'Дата начала штрафного периода';
COMMENT ON COLUMN public.rul_penalty.end_date IS 'Дата завершения штрафного периода';
COMMENT ON COLUMN public.rul_penalty.amount IS 'Расчетная сумма (что это?)';
COMMENT ON COLUMN public.rul_penalty.source_invoice_id IS 'Ссылка на счет на основе которого рассчитана пеня';
COMMENT ON COLUMN public.rul_penalty.penalty_type_id IS 'Тип пени';
COMMENT ON COLUMN public.rul_penalty.penalty_value IS 'Размер пени';
COMMENT ON COLUMN public.rul_penalty.penalty_nds_value IS 'Размер ндс пени (деньги)';
COMMENT ON COLUMN public.rul_penalty.invoice_id IS 'Ссылка на счет куда включено';
COMMENT ON COLUMN public.rul_penalty.transaction_transaction_id IS 'Ссылка на основание выставления пени';
COMMENT ON COLUMN public.rul_penalty.penalty IS 'Ставка по пене (Наследуется из договора)';
COMMENT ON COLUMN public.rul_penalty.is_clone IS 'Показывает, что пеня созданна в виде клона с инвертированной суммой';
