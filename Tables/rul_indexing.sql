CREATE TABLE public.rul_indexing (
    indexing_id bigint DEFAULT nextval('rul_indexing_indexing_id_seq'::regclass) NOT NULL,
    charge_id bigint,
    percent_index_consumption numeric,
    index_date timestamp without time zone,
    currency_rate numeric,
    index_value numeric,
    index_coefficient numeric,
    index_amount numeric,
    index_nds numeric,
    invoice_id bigint,
    transaction_transaction_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    transaction_reversal_id bigint,
    is_clone smallint DEFAULT 0 NOT NULL,
    percent_charge_repayment numeric,
    index_operation_code character varying(64)
    ,
    CONSTRAINT rul_indexing_pkey PRIMARY KEY (indexing_id),
    CONSTRAINT fk_charge_id FOREIGN KEY (charge_id) REFERENCES rul_charge(charge_id),
    CONSTRAINT fk_invoice_id FOREIGN KEY (invoice_id) REFERENCES rul_invoice(invoice_id),
    CONSTRAINT fk_transaction_transaction_id FOREIGN KEY (transaction_transaction_id) REFERENCES rul_transaction_transaction(transaction_transaction_id)
);

COMMENT ON COLUMN public.rul_indexing.charge_id IS 'Ссылка на начисление';
COMMENT ON COLUMN public.rul_indexing.percent_index_consumption IS 'Индексируемый процент расхода';
COMMENT ON COLUMN public.rul_indexing.index_date IS 'Дата индексации';
COMMENT ON COLUMN public.rul_indexing.currency_rate IS 'Курс ваюты на дату проведения индексации';
COMMENT ON COLUMN public.rul_indexing.index_value IS 'Проидексированный тариф';
COMMENT ON COLUMN public.rul_indexing.index_coefficient IS 'Коэффициент индексации';
COMMENT ON COLUMN public.rul_indexing.index_amount IS 'Сумма индексации без НДС';
COMMENT ON COLUMN public.rul_indexing.index_nds IS 'НДС от суммы индексации';
COMMENT ON COLUMN public.rul_indexing.invoice_id IS 'Ссылка на счет куда включено';
COMMENT ON COLUMN public.rul_indexing.transaction_reversal_id IS 'Ссылка на сторнирующую запись, которая создала эту индексацию';
COMMENT ON COLUMN public.rul_indexing.is_clone IS 'Показывает, что индексация заведена как клонированная. Все клоннированые записи инвертируются с -.';
COMMENT ON COLUMN public.rul_indexing.percent_charge_repayment IS 'Погашено по начислению, процент';
