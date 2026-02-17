CREATE TABLE public.rul_invoice (
    invoice_id bigint DEFAULT nextval('rul_invoice_invoice_id_seq'::regclass) NOT NULL,
    invoice_code character varying(128) NOT NULL,
    agreement_id bigint,
    billing_start_date timestamp without time zone,
    billing_end_date timestamp without time zone,
    create_date timestamp without time zone,
    sum_amount numeric,
    sum_nds numeric,
    sum_amount_nds numeric,
    indexing_amount numeric,
    indexing_nds numeric,
    penalty numeric,
    balance numeric,
    total_amount numeric,
    deleted numeric(1,0) DEFAULT 0 NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    invoice_group_index bigint,
    penalty_amount numeric,
    penalty_nds numeric,
    invoice_type_id bigint DEFAULT 1,
    sum_amount_unnds numeric,
    indexing_amount_unnds numeric,
    penalty_amount_unnds numeric,
    pay_value numeric,
    invoice_confirm_status_id bigint DEFAULT 1,
    invoice_digital_signature_status_id bigint DEFAULT 1
    ,
    CONSTRAINT rul_invoice_pkey PRIMARY KEY (invoice_id),
    CONSTRAINT fk_agreement_id FOREIGN KEY (agreement_id) REFERENCES rul_agreement(agreement_id),
    CONSTRAINT fk_invoice_confirm_status_id FOREIGN KEY (invoice_confirm_status_id) REFERENCES rul_invoice_confirm_status(invoice_confirm_status_id),
    CONSTRAINT fk_invoice_type_id FOREIGN KEY (invoice_type_id) REFERENCES rul_invoice_type(invoice_type_id)
);

COMMENT ON COLUMN public.rul_invoice.invoice_code IS 'Номер счета (Буквы/цифры)';
COMMENT ON COLUMN public.rul_invoice.agreement_id IS 'Ссылка на договор';
COMMENT ON COLUMN public.rul_invoice.billing_start_date IS 'Дата начала расчетного периода';
COMMENT ON COLUMN public.rul_invoice.billing_end_date IS 'Дата завершения расчетного периода';
COMMENT ON COLUMN public.rul_invoice.create_date IS 'Дата выставления(подтверждения?) счета';
COMMENT ON COLUMN public.rul_invoice.sum_amount IS 'Начисления без НДС, облагаемые НДС';
COMMENT ON COLUMN public.rul_invoice.sum_nds IS 'НДС по начислениям';
COMMENT ON COLUMN public.rul_invoice.sum_amount_nds IS 'Не используется';
COMMENT ON COLUMN public.rul_invoice.indexing_amount IS 'Индексация без НДС, облагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.indexing_nds IS 'НДС по индексации';
COMMENT ON COLUMN public.rul_invoice.penalty IS '(возможно не используется) Размер пени';
COMMENT ON COLUMN public.rul_invoice.balance IS 'Сальдо на дату выставления (остаток)';
COMMENT ON COLUMN public.rul_invoice.total_amount IS 'Итого к оплате';
COMMENT ON COLUMN public.rul_invoice.invoice_group_index IS 'ИГС';
COMMENT ON COLUMN public.rul_invoice.penalty_amount IS 'Пеня без НДС, облагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.penalty_nds IS 'НДС по пене';
COMMENT ON COLUMN public.rul_invoice.invoice_type_id IS 'Ссылка на тип счета фактуры';
COMMENT ON COLUMN public.rul_invoice.sum_amount_unnds IS 'Начисления необлагаемые НДС';
COMMENT ON COLUMN public.rul_invoice.indexing_amount_unnds IS 'Индексация необлагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.penalty_amount_unnds IS 'Пеня не облагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.pay_value IS 'Оплачено в течении месяца (без понятия точно ли такое поле нужно было)';
COMMENT ON COLUMN public.rul_invoice.invoice_confirm_status_id IS 'Ссылка на ид. статуса подтверждения счет-фактуры';
