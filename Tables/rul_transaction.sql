CREATE TABLE public.rul_transaction (
    transaction_id bigint DEFAULT nextval('rul_transaction_transaction_id_seq'::regclass) NOT NULL,
    client_id bigint,
    operation_id bigint,
    subconto_type_id bigint,
    content character varying(256),
    amount numeric,
    transaction_type_id bigint,
    create_date timestamp without time zone,
    accept_date timestamp without time zone,
    calculated_date timestamp without time zone,
    operation_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    transaction_template_id bigint,
    is_system boolean DEFAULT true,
    debit_subinvoice character varying(128),
    credit_subinvoice character varying(128),
    document character varying(256),
    external_id bigint,
    code_ay character varying(64),
    operation_code character varying(64),
    correlation_transaction_id bigint,
    is_debit smallint DEFAULT 1 NOT NULL
    ,
    CONSTRAINT rul_transaction_pkey PRIMARY KEY (transaction_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_operation_id FOREIGN KEY (operation_id) REFERENCES rul_operation(operation_id),
    CONSTRAINT fk_subconto_type_id FOREIGN KEY (subconto_type_id) REFERENCES rul_subconto_type(subconto_type_id),
    CONSTRAINT fk_transaction_template_id FOREIGN KEY (transaction_template_id) REFERENCES rul_transaction_template(transaction_template_id),
    CONSTRAINT fk_transaction_type_id FOREIGN KEY (transaction_type_id) REFERENCES rul_transaction_type(transaction_type_id)
);

COMMENT ON COLUMN public.rul_transaction.client_id IS 'Поставщик';
COMMENT ON COLUMN public.rul_transaction.operation_id IS 'Ссылка на операцию';
COMMENT ON COLUMN public.rul_transaction.subconto_type_id IS 'Вид субконто';
COMMENT ON COLUMN public.rul_transaction.content IS 'Содержание';
COMMENT ON COLUMN public.rul_transaction.amount IS 'Сумма';
COMMENT ON COLUMN public.rul_transaction.transaction_type_id IS 'Ссылка на тип проводки';
COMMENT ON COLUMN public.rul_transaction.create_date IS 'Дата регистрации';
COMMENT ON COLUMN public.rul_transaction.accept_date IS 'Дата подтверждения';
COMMENT ON COLUMN public.rul_transaction.calculated_date IS 'Расчетная дата';
COMMENT ON COLUMN public.rul_transaction.operation_date IS 'Дата операции';
COMMENT ON COLUMN public.rul_transaction.op_date IS 'Дата изменения';
COMMENT ON COLUMN public.rul_transaction.transaction_template_id IS 'Ссылка на шаблон проводки';
COMMENT ON COLUMN public.rul_transaction.is_system IS 'Системная ли проводка';
COMMENT ON COLUMN public.rul_transaction.debit_subinvoice IS 'Дебетовый счет';
COMMENT ON COLUMN public.rul_transaction.credit_subinvoice IS 'Кредитовый счет';
COMMENT ON COLUMN public.rul_transaction.document IS 'Документ';
COMMENT ON COLUMN public.rul_transaction.external_id IS 'ID внешней системы';
COMMENT ON COLUMN public.rul_transaction.code_ay IS 'Код АУ';
COMMENT ON COLUMN public.rul_transaction.operation_code IS 'Код операции';
COMMENT ON COLUMN public.rul_transaction.correlation_transaction_id IS 'Сквозной айди (будем проставлять айди дебетовой проводки при ее размножении на кредит и дебет)';
COMMENT ON COLUMN public.rul_transaction.is_debit IS 'Указатель на проводку (1 - дебетовая, 0 - кредитовая)';
