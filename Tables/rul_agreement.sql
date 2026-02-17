CREATE TABLE public.rul_agreement (
    agreement_id bigint DEFAULT nextval('rul_agreement_agreement_id_seq'::regclass) NOT NULL,
    parent_agreement_id bigint,
    supplier_client_id bigint,
    customer_client_id bigint,
    billing_period_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    agreement_code character varying(256),
    agreement_name character varying(256),
    description character varying(2048),
    supplier_user_id bigint,
    customer_user_id bigint,
    supplier_document_name character varying(256),
    supplier_document_info character varying(256),
    customer_document_name character varying(256),
    customer_document_info character varying(256),
    supplier_description character varying(256),
    customer_description character varying(256),
    is_active numeric(1,0),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    owner_client_id bigint,
    owner_user_id bigint,
    owner_document_name character varying(256),
    owner_document_info character varying(256),
    pay_day_count bigint,
    penalty numeric,
    supplier_responsible_user_id bigint,
    customer_responsible_user_id bigint,
    owner_agreement_id bigint,
    agreement_type_id bigint,
    penalty_operation_template_id bigint,
    code_ay character varying(64),
    payment_mechanism_id bigint,
    second_supplier_user_id bigint,
    signing_date timestamp without time zone
    ,
    CONSTRAINT rul_agreement_pkey PRIMARY KEY (agreement_id),
    CONSTRAINT fk_agreement_type_id FOREIGN KEY (agreement_type_id) REFERENCES rul_agreement_type(agreement_type_id),
    CONSTRAINT fk_billing_period_id FOREIGN KEY (billing_period_id) REFERENCES rul_billing_period(billing_period_id),
    CONSTRAINT fk_customer_client_id FOREIGN KEY (customer_client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_customer_responsible_user_id FOREIGN KEY (customer_responsible_user_id) REFERENCES rul_user(user_id),
    CONSTRAINT fk_customr_user_id FOREIGN KEY (customer_user_id) REFERENCES rul_user(user_id),
    CONSTRAINT fk_owner_client_id FOREIGN KEY (supplier_client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_owner_responsible_user_id FOREIGN KEY (supplier_responsible_user_id) REFERENCES rul_user(user_id),
    CONSTRAINT fk_owner_user_id FOREIGN KEY (supplier_user_id) REFERENCES rul_user(user_id),
    CONSTRAINT fk_parent_agreement_id FOREIGN KEY (parent_agreement_id) REFERENCES rul_agreement(agreement_id),
    CONSTRAINT fk_payment_mechanism_id FOREIGN KEY (payment_mechanism_id) REFERENCES rul_payment_mechanism(payment_mechanism_id),
    CONSTRAINT fk_penalty_operation_template_id FOREIGN KEY (penalty_operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_second_supplier_user_id FOREIGN KEY (second_supplier_user_id) REFERENCES rul_user(user_id),
    CONSTRAINT fk_subscriber_agreement_id FOREIGN KEY (owner_agreement_id) REFERENCES rul_agreement(agreement_id),
    CONSTRAINT fk_subscriber_client_id FOREIGN KEY (owner_client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_subscriber_user_id FOREIGN KEY (owner_user_id) REFERENCES rul_user(user_id)
);

COMMENT ON COLUMN public.rul_agreement.parent_agreement_id IS 'Если это доп соглашение, то указывает на родительский договор к которому создано доп соглашение. Сслыка на договор (сквозной айди)';
COMMENT ON COLUMN public.rul_agreement.supplier_client_id IS 'Сслыка на поставщика по  договору (клиент)';
COMMENT ON COLUMN public.rul_agreement.customer_client_id IS 'Слылка на плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.billing_period_id IS 'Переодичность расчетов';
COMMENT ON COLUMN public.rul_agreement.start_date IS 'Начало даты действия';
COMMENT ON COLUMN public.rul_agreement.end_date IS 'Конец даты действия';
COMMENT ON COLUMN public.rul_agreement.agreement_code IS 'Номер договора';
COMMENT ON COLUMN public.rul_agreement.agreement_name IS 'Название договора';
COMMENT ON COLUMN public.rul_agreement.description IS 'Описание договора';
COMMENT ON COLUMN public.rul_agreement.supplier_user_id IS 'Подписант со стороны поставщика';
COMMENT ON COLUMN public.rul_agreement.customer_user_id IS 'Подписант со стороны плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.supplier_document_name IS 'Действует на основании(Поставщик)';
COMMENT ON COLUMN public.rul_agreement.supplier_document_info IS 'Описание документа';
COMMENT ON COLUMN public.rul_agreement.customer_document_name IS 'Действует на основании(Абонент)';
COMMENT ON COLUMN public.rul_agreement.customer_document_info IS 'Описание документа';
COMMENT ON COLUMN public.rul_agreement.supplier_description IS 'Описание для поставщика';
COMMENT ON COLUMN public.rul_agreement.customer_description IS 'Описание для плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.owner_client_id IS 'Ссылка на владельца объекта';
COMMENT ON COLUMN public.rul_agreement.owner_user_id IS 'Подписант со стороны владельца объекта';
COMMENT ON COLUMN public.rul_agreement.owner_document_name IS 'Действует на основании(Субабонент)';
COMMENT ON COLUMN public.rul_agreement.owner_document_info IS 'Описание документа';
COMMENT ON COLUMN public.rul_agreement.pay_day_count IS 'Количество дней на оплату';
COMMENT ON COLUMN public.rul_agreement.penalty IS 'Пеня, % годовых';
COMMENT ON COLUMN public.rul_agreement.supplier_responsible_user_id IS 'Ответственное за рассчеты лицо поставщика';
COMMENT ON COLUMN public.rul_agreement.customer_responsible_user_id IS 'Ответственное за рассчеты лицо плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.owner_agreement_id IS 'При трехстороннем договоре указывает на двухсторонний с которым связан объект. Ссылка на договор владельца объекта.';
COMMENT ON COLUMN public.rul_agreement.agreement_type_id IS 'Ссылка на тип договора';
COMMENT ON COLUMN public.rul_agreement.penalty_operation_template_id IS 'ССылка на операцию по пене';
COMMENT ON COLUMN public.rul_agreement.code_ay IS 'Код АУ';
COMMENT ON COLUMN public.rul_agreement.payment_mechanism_id IS 'Ссылка на механизм оплаты';
COMMENT ON COLUMN public.rul_agreement.second_supplier_user_id IS 'Подписант поставщика №2';
COMMENT ON COLUMN public.rul_agreement.signing_date IS 'Дата подписания';
