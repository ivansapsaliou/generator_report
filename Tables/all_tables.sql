-- Таблица: public.a_111111
CREATE TABLE public.a_111111 (
    line_id bigint,
    line_name text,
    node_id bigint,
    child_id bigint,
    level integer,
    path bigint[],
    path_str text,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    accounting_type_id bigint,
    accounting_type_node_id bigint,
    connection_id bigint,
    branch_id bigint,
    group_recalculation_attitude_id bigint,
    allocation_source_consumption_id bigint,
    value numeric,
    connection_name character varying,
    val numeric,
    source_consumption_id integer
);

-- ======================================================================

-- Таблица: public.koef
CREATE TABLE public.koef (
    k numeric
);

-- ======================================================================

-- Таблица: public.report_templates
CREATE TABLE public.report_templates (
    id integer DEFAULT nextval('report_templates_id_seq'::regclass) NOT NULL,
    name text NOT NULL,
    config jsonb NOT NULL,
    created_by text,
    created_at timestamp with time zone DEFAULT now()
    ,
    CONSTRAINT report_templates_pkey PRIMARY KEY (id)
);

-- ======================================================================

-- Таблица: public.rul_access_token
CREATE TABLE public.rul_access_token (
    id bigint DEFAULT nextval('rul_access_token_id_seq'::regclass) NOT NULL,
    token character varying(64) NOT NULL,
    date_set timestamp(6) without time zone NOT NULL,
    description character varying(256)
    ,
    CONSTRAINT pk_access_token_id PRIMARY KEY (id)
);

-- ======================================================================

-- Таблица: public.rul_access_token_method
CREATE TABLE public.rul_access_token_method (
    id bigint DEFAULT nextval('rul_access_token_method_id_seq'::regclass) NOT NULL,
    access_token_id bigint,
    mask character varying(256)
    ,
    CONSTRAINT pk_access_token_method PRIMARY KEY (id)
);

-- ======================================================================

-- Таблица: public.rul_accounting_type
CREATE TABLE public.rul_accounting_type (
    accounting_type_id bigint DEFAULT nextval('rul_accounting_type_accounting_type_id_seq'::regclass) NOT NULL,
    accounting_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_accounting_type_pkey PRIMARY KEY (accounting_type_id)
);
COMMENT ON COLUMN public.rul_accounting_type.accounting_type_name IS 'Способ учета';

-- ======================================================================

-- Таблица: public.rul_accounting_type_node
CREATE TABLE public.rul_accounting_type_node (
    accounting_type_node_id bigint DEFAULT nextval('rul_accounting_type_node_accounting_type_node_id_seq'::regclass) NOT NULL,
    accounting_type_id bigint,
    node_calculate_parameter_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    formula_id bigint
    ,
    CONSTRAINT rul_accounting_type_node_pkey PRIMARY KEY (accounting_type_node_id),
    CONSTRAINT fk_accounting_type_id FOREIGN KEY (accounting_type_id) REFERENCES rul_accounting_type(accounting_type_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id)
);
COMMENT ON COLUMN public.rul_accounting_type_node.accounting_type_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_accounting_type_node.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_accounting_type_node.node_calculate_parameter_id IS 'Ссылка на рассчетный параметр в узле';

-- ======================================================================

-- Таблица: public.rul_agreement
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
COMMENT ON COLUMN public.rul_agreement.agreement_code IS 'Номер договора';
COMMENT ON COLUMN public.rul_agreement.agreement_name IS 'Название договора';
COMMENT ON COLUMN public.rul_agreement.agreement_type_id IS 'Ссылка на тип договора';
COMMENT ON COLUMN public.rul_agreement.billing_period_id IS 'Переодичность расчетов';
COMMENT ON COLUMN public.rul_agreement.code_ay IS 'Код АУ';
COMMENT ON COLUMN public.rul_agreement.customer_client_id IS 'Слылка на плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.customer_description IS 'Описание для плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.customer_document_info IS 'Описание документа';
COMMENT ON COLUMN public.rul_agreement.customer_document_name IS 'Действует на основании(Абонент)';
COMMENT ON COLUMN public.rul_agreement.customer_responsible_user_id IS 'Ответственное за рассчеты лицо плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.customer_user_id IS 'Подписант со стороны плательщика(абонент/субабонент)';
COMMENT ON COLUMN public.rul_agreement.description IS 'Описание договора';
COMMENT ON COLUMN public.rul_agreement.end_date IS 'Конец даты действия';
COMMENT ON COLUMN public.rul_agreement.owner_agreement_id IS 'При трехстороннем договоре указывает на двухсторонний с которым связан объект. Ссылка на договор владельца объекта.';
COMMENT ON COLUMN public.rul_agreement.owner_client_id IS 'Ссылка на владельца объекта';
COMMENT ON COLUMN public.rul_agreement.owner_document_info IS 'Описание документа';
COMMENT ON COLUMN public.rul_agreement.owner_document_name IS 'Действует на основании(Субабонент)';
COMMENT ON COLUMN public.rul_agreement.owner_user_id IS 'Подписант со стороны владельца объекта';
COMMENT ON COLUMN public.rul_agreement.parent_agreement_id IS 'Если это доп соглашение, то указывает на родительский договор к которому создано доп соглашение. Сслыка на договор (сквозной айди)';
COMMENT ON COLUMN public.rul_agreement.pay_day_count IS 'Количество дней на оплату';
COMMENT ON COLUMN public.rul_agreement.payment_mechanism_id IS 'Ссылка на механизм оплаты';
COMMENT ON COLUMN public.rul_agreement.penalty IS 'Пеня, % годовых';
COMMENT ON COLUMN public.rul_agreement.penalty_operation_template_id IS 'ССылка на операцию по пене';
COMMENT ON COLUMN public.rul_agreement.second_supplier_user_id IS 'Подписант поставщика №2';
COMMENT ON COLUMN public.rul_agreement.signing_date IS 'Дата подписания';
COMMENT ON COLUMN public.rul_agreement.start_date IS 'Начало даты действия';
COMMENT ON COLUMN public.rul_agreement.supplier_client_id IS 'Сслыка на поставщика по  договору (клиент)';
COMMENT ON COLUMN public.rul_agreement.supplier_description IS 'Описание для поставщика';
COMMENT ON COLUMN public.rul_agreement.supplier_document_info IS 'Описание документа';
COMMENT ON COLUMN public.rul_agreement.supplier_document_name IS 'Действует на основании(Поставщик)';
COMMENT ON COLUMN public.rul_agreement.supplier_responsible_user_id IS 'Ответственное за рассчеты лицо поставщика';
COMMENT ON COLUMN public.rul_agreement.supplier_user_id IS 'Подписант со стороны поставщика';

-- ======================================================================

-- Таблица: public.rul_agreement_service_type
CREATE TABLE public.rul_agreement_service_type (
    agreement_service_type_id bigint DEFAULT nextval('rul_agreement_service_type_agreement_service_type_id_seq'::regclass) NOT NULL,
    agreement_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_agreement_service_type_pkey PRIMARY KEY (agreement_service_type_id),
    CONSTRAINT fk_agreement_id FOREIGN KEY (agreement_id) REFERENCES rul_agreement(agreement_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);
COMMENT ON COLUMN public.rul_agreement_service_type.agreement_id IS 'Ссылка на договор';
COMMENT ON COLUMN public.rul_agreement_service_type.service_type_id IS 'Ссылка на вид услуги(энергоресурс)';

-- ======================================================================

-- Таблица: public.rul_agreement_type
CREATE TABLE public.rul_agreement_type (
    agreement_type_id bigint DEFAULT nextval('rul_agreement_type_agreement_type_id_seq'::regclass) NOT NULL,
    agreement_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_agreement_type_pkey PRIMARY KEY (agreement_type_id)
);
COMMENT ON COLUMN public.rul_agreement_type.agreement_type_name IS 'Вид договора';

-- ======================================================================

-- Таблица: public.rul_argument_class
CREATE TABLE public.rul_argument_class (
    argument_class_id bigint DEFAULT nextval('rul_argument_class_argument_class_id_seq'::regclass) NOT NULL,
    argument_class_name character varying(2048),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_argument_class_pkey PRIMARY KEY (argument_class_id)
);
COMMENT ON COLUMN public.rul_argument_class.argument_class_name IS 'Названия класса аргумента';

-- ======================================================================

-- Таблица: public.rul_argument_formula
CREATE TABLE public.rul_argument_formula (
    argument_formula_id bigint DEFAULT nextval('rul_argument_formula_argument_formula_id_seq'::regclass) NOT NULL,
    argument_formula_name character varying(256),
    argument_formula_code character varying(16),
    formula_id bigint,
    parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    argument_type_id bigint,
    argument_class_id bigint,
    unit_id bigint
    ,
    CONSTRAINT rul_argument_formula_pkey PRIMARY KEY (argument_formula_id),
    CONSTRAINT fk_argument_class_id FOREIGN KEY (argument_class_id) REFERENCES rul_argument_class(argument_class_id),
    CONSTRAINT fk_argument_type_id FOREIGN KEY (argument_type_id) REFERENCES rul_argument_type(argument_type_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_unit_id FOREIGN KEY (unit_id) REFERENCES rul_unit(unit_id)
);
COMMENT ON COLUMN public.rul_argument_formula.argument_class_id IS 'Ссылка на класс аргумента';
COMMENT ON COLUMN public.rul_argument_formula.argument_formula_code IS 'Код аргумента формулы';
COMMENT ON COLUMN public.rul_argument_formula.argument_formula_name IS 'Название аргумента формулы';
COMMENT ON COLUMN public.rul_argument_formula.argument_type_id IS 'Ссылка на тип аргумента';
COMMENT ON COLUMN public.rul_argument_formula.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_argument_formula.parameter_id IS 'Ссылка на Входной параметр(Так понимаю, тот по которому ищутся показания)';
COMMENT ON COLUMN public.rul_argument_formula.unit_id IS 'Ссылка на Единицу измерения';

-- ======================================================================

-- Таблица: public.rul_argument_type
CREATE TABLE public.rul_argument_type (
    argument_type_id bigint DEFAULT nextval('rul_argument_type_argument_type_id_seq'::regclass) NOT NULL,
    argument_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_argument_type_pkey PRIMARY KEY (argument_type_id)
);
COMMENT ON COLUMN public.rul_argument_type.argument_type_name IS 'Название типа аргумента';

-- ======================================================================

-- Таблица: public.rul_attribute_block
CREATE TABLE public.rul_attribute_block (
    attribute_block_id bigint DEFAULT nextval('rul_attribute_block_attribute_block_id_seq'::regclass) NOT NULL,
    attribute_block_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_block_pkey PRIMARY KEY (attribute_block_id)
);
COMMENT ON COLUMN public.rul_attribute_block.attribute_block_name IS 'Название блока атрибута';

-- ======================================================================

-- Таблица: public.rul_attribute_section
CREATE TABLE public.rul_attribute_section (
    attribute_section_id bigint DEFAULT nextval('rul_attribute_section_attribute_section_id_seq'::regclass) NOT NULL,
    attribute_section_name character varying(128),
    attribute_section_code character varying(64),
    attribute_section_type_id bigint,
    attribute_block_id bigint,
    position_string smallint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    position_number smallint,
    unit_id bigint
    ,
    CONSTRAINT rul_attribute_section_pkey PRIMARY KEY (attribute_section_id),
    CONSTRAINT fk_attribute_block_id FOREIGN KEY (attribute_block_id) REFERENCES rul_attribute_block(attribute_block_id),
    CONSTRAINT fk_attribute_section_type_id FOREIGN KEY (attribute_section_type_id) REFERENCES rul_attribute_section_type(attribute_section_type_id),
    CONSTRAINT fk_unit_id FOREIGN KEY (unit_id) REFERENCES rul_unit(unit_id)
);
COMMENT ON COLUMN public.rul_attribute_section.attribute_block_id IS 'Блок на вэб форме';
COMMENT ON COLUMN public.rul_attribute_section.attribute_section_code IS 'Обозначение атрибута участка';
COMMENT ON COLUMN public.rul_attribute_section.attribute_section_name IS 'Название атрибута участка';
COMMENT ON COLUMN public.rul_attribute_section.attribute_section_type_id IS 'Тип атрибута участка';
COMMENT ON COLUMN public.rul_attribute_section.position_number IS 'Номер позиции в строке';
COMMENT ON COLUMN public.rul_attribute_section.position_string IS 'Номер сроки в блоке';
COMMENT ON COLUMN public.rul_attribute_section.unit_id IS 'Ссылка на единицу измерения';

-- ======================================================================

-- Таблица: public.rul_attribute_section_formula
CREATE TABLE public.rul_attribute_section_formula (
    attribute_section_formula_id bigint DEFAULT nextval('rul_attribute_section_formula_attribute_section_formula_id_seq'::regclass) NOT NULL,
    attribute_section_id bigint,
    formula_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_section_formula_pkey PRIMARY KEY (attribute_section_formula_id),
    CONSTRAINT fk_attribute_section_id FOREIGN KEY (attribute_section_id) REFERENCES rul_attribute_section(attribute_section_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id)
);
COMMENT ON COLUMN public.rul_attribute_section_formula.attribute_section_id IS 'Ссылка на атрибут участка';
COMMENT ON COLUMN public.rul_attribute_section_formula.formula_id IS 'Ссылка на формулу';

-- ======================================================================

-- Таблица: public.rul_attribute_section_type
CREATE TABLE public.rul_attribute_section_type (
    attribute_section_type_id bigint DEFAULT nextval('rul_attribute_section_type_attribute_section_type_id_seq'::regclass) NOT NULL,
    attribute_section_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_section_type_pkey PRIMARY KEY (attribute_section_type_id)
);
COMMENT ON COLUMN public.rul_attribute_section_type.attribute_section_type_name IS 'Название типа атрибута';

-- ======================================================================

-- Таблица: public.rul_attribute_section_value
CREATE TABLE public.rul_attribute_section_value (
    attribute_section_value_id bigint DEFAULT nextval('rul_attribute_section_value_attribute_section_value_id_seq'::regclass) NOT NULL,
    section_id bigint,
    attribute_section_id bigint,
    value numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_section_value_pkey PRIMARY KEY (attribute_section_value_id),
    CONSTRAINT fk_section_id FOREIGN KEY (section_id) REFERENCES rul_section(section_id)
);
COMMENT ON COLUMN public.rul_attribute_section_value.attribute_section_id IS 'Ссылка на атрибут участка';
COMMENT ON COLUMN public.rul_attribute_section_value.section_id IS 'Ссылка на участок';
COMMENT ON COLUMN public.rul_attribute_section_value.value IS 'Значение атрибута';

-- ======================================================================

-- Таблица: public.rul_average_value
CREATE TABLE public.rul_average_value (
    average_value_id bigint DEFAULT nextval('rul_average_value_average_value_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    indication_start_date timestamp without time zone,
    indication_end_date timestamp without time zone
    ,
    CONSTRAINT rul_average_value_pkey PRIMARY KEY (average_value_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id)
);
COMMENT ON COLUMN public.rul_average_value.accounting_type_node_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_average_value.end_date IS 'Дата по (которой заканчивается расчет кэфа по среднему)';
COMMENT ON COLUMN public.rul_average_value.indication_end_date IS 'Дата, которая преобразована методом расчета по среднему';
COMMENT ON COLUMN public.rul_average_value.indication_start_date IS 'Дата, которая преобразована методом расчета по среднему';
COMMENT ON COLUMN public.rul_average_value.start_date IS 'Дата с (с которой начинает расчитываться кэф по среднему)';

-- ======================================================================

-- Таблица: public.rul_average_value_argument
CREATE TABLE public.rul_average_value_argument (
    average_value_argument_id bigint DEFAULT nextval('rul_average_value_argument_average_value_argument_id_seq'::regclass) NOT NULL,
    argument_formula_id bigint,
    value numeric,
    average_value_id bigint,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_average_value_argument_pkey PRIMARY KEY (average_value_argument_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_average_value_id FOREIGN KEY (average_value_id) REFERENCES rul_average_value(average_value_id),
    CONSTRAINT fk_op_user_id FOREIGN KEY (op_user_id) REFERENCES rul_user(user_id)
);
COMMENT ON COLUMN public.rul_average_value_argument.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_average_value_argument.average_value_id IS 'Ссылка на расход по среднему';
COMMENT ON COLUMN public.rul_average_value_argument.value IS 'Значение аргумента';

-- ======================================================================

-- Таблица: public.rul_balancing
CREATE TABLE public.rul_balancing (
    balancing_id bigint DEFAULT nextval('rul_balancing_balancing_id_seq'::regclass) NOT NULL,
    node_calculate_parameter_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    balancing_coefficient numeric,
    balancing_indication numeric,
    sum_losses_balance numeric,
    realize_consumption numeric,
    sum_losses_unbilled numeric
    ,
    CONSTRAINT rul_balancing_pkey PRIMARY KEY (balancing_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id)
);
COMMENT ON COLUMN public.rul_balancing.balancing_coefficient IS 'Коэффициент небаланса';
COMMENT ON COLUMN public.rul_balancing.balancing_indication IS 'Отпущено с узла';
COMMENT ON COLUMN public.rul_balancing.end_date IS 'Дата завершения периода';
COMMENT ON COLUMN public.rul_balancing.node_calculate_parameter_id IS 'Ссылка на расчетный параметр';
COMMENT ON COLUMN public.rul_balancing.realize_consumption IS 'Реализовано';
COMMENT ON COLUMN public.rul_balancing.start_date IS 'Дата начала периода';
COMMENT ON COLUMN public.rul_balancing.sum_losses_balance IS 'Потери поставщика';
COMMENT ON COLUMN public.rul_balancing.sum_losses_unbilled IS 'Сумма списываемых потерь';

-- ======================================================================

-- Таблица: public.rul_bank_account
CREATE TABLE public.rul_bank_account (
    bank_account_id bigint DEFAULT nextval('rul_bank_account_bank_account_id_seq'::regclass) NOT NULL,
    bank_account_code character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    is_main numeric(1,0) DEFAULT 1 NOT NULL,
    bank_name character varying(256),
    bank_address character varying(256),
    unp character varying(256),
    okpo character varying(256),
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    bank_code character varying(256)
    ,
    CONSTRAINT rul_bank_account_pkey PRIMARY KEY (bank_account_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);
COMMENT ON COLUMN public.rul_bank_account.bank_account_code IS 'Рассчетный счет';
COMMENT ON COLUMN public.rul_bank_account.bank_address IS 'Адресс банка';
COMMENT ON COLUMN public.rul_bank_account.bank_code IS 'Код банка';
COMMENT ON COLUMN public.rul_bank_account.bank_name IS 'Наименование банка';
COMMENT ON COLUMN public.rul_bank_account.client_id IS 'Ссылка на клиента(контрагента)';
COMMENT ON COLUMN public.rul_bank_account.end_date IS 'Дата завершения действия счета';
COMMENT ON COLUMN public.rul_bank_account.is_main IS 'Флаг основной счет или дополнительный';
COMMENT ON COLUMN public.rul_bank_account.okpo IS 'ОКПО';
COMMENT ON COLUMN public.rul_bank_account.start_date IS 'Дата начала действия счета';
COMMENT ON COLUMN public.rul_bank_account.unp IS 'УНП';

-- ======================================================================

-- Таблица: public.rul_billing_period
CREATE TABLE public.rul_billing_period (
    billing_period_id bigint DEFAULT nextval('rul_billing_period_billing_period_id_seq'::regclass) NOT NULL,
    billing_period_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_billing_period_pkey PRIMARY KEY (billing_period_id)
);

-- ======================================================================

-- Таблица: public.rul_brand
CREATE TABLE public.rul_brand (
    brand_id bigint DEFAULT nextval('rul_brand_brand_id_seq'::regclass) NOT NULL,
    brand_name character varying(1024),
    brand_holder character varying(256),
    interval bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    diameter character varying(128)
    ,
    CONSTRAINT rul_brand_pkey PRIMARY KEY (brand_id)
);
COMMENT ON COLUMN public.rul_brand.brand_holder IS 'Владелец/производитель марки';
COMMENT ON COLUMN public.rul_brand.brand_name IS 'Название марки производителя';
COMMENT ON COLUMN public.rul_brand.diameter IS 'Диаметер';
COMMENT ON COLUMN public.rul_brand.interval IS 'Межповерочный интервал';

-- ======================================================================

-- Таблица: public.rul_brand_parameter
CREATE TABLE public.rul_brand_parameter (
    brand_parameter_id bigint DEFAULT nextval('rul_brand_parameter_brand_parameter_id_seq'::regclass) NOT NULL,
    brand_id bigint,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    parameter_id bigint
    ,
    CONSTRAINT rul_kind_parameter_pkey PRIMARY KEY (brand_parameter_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_parameter_fk FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);
COMMENT ON COLUMN public.rul_brand_parameter.brand_id IS 'Ссылка на марку производителя';
COMMENT ON COLUMN public.rul_brand_parameter.description IS 'Описание';
COMMENT ON COLUMN public.rul_brand_parameter.parameter_id IS 'Ссылка на параметр';

-- ======================================================================

-- Таблица: public.rul_brand_service_type
CREATE TABLE public.rul_brand_service_type (
    brand_service_type_id bigint DEFAULT nextval('rul_brand_service_type_brand_service_type_id_seq'::regclass) NOT NULL,
    brand_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_brand_service_type_pkey PRIMARY KEY (brand_service_type_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);
COMMENT ON COLUMN public.rul_brand_service_type.brand_id IS 'Сслыка с маркой производителя';
COMMENT ON COLUMN public.rul_brand_service_type.service_type_id IS 'Сслыка на тип услуги(энергоресурс)';

-- ======================================================================

-- Таблица: public.rul_charge
CREATE TABLE public.rul_charge (
    charge_id bigint DEFAULT nextval('rul_charge_charge_id_seq'::regclass) NOT NULL,
    connection_id bigint,
    sum_consumption numeric,
    base_value numeric,
    amount numeric,
    nds_percent numeric,
    note character varying(2048),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    billing_start_date timestamp without time zone,
    billing_end_date timestamp without time zone,
    charge_type_id bigint,
    amount_nds numeric,
    nds_rub numeric,
    charge_checked numeric(1,0) DEFAULT 0 NOT NULL,
    need_recount numeric(1,0) DEFAULT 0 NOT NULL,
    invoice_id bigint,
    source_id bigint DEFAULT 1,
    currency_rate numeric,
    cost_factor numeric,
    invoice_group_index bigint DEFAULT 1,
    balancing_id bigint,
    comitet_resolution character varying(256) DEFAULT '-'::character varying,
    balancing_coefficient numeric
    ,
    CONSTRAINT rul_charge_pkey PRIMARY KEY (charge_id),
    CONSTRAINT fk_balancing_id FOREIGN KEY (balancing_id) REFERENCES rul_balancing(balancing_id),
    CONSTRAINT fk_charge_type FOREIGN KEY (charge_type_id) REFERENCES rul_charge_type(charge_type_id),
    CONSTRAINT fk_connection_id FOREIGN KEY (connection_id) REFERENCES rul_connection(connection_id),
    CONSTRAINT fk_source_id FOREIGN KEY (source_id) REFERENCES rul_source(source_id)
);
COMMENT ON COLUMN public.rul_charge.amount IS 'Итоговая стоимость (без ндс)';
COMMENT ON COLUMN public.rul_charge.amount_nds IS 'Сумма с НДС';
COMMENT ON COLUMN public.rul_charge.balancing_id IS 'Ссылка на то, какой балансировкой исправлено';
COMMENT ON COLUMN public.rul_charge.base_value IS 'Сам тариф';
COMMENT ON COLUMN public.rul_charge.billing_end_date IS 'Дата завершения рассчетного периода';
COMMENT ON COLUMN public.rul_charge.billing_start_date IS 'Начало рассчетного периода';
COMMENT ON COLUMN public.rul_charge.charge_checked IS 'Флаг подтверждения начисления';
COMMENT ON COLUMN public.rul_charge.charge_type_id IS 'Ссылка на тип начисления';
COMMENT ON COLUMN public.rul_charge.comitet_resolution IS 'Решение исполкома взятое из тарифа';
COMMENT ON COLUMN public.rul_charge.connection_id IS 'Ссылка на подключение';
COMMENT ON COLUMN public.rul_charge.cost_factor IS 'Удельный вес затрат';
COMMENT ON COLUMN public.rul_charge.currency_rate IS 'Базовый курс (курс валюты)';
COMMENT ON COLUMN public.rul_charge.end_date IS 'Дата по (расход)';
COMMENT ON COLUMN public.rul_charge.invoice_group_index IS 'ИГС (индекс группировки счетов)';
COMMENT ON COLUMN public.rul_charge.invoice_id IS 'Ссылка на счет, если он создан.';
COMMENT ON COLUMN public.rul_charge.nds_percent IS 'НДС, проценты';
COMMENT ON COLUMN public.rul_charge.nds_rub IS 'НДС, рубли';
COMMENT ON COLUMN public.rul_charge.need_recount IS 'Требует пересчета';
COMMENT ON COLUMN public.rul_charge.note IS 'Примечание';
COMMENT ON COLUMN public.rul_charge.source_id IS 'Источник того, откуда пришел расход по начислению';
COMMENT ON COLUMN public.rul_charge.start_date IS 'Дата с (расхода)';
COMMENT ON COLUMN public.rul_charge.sum_consumption IS 'Итоговый расход';

-- ======================================================================

-- Таблица: public.rul_charge_detail
CREATE TABLE public.rul_charge_detail (
    charge_detail_id bigint DEFAULT nextval('rul_charge_detail_charge_detail_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    note character varying(2048),
    charge_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    rate_value_id bigint,
    connection_id bigint,
    note2 character varying(2048)
    ,
    CONSTRAINT rul_charge_detail_pkey PRIMARY KEY (charge_detail_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id)
);
COMMENT ON COLUMN public.rul_charge_detail.accounting_type_node_id IS 'Ссылка на параметр способ учета в узле';
COMMENT ON COLUMN public.rul_charge_detail.charge_id IS 'Ссылка на начисление';
COMMENT ON COLUMN public.rul_charge_detail.consumption IS 'Сам расход';
COMMENT ON COLUMN public.rul_charge_detail.end_date IS 'Дата по (расход)';
COMMENT ON COLUMN public.rul_charge_detail.note IS 'Примечание';
COMMENT ON COLUMN public.rul_charge_detail.rate_value_id IS 'Ссылка на значение тарифа';
COMMENT ON COLUMN public.rul_charge_detail.start_date IS 'Дата с (расхода)';

-- ======================================================================

-- Таблица: public.rul_charge_type
CREATE TABLE public.rul_charge_type (
    charge_type_id bigint DEFAULT nextval('rul_charge_type_charge_type_id_seq'::regclass) NOT NULL,
    charge_type_name character varying(2048),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_charge_type_pkey PRIMARY KEY (charge_type_id)
);
COMMENT ON COLUMN public.rul_charge_type.charge_type_name IS 'Названия типа начисления';

-- ======================================================================

-- Таблица: public.rul_check_type
CREATE TABLE public.rul_check_type (
    check_type_id bigint DEFAULT nextval('rul_check_type_check_type_id_seq'::regclass) NOT NULL,
    check_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    choise smallint DEFAULT 0
    ,
    CONSTRAINT rul_check_type_pkey PRIMARY KEY (check_type_id)
);
COMMENT ON COLUMN public.rul_check_type.check_type_name IS 'Название способа снятия показания';
COMMENT ON COLUMN public.rul_check_type.choise IS '0 - нет, 1 - да';

-- ======================================================================

-- Таблица: public.rul_classifier
CREATE TABLE public.rul_classifier (
    classifier_id bigint DEFAULT nextval('rul_classifier_classifier_id_seq'::regclass) NOT NULL,
    classifier_name character varying(128),
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    classifier_type_id bigint DEFAULT 1 NOT NULL
    ,
    CONSTRAINT rul_classifier_pkey PRIMARY KEY (classifier_id),
    CONSTRAINT fk_classifier_type FOREIGN KEY (classifier_type_id) REFERENCES rul_classifier_type(classifier_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);
COMMENT ON COLUMN public.rul_classifier.classifier_name IS 'Название классификатора';
COMMENT ON COLUMN public.rul_classifier.classifier_type_id IS 'Ссылка на тип классификатора. (На данный момент будет использоваться для агрегации данных в отчетах)';
COMMENT ON COLUMN public.rul_classifier.client_id IS 'Ссылка на поставщика';

-- ======================================================================

-- Таблица: public.rul_classifier_network_fragment
CREATE TABLE public.rul_classifier_network_fragment (
    classifier_network_fragment_id bigint DEFAULT nextval('rul_classifier_network_fragme_classifier_network_fragment_i_seq'::regclass) NOT NULL,
    classifier_id bigint,
    network_fragment_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_classifier_network_fragment_pkey PRIMARY KEY (classifier_network_fragment_id),
    CONSTRAINT fk_classifier_id FOREIGN KEY (classifier_id) REFERENCES rul_classifier(classifier_id),
    CONSTRAINT fk_network_fragment_id FOREIGN KEY (network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id)
);
COMMENT ON COLUMN public.rul_classifier_network_fragment.classifier_id IS 'Ссылка на классификатор';
COMMENT ON COLUMN public.rul_classifier_network_fragment.network_fragment_id IS 'Ссылка на фрагмент сети';

-- ======================================================================

-- Таблица: public.rul_classifier_type
CREATE TABLE public.rul_classifier_type (
    classifier_type_id bigint DEFAULT nextval('rul_classifier_type_classifier_type_id_seq'::regclass) NOT NULL,
    classifier_type_name character varying(256),
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_classifier_type_pkey PRIMARY KEY (classifier_type_id)
);
COMMENT ON COLUMN public.rul_classifier_type.classifier_type_name IS 'Тип классификатора';

-- ======================================================================

-- Таблица: public.rul_client
CREATE TABLE public.rul_client (
    client_id bigint DEFAULT nextval('rul_client_client_id_seq'::regclass) NOT NULL,
    client_name character varying(2048),
    email character varying(256),
    description character varying(8000),
    is_active numeric(1,0) DEFAULT 1 NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    locality_id bigint,
    district_id bigint,
    region_id bigint,
    building character varying(128),
    housing character varying(128),
    room character varying(128),
    post_index character varying(128),
    street_id bigint,
    filial character varying(2048),
    payment_order_index bigint DEFAULT 0 NOT NULL,
    client_group_id bigint
    ,
    CONSTRAINT rul_client_pkey PRIMARY KEY (client_id),
    CONSTRAINT fk_client_group_id FOREIGN KEY (client_group_id) REFERENCES rul_client_group(client_group_id),
    CONSTRAINT fk_postal_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_postal_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_postal_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id),
    CONSTRAINT fk_postal_street_id FOREIGN KEY (street_id) REFERENCES rul_street(street_id)
);

COMMENT ON TABLE public.rul_client IS 'Таблица контрагентов с почтовым адресом';

COMMENT ON COLUMN public.rul_client.building IS 'Почт.адресс: Номер здания';
COMMENT ON COLUMN public.rul_client.client_group_id IS 'Ссылка на группу контрагентов (клиентов)';
COMMENT ON COLUMN public.rul_client.client_name IS 'Название клиента (контрагента)';
COMMENT ON COLUMN public.rul_client.description IS 'Описание клиента';
COMMENT ON COLUMN public.rul_client.district_id IS 'Почт.адресс: Ссылка на район(справочник)';
COMMENT ON COLUMN public.rul_client.email IS 'Адрес электронной почты';
COMMENT ON COLUMN public.rul_client.filial IS 'Филиал';
COMMENT ON COLUMN public.rul_client.housing IS 'Почт.адресс: Номер корпуса';
COMMENT ON COLUMN public.rul_client.locality_id IS 'Почт.адресс: Ссылка на населенный пункт(справочник)';
COMMENT ON COLUMN public.rul_client.payment_order_index IS 'Индекс для платежных требований (Типо сиквенса, указывает на следующий номер для платежного требования в рамках одного клиента)';
COMMENT ON COLUMN public.rul_client.post_index IS 'Почт.адресс: Почтовый индекс';
COMMENT ON COLUMN public.rul_client.region_id IS 'Почт.адресс: Ссылка на область(справочник)';
COMMENT ON COLUMN public.rul_client.room IS 'Почт.адресс: Номер помещения';
COMMENT ON COLUMN public.rul_client.street_id IS 'Почт.адресс: Ссылка на улицу(справочник)';

-- ======================================================================

-- Таблица: public.rul_client_client_type
CREATE TABLE public.rul_client_client_type (
    client_client_type_id bigint DEFAULT nextval('rul_client_client_type_client_client_type_id_seq'::regclass) NOT NULL,
    client_type_id bigint,
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_client_type_pkey PRIMARY KEY (client_client_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);

COMMENT ON TABLE public.rul_client_client_type IS 'Таблица связка, которая описывает какого типа конрагент';

COMMENT ON COLUMN public.rul_client_client_type.client_id IS 'Ссылка на контрагента';
COMMENT ON COLUMN public.rul_client_client_type.client_type_id IS 'Сслыка на Тип контрагента';

-- ======================================================================

-- Таблица: public.rul_client_control
CREATE TABLE public.rul_client_control (
    client_control_id bigint DEFAULT nextval('rul_client_control_client_control_id_seq'::regclass) NOT NULL,
    curator_client_id bigint NOT NULL,
    dependent_client_id bigint NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_control_pkey PRIMARY KEY (client_control_id),
    CONSTRAINT chk_self_reference CHECK ((curator_client_id <> dependent_client_id)),
    CONSTRAINT uq_curator_dependent UNIQUE (curator_client_id, dependent_client_id),
    CONSTRAINT fk_curator FOREIGN KEY (curator_client_id) REFERENCES rul_client(client_id) ON DELETE CASCADE,
    CONSTRAINT fk_dependent FOREIGN KEY (dependent_client_id) REFERENCES rul_client(client_id) ON DELETE CASCADE
);

-- ======================================================================

-- Таблица: public.rul_client_group
CREATE TABLE public.rul_client_group (
    client_group_id bigint DEFAULT nextval('rul_client_group_client_group_id_seq'::regclass) NOT NULL,
    client_group_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    bank_subject_status character varying(16)
    ,
    CONSTRAINT rul_client_group_pkey PRIMARY KEY (client_group_id)
);
COMMENT ON COLUMN public.rul_client_group.client_group_id IS 'Айди группы клиентов';
COMMENT ON COLUMN public.rul_client_group.client_group_name IS 'Название группы клиентов';
COMMENT ON COLUMN public.rul_client_group.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_client_group.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';

-- ======================================================================

-- Таблица: public.rul_client_object
CREATE TABLE public.rul_client_object (
    client_object_id bigint DEFAULT nextval('rul_client_object_client_object_id_seq'::regclass) NOT NULL,
    object_id bigint,
    client_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    room character varying(128)
    ,
    CONSTRAINT rul_client_object_pkey PRIMARY KEY (client_object_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES rul_object(object_id)
);
COMMENT ON COLUMN public.rul_client_object.client_id IS 'Сслыка на клиента';
COMMENT ON COLUMN public.rul_client_object.end_date IS 'Конец даты действия';
COMMENT ON COLUMN public.rul_client_object.object_id IS 'Ссылка на объекта';
COMMENT ON COLUMN public.rul_client_object.room IS 'Арендуемое помещение';
COMMENT ON COLUMN public.rul_client_object.start_date IS 'Начало даты действия';

-- ======================================================================

-- Таблица: public.rul_client_service_type
CREATE TABLE public.rul_client_service_type (
    client_service_type_id bigint DEFAULT nextval('rul_client_service_type_client_service_type_id_seq'::regclass) NOT NULL,
    service_type_id bigint,
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_service_type_pkey PRIMARY KEY (client_service_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON TABLE public.rul_client_service_type IS 'Таблица связка которая указывает какие виды услуг выбраны для конкретного контрагента';

COMMENT ON COLUMN public.rul_client_service_type.client_id IS 'Ссылка на контрагента';
COMMENT ON COLUMN public.rul_client_service_type.service_type_id IS 'Сслыка на Вид услуги';

-- ======================================================================

-- Таблица: public.rul_client_type
CREATE TABLE public.rul_client_type (
    client_type_id bigint DEFAULT nextval('rul_client_type_client_type_id_seq'::regclass) NOT NULL,
    client_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_type_pkey PRIMARY KEY (client_type_id)
);

COMMENT ON TABLE public.rul_client_type IS 'Таблица справочник для типов контрагентов';

COMMENT ON COLUMN public.rul_client_type.client_type_name IS 'Тип контрагента';

-- ======================================================================

-- Таблица: public.rul_client_type_service_type
CREATE TABLE public.rul_client_type_service_type (
    client_type_service_type_id bigint DEFAULT nextval('rul_client_type_service_type_client_type_service_type_id_seq'::regclass) NOT NULL,
    service_type_id bigint,
    client_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_type_service_type_pkey PRIMARY KEY (client_type_service_type_id),
    CONSTRAINT fk_client_type_id FOREIGN KEY (client_type_id) REFERENCES rul_client_type(client_type_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON TABLE public.rul_client_type_service_type IS 'Таблица, которая показывает какие виды улгуг могут быть доступны определенным типам контрагентов';

COMMENT ON COLUMN public.rul_client_type_service_type.client_type_id IS 'Ссылка на Тип контрагента';
COMMENT ON COLUMN public.rul_client_type_service_type.service_type_id IS 'Сслыка на Вид услуги';

-- ======================================================================

-- Таблица: public.rul_client_type_user_type
CREATE TABLE public.rul_client_type_user_type (
    client_type_user_type_id bigint DEFAULT nextval('rul_client_type_user_type_client_type_user_type_id_seq'::regclass) NOT NULL,
    client_type_id bigint,
    user_type_id bigint,
    position smallint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_type_user_type_pkey PRIMARY KEY (client_type_user_type_id),
    CONSTRAINT fk_functional_access_id FOREIGN KEY (client_type_id) REFERENCES rul_functional_access(functional_access_id),
    CONSTRAINT fk_user_type_id FOREIGN KEY (user_type_id) REFERENCES rul_user_type(user_type_id)
);

COMMENT ON TABLE public.rul_client_type_user_type IS 'Маппинг для типов клиентов и типов юзеров';

COMMENT ON COLUMN public.rul_client_type_user_type.client_type_id IS 'Ссылка на тип клиента';
COMMENT ON COLUMN public.rul_client_type_user_type.deleted IS 'Удален или нет';
COMMENT ON COLUMN public.rul_client_type_user_type.op_date IS 'Дата совершения действия';
COMMENT ON COLUMN public.rul_client_type_user_type.op_user_id IS 'Ссылка на таблицу пользователи';
COMMENT ON COLUMN public.rul_client_type_user_type.position IS 'Позиция для фронта';
COMMENT ON COLUMN public.rul_client_type_user_type.user_type_id IS 'Ссылка на тип доступа юзера';

-- ======================================================================

-- Таблица: public.rul_comitet
CREATE TABLE public.rul_comitet (
    comitet_id bigint DEFAULT nextval('rul_comitet_comitet_id_seq'::regclass) NOT NULL,
    comitet_name character varying(256),
    region_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_comitet_pkey PRIMARY KEY (comitet_id),
    CONSTRAINT fk_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id)
);
COMMENT ON COLUMN public.rul_comitet.comitet_name IS 'Название Облисполкома';
COMMENT ON COLUMN public.rul_comitet.region_id IS 'Ссылка на область';

-- ======================================================================

-- Таблица: public.rul_connection
CREATE TABLE public.rul_connection (
    connection_id bigint DEFAULT nextval('rul_connection_connection_id_seq'::regclass) NOT NULL,
    connection_name character varying(256),
    agreement_id bigint,
    percent_consumption numeric,
    node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    service_type_id bigint,
    service_operation_template_id bigint,
    rate_id bigint,
    node_calculate_parameter_id bigint,
    canalized_part numeric,
    client_object_id bigint,
    unaccounted_source_consumption_id bigint,
    allocation_source_consumption_id bigint,
    resource_balance_attitude_id bigint,
    group_recalculation_attitude_id bigint,
    invoice_group_index bigint DEFAULT 1,
    percent_losses numeric,
    indexing_operation_template_id bigint,
    advance_operation_template_id bigint,
    losses_policy_id bigint,
    purpose_consumption_id bigint,
    losses_rate_id bigint
    ,
    CONSTRAINT rul_connection_pkey PRIMARY KEY (connection_id),
    CONSTRAINT fk_advance_operation_template_id FOREIGN KEY (advance_operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_agreement_id FOREIGN KEY (agreement_id) REFERENCES rul_agreement(agreement_id),
    CONSTRAINT fk_allocation_source_consumption_id FOREIGN KEY (allocation_source_consumption_id) REFERENCES rul_source_consumption(source_consumption_id),
    CONSTRAINT fk_group_recalculation_attitude_id FOREIGN KEY (group_recalculation_attitude_id) REFERENCES rul_group_recalculation_attitude(group_recalculation_attitude_id),
    CONSTRAINT fk_indexing_operation_template_id FOREIGN KEY (indexing_operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_losses_policy FOREIGN KEY (losses_policy_id) REFERENCES rul_losses_policy(losses_policy_id),
    CONSTRAINT fk_losses_rate_id FOREIGN KEY (losses_rate_id) REFERENCES rul_rate(rate_id),
    CONSTRAINT fk_node_calculate_paramater_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id),
    CONSTRAINT fk_owner_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id),
    CONSTRAINT fk_purpose_consumption_id FOREIGN KEY (purpose_consumption_id) REFERENCES rul_purpose_consumption(purpose_consumption_id),
    CONSTRAINT fk_rate_id FOREIGN KEY (rate_id) REFERENCES rul_rate(rate_id),
    CONSTRAINT fk_resource_balance_attitude_id FOREIGN KEY (resource_balance_attitude_id) REFERENCES rul_resource_balance_attitude(resource_balance_attitude_id),
    CONSTRAINT fk_service_operation_template_id FOREIGN KEY (service_operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id),
    CONSTRAINT fk_target_consumer_client_object_id FOREIGN KEY (client_object_id) REFERENCES rul_client_object(client_object_id),
    CONSTRAINT fk_unaccounted_source_consumption_id FOREIGN KEY (unaccounted_source_consumption_id) REFERENCES rul_source_consumption(source_consumption_id)
);
COMMENT ON COLUMN public.rul_connection.advance_operation_template_id IS 'Ссылка на шаблон операции по предоплате';
COMMENT ON COLUMN public.rul_connection.agreement_id IS 'Ссылка на договор';
COMMENT ON COLUMN public.rul_connection.allocation_source_consumption_id IS 'Ссылка на источник расходов при распределении расходов ГПУ';
COMMENT ON COLUMN public.rul_connection.canalized_part IS 'Процент канализации';
COMMENT ON COLUMN public.rul_connection.client_object_id IS 'Ссылка на Котрагента размещенного на объекте (Не имеет отношения к договору, т.к. может быть отличным от него)';
COMMENT ON COLUMN public.rul_connection.connection_name IS 'Название для начислений';
COMMENT ON COLUMN public.rul_connection.end_date IS 'Дата отключения узла';
COMMENT ON COLUMN public.rul_connection.group_recalculation_attitude_id IS 'Ссылка на справочник подлежит ли пересчету по ГПУ';
COMMENT ON COLUMN public.rul_connection.indexing_operation_template_id IS 'Ссылка на шаблон операции по индексации';
COMMENT ON COLUMN public.rul_connection.invoice_group_index IS 'ИГС (Участвует в формировании счетов)';
COMMENT ON COLUMN public.rul_connection.losses_policy_id IS 'Политика по потерям';
COMMENT ON COLUMN public.rul_connection.losses_rate_id IS 'Ссылка на тарифную группу для потерь';
COMMENT ON COLUMN public.rul_connection.node_calculate_parameter_id IS 'Ссылка на Рассчетный параметр узла';
COMMENT ON COLUMN public.rul_connection.node_id IS 'Ссылка на узел потребителя';
COMMENT ON COLUMN public.rul_connection.percent_consumption IS 'Процент расхода';
COMMENT ON COLUMN public.rul_connection.percent_losses IS 'Процент потерь (только для воды кроде как)';
COMMENT ON COLUMN public.rul_connection.purpose_consumption_id IS 'Ссылка на Административное назначение потребления';
COMMENT ON COLUMN public.rul_connection.rate_id IS 'Ссылка на тарифную группу';
COMMENT ON COLUMN public.rul_connection.resource_balance_attitude_id IS 'Ссылка на справочник подлежит ли балансировке';
COMMENT ON COLUMN public.rul_connection.service_operation_template_id IS 'Ссылка на шаблон операций по услуге';
COMMENT ON COLUMN public.rul_connection.service_type_id IS 'Ссылка на Вид услуги';
COMMENT ON COLUMN public.rul_connection.start_date IS 'Дата подключения узла';
COMMENT ON COLUMN public.rul_connection.unaccounted_source_consumption_id IS 'Ссылка на источник расходов при безучетном потреблении';

-- ======================================================================

-- Таблица: public.rul_connection_connection
CREATE TABLE public.rul_connection_connection (
    connection_connection_id bigint DEFAULT nextval('rul_connection_connection_connection_connection_id_seq'::regclass) NOT NULL,
    destination_connection_id bigint,
    source_connection_id bigint,
    formula_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_connection_connection_pkey PRIMARY KEY (connection_connection_id),
    CONSTRAINT fk_destination_connection_id FOREIGN KEY (destination_connection_id) REFERENCES rul_connection(connection_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_source_connection_id FOREIGN KEY (source_connection_id) REFERENCES rul_connection(connection_id)
);
COMMENT ON COLUMN public.rul_connection_connection.destination_connection_id IS 'То подключение, к которому заведен расчет по внешнему узлу';
COMMENT ON COLUMN public.rul_connection_connection.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_connection_connection.source_connection_id IS 'То подключение, из которого будут выбираться расходы';

-- ======================================================================

-- Таблица: public.rul_constant_value
CREATE TABLE public.rul_constant_value (
    constant_value_id bigint DEFAULT nextval('rul_constant_value_constant_value_id_seq'::regclass) NOT NULL,
    argument_formula_id bigint,
    version_constant_id bigint,
    value numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_constant_value_pkey PRIMARY KEY (constant_value_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_version_constant_id FOREIGN KEY (version_constant_id) REFERENCES rul_version_constant(version_constant_id)
);
COMMENT ON COLUMN public.rul_constant_value.argument_formula_id IS 'Ссылка аргумент формулы';
COMMENT ON COLUMN public.rul_constant_value.value IS 'Значение';
COMMENT ON COLUMN public.rul_constant_value.version_constant_id IS 'Ссылка на версию константы';

-- ======================================================================

-- Таблица: public.rul_construction
CREATE TABLE public.rul_construction (
    construction_id bigint DEFAULT nextval('rul_construction_construction_id_seq'::regclass) NOT NULL,
    construction_name character varying(128),
    parameter_target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_construction_pkey PRIMARY KEY (construction_id),
    CONSTRAINT fk_parameter_target_use_id FOREIGN KEY (parameter_target_use_id) REFERENCES rul_parameter_target_use(parameter_target_use_id)
);
COMMENT ON COLUMN public.rul_construction.construction_name IS 'Название конструкции';
COMMENT ON COLUMN public.rul_construction.parameter_target_use_id IS 'Ссылка на назначение';

-- ======================================================================

-- Таблица: public.rul_consumption
CREATE TABLE public.rul_consumption (
    consumption_id bigint DEFAULT nextval('rul_consumption_consumption_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    value numeric,
    is_unbalanced numeric(1,0) DEFAULT 0 NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    node_panel_id bigint,
    node_panel_argument_id bigint,
    avg_day_value numeric,
    connection_id bigint,
    note character varying(256)
    ,
    CONSTRAINT rul_consumption_pkey PRIMARY KEY (consumption_id)
);
COMMENT ON COLUMN public.rul_consumption.accounting_type_node_id IS 'Ссылка на параметр спочобо учета в узле';
COMMENT ON COLUMN public.rul_consumption.avg_day_value IS 'Средний расход за день';
COMMENT ON COLUMN public.rul_consumption.connection_id IS '28.11. Устаревшее, сейчес не использую. Ссылка на подключение';
COMMENT ON COLUMN public.rul_consumption.end_date IS 'Дата по (расход)';
COMMENT ON COLUMN public.rul_consumption.node_panel_id IS 'Ссылка на параметр измеряемый узлом';
COMMENT ON COLUMN public.rul_consumption.note IS 'Обоснование';
COMMENT ON COLUMN public.rul_consumption.start_date IS 'Дата с (расхода)';
COMMENT ON COLUMN public.rul_consumption.value IS 'Сам расход';

-- ======================================================================

-- Таблица: public.rul_consumption_all
CREATE TABLE public.rul_consumption_all (
    node_panel_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    value_number numeric,
    diff numeric
);

-- ======================================================================

-- Таблица: public.rul_consumption_average
CREATE TABLE public.rul_consumption_average (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    value numeric,
    accounting_type_node_id bigint,
    node_calculate_parameter_id bigint
);

-- ======================================================================

-- Таблица: public.rul_consumption_day
CREATE TABLE public.rul_consumption_day (
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    node_panel_id bigint,
    node_panel_argument_id bigint
);

-- ======================================================================

-- Таблица: public.rul_consumption_detail
CREATE TABLE public.rul_consumption_detail (
    node_panel_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    value_number numeric
);

-- ======================================================================

-- Таблица: public.rul_consumption_load
CREATE TABLE public.rul_consumption_load (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    formula_connection_id bigint,
    version_load_standard_id bigint,
    value numeric,
    accounting_type_node_id bigint,
    coefficient numeric DEFAULT 1,
    theoretical_calculation boolean DEFAULT true,
    balancing_coefficient numeric,
    description character varying(2048),
    balancing_id bigint,
    note character varying(256)
);
COMMENT ON COLUMN public.rul_consumption_load.balancing_coefficient IS 'Коэфициент балансировки';
COMMENT ON COLUMN public.rul_consumption_load.coefficient IS 'Коэффициент распределения';
COMMENT ON COLUMN public.rul_consumption_load.note IS 'Обоснование';
COMMENT ON COLUMN public.rul_consumption_load.theoretical_calculation IS 'Флаг указывающий на то, брать ли в начисления расход';

-- ======================================================================

-- Таблица: public.rul_consumption_losses
CREATE TABLE public.rul_consumption_losses (
    line_id bigint,
    section_id bigint,
    v_p numeric,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    p numeric,
    g numeric,
    value numeric,
    connection_id bigint,
    theoretical_calculation boolean DEFAULT true,
    path bigint[],
    accounting_type_node_id bigint,
    coefficient numeric,
    balancing_coefficient numeric,
    balancing_id bigint,
    note character varying(256),
    is_balancing_losses numeric
);
COMMENT ON COLUMN public.rul_consumption_losses.balancing_coefficient IS 'Коэффициент Балансировки';
COMMENT ON COLUMN public.rul_consumption_losses.coefficient IS 'Коэффициент расчета ГПУ';
COMMENT ON COLUMN public.rul_consumption_losses.note IS 'Обоснование';

-- ======================================================================

-- Таблица: public.rul_consumption_pipe
CREATE TABLE public.rul_consumption_pipe (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    value numeric,
    accounting_type_node_id bigint,
    node_calculate_parameter_id bigint
);

-- ======================================================================

-- Таблица: public.rul_consumption_source_connection
CREATE TABLE public.rul_consumption_source_connection (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    accounting_type_node_id bigint,
    node_calculate_parameter_id bigint,
    accounting_type_id bigint,
    value numeric,
    coefficient numeric DEFAULT 1,
    theoretical_calculation boolean DEFAULT true,
    balancing_coefficient numeric,
    balancing_id bigint,
    note character varying(256)
);
COMMENT ON COLUMN public.rul_consumption_source_connection.balancing_coefficient IS 'Коэффициент балансировки';
COMMENT ON COLUMN public.rul_consumption_source_connection.coefficient IS 'Коэффициент распределения';
COMMENT ON COLUMN public.rul_consumption_source_connection.note IS 'Обоснование';
COMMENT ON COLUMN public.rul_consumption_source_connection.theoretical_calculation IS 'Флаг указывающий на то, брать ли в начисления расход';

-- ======================================================================

-- Таблица: public.rul_consumption_standard
CREATE TABLE public.rul_consumption_standard (
    connection_id bigint,
    connection_name character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    value numeric,
    formula_connection_id bigint,
    version_load_standard_id bigint,
    accounting_type_node_id bigint,
    node_calculate_parameter_id bigint,
    coefficient numeric DEFAULT 1,
    theoretical_calculation boolean DEFAULT true,
    balancing_coefficient numeric,
    balancing_id bigint,
    note character varying(256)
);
COMMENT ON COLUMN public.rul_consumption_standard.balancing_coefficient IS 'Коэффициент балансировки';
COMMENT ON COLUMN public.rul_consumption_standard.coefficient IS 'Коэффициент распределения';
COMMENT ON COLUMN public.rul_consumption_standard.note IS 'Обоснование';
COMMENT ON COLUMN public.rul_consumption_standard.theoretical_calculation IS 'Флаг указывающий на то, брать ли в начисления расход';

-- ======================================================================

-- Таблица: public.rul_currency
CREATE TABLE public.rul_currency (
    currency_code character varying(16) NOT NULL,
    currency_id bigint DEFAULT nextval('rul_currency_currency_id_seq'::regclass) NOT NULL,
    currency_name character varying(256),
    currency_fract_name character varying(256),
    currency_symbol character(1)
    ,
    CONSTRAINT rul_currency_pkey PRIMARY KEY (currency_code),
    CONSTRAINT rul_currency_currency_id_key UNIQUE (currency_id)
);
COMMENT ON COLUMN public.rul_currency.currency_code IS 'Код валюты';
COMMENT ON COLUMN public.rul_currency.currency_fract_name IS 'Название копеек (цент и т.д.)';
COMMENT ON COLUMN public.rul_currency.currency_name IS 'Название валюты';
COMMENT ON COLUMN public.rul_currency.currency_symbol IS 'Символ, которым обозначаются деньги в данной валюте';

-- ======================================================================

-- Таблица: public.rul_currency_rate
CREATE TABLE public.rul_currency_rate (
    currency_code character varying(16) NOT NULL,
    currency_rate_date timestamp(0) without time zone NOT NULL,
    currency_rate numeric NOT NULL,
    currency_rate_id bigint DEFAULT nextval('rul_currency_rate_currency_rate_id_seq'::regclass) NOT NULL
    ,
    CONSTRAINT bas_currency_rate_pkey PRIMARY KEY (currency_code, currency_rate_date)
);
COMMENT ON COLUMN public.rul_currency_rate.currency_code IS 'Код валюты';
COMMENT ON COLUMN public.rul_currency_rate.currency_rate IS 'Курс валюты';
COMMENT ON COLUMN public.rul_currency_rate.currency_rate_date IS 'Дата курса валюты';

-- ======================================================================

-- Таблица: public.rul_debug_log
CREATE TABLE public.rul_debug_log (
    debug_log_id bigint DEFAULT nextval('rul_debug_log_debug_log_id_seq'::regclass) NOT NULL,
    debug_user_id character varying(64),
    debug_module character varying(64),
    debug_message character varying(512),
    debug_time timestamp without time zone DEFAULT (CURRENT_TIMESTAMP)::timestamp without time zone
    ,
    CONSTRAINT rul_debug_log_pkey PRIMARY KEY (debug_log_id)
);

-- ======================================================================

-- Таблица: public.rul_district
CREATE TABLE public.rul_district (
    district_id bigint DEFAULT nextval('rul_district_district_id_seq'::regclass) NOT NULL,
    district_name character varying(2048),
    region_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_district_pkey PRIMARY KEY (district_id),
    CONSTRAINT fk_op_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id)
);
COMMENT ON COLUMN public.rul_district.district_name IS 'Название района';
COMMENT ON COLUMN public.rul_district.region_id IS 'Ссылка на область';

-- ======================================================================

-- Таблица: public.rul_entity
CREATE TABLE public.rul_entity (
    entity_id bigint DEFAULT nextval('rul_entity_entity_id_seq'::regclass) NOT NULL,
    entity_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_entity_pkey PRIMARY KEY (entity_id)
);
COMMENT ON COLUMN public.rul_entity.entity_name IS 'Вид сущности к которой привязан файл';

-- ======================================================================

-- Таблица: public.rul_file
CREATE TABLE public.rul_file (
    file_id bigint DEFAULT nextval('rul_file_file_id_seq'::regclass) NOT NULL,
    real_name character varying(256),
    label_name character varying(256),
    file_extension_id bigint,
    size integer,
    width integer,
    height integer,
    file_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    entity_id bigint,
    entity_item_id bigint,
    file_matter_type_id bigint,
    hash character varying(255)
    ,
    CONSTRAINT pk_file PRIMARY KEY (file_id),
    CONSTRAINT fk_entity_id FOREIGN KEY (entity_id) REFERENCES rul_entity(entity_id),
    CONSTRAINT fk_file__file_extension FOREIGN KEY (file_extension_id) REFERENCES rul_file_extension(file_extension_id),
    CONSTRAINT fk_file__file_type FOREIGN KEY (file_type_id) REFERENCES rul_file_type(file_type_id),
    CONSTRAINT fk_file_content_type_id FOREIGN KEY (file_matter_type_id) REFERENCES rul_file_matter_type(file_matter_type_id)
);
COMMENT ON COLUMN public.rul_file.file_matter_type_id IS 'Ссылка на ид. типа контента в файле';

-- ======================================================================

-- Таблица: public.rul_file_extension
CREATE TABLE public.rul_file_extension (
    file_extension_id bigint DEFAULT nextval('rul_file_extension_file_extension_id_seq'::regclass) NOT NULL,
    file_type_id bigint,
    name character varying(256),
    content_type character varying(255),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT pk_file_extension PRIMARY KEY (file_extension_id),
    CONSTRAINT fk_file_extension_file_type FOREIGN KEY (file_type_id) REFERENCES rul_file_type(file_type_id)
);

-- ======================================================================

-- Таблица: public.rul_file_matter_type
CREATE TABLE public.rul_file_matter_type (
    file_matter_type_id bigint DEFAULT nextval('rul_file_matter_type_file_matter_type_id_seq'::regclass) NOT NULL,
    file_matter_type_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_file_content_type_pkey PRIMARY KEY (file_matter_type_id)
);

COMMENT ON TABLE public.rul_file_matter_type IS 'Таблица типов контента в файле';

COMMENT ON COLUMN public.rul_file_matter_type.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_file_matter_type.file_matter_type_id IS 'Идентификатор типа контента в файле';
COMMENT ON COLUMN public.rul_file_matter_type.file_matter_type_name IS 'Название типа контента в файле';
COMMENT ON COLUMN public.rul_file_matter_type.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';

-- ======================================================================

-- Таблица: public.rul_file_type
CREATE TABLE public.rul_file_type (
    file_type_id bigint DEFAULT nextval('rul_file_type_file_type_id_seq'::regclass) NOT NULL,
    file_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT pk_file_type PRIMARY KEY (file_type_id)
);

-- ======================================================================

-- Таблица: public.rul_first_indication
CREATE TABLE public.rul_first_indication (
    first_indication_id bigint DEFAULT nextval('rul_first_indication_first_indication_id_seq'::regclass) NOT NULL,
    value numeric,
    parameter_id bigint,
    node_meter_id bigint,
    check_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_first_indication_pkey PRIMARY KEY (first_indication_id),
    CONSTRAINT fk_node_meter_id FOREIGN KEY (node_meter_id) REFERENCES rul_node_meter(node_meter_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);
COMMENT ON COLUMN public.rul_first_indication.node_meter_id IS 'Ссылка на размещение прибора учета';
COMMENT ON COLUMN public.rul_first_indication.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_first_indication.value IS 'Показание';

-- ======================================================================

-- Таблица: public.rul_formula
CREATE TABLE public.rul_formula (
    formula_id bigint DEFAULT nextval('rul_formula_formula_id_seq'::regclass) NOT NULL,
    formula_name character varying(256),
    file_id bigint,
    parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    formula_text character varying(512),
    source_consumption_id bigint,
    is_template numeric(1,0) DEFAULT 0,
    method_id bigint
    ,
    CONSTRAINT rul_formula_pkey PRIMARY KEY (formula_id),
    CONSTRAINT fk_file_id FOREIGN KEY (file_id) REFERENCES rul_file(file_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_source_consumption_id FOREIGN KEY (source_consumption_id) REFERENCES rul_source_consumption(source_consumption_id)
);
COMMENT ON COLUMN public.rul_formula.file_id IS 'Ссылка на Изображение??';
COMMENT ON COLUMN public.rul_formula.formula_name IS 'Название формулы';
COMMENT ON COLUMN public.rul_formula.formula_text IS 'Формула отображающаяся в интерфейсе';
COMMENT ON COLUMN public.rul_formula.is_template IS 'Флаг, является ли шаблоном формула';
COMMENT ON COLUMN public.rul_formula.method_id IS 'Метод, не ссылка';
COMMENT ON COLUMN public.rul_formula.parameter_id IS 'Параметр энергоресурса, который получается в результе вычисления';
COMMENT ON COLUMN public.rul_formula.source_consumption_id IS 'Ссылка на источник данных о расходах';

-- ======================================================================

-- Таблица: public.rul_formula_connection
CREATE TABLE public.rul_formula_connection (
    formula_connection_id bigint DEFAULT nextval('rul_formula_connection_formula_connection_id_seq'::regclass) NOT NULL,
    formula_id bigint,
    connection_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_formula_connection_pkey PRIMARY KEY (formula_connection_id),
    CONSTRAINT fk_connection_id FOREIGN KEY (connection_id) REFERENCES rul_connection(connection_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id)
);
COMMENT ON COLUMN public.rul_formula_connection.connection_id IS 'Ссылка на подключение';
COMMENT ON COLUMN public.rul_formula_connection.formula_id IS 'Ссылка на формулу';

-- ======================================================================

-- Таблица: public.rul_functional_access
CREATE TABLE public.rul_functional_access (
    functional_access_id bigint DEFAULT nextval('rul_functional_access_functional_access_id_seq'::regclass) NOT NULL,
    functional_access_name character varying(250) NOT NULL,
    system_name character varying(124),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    weight smallint
    ,
    CONSTRAINT rul_functional_access_pkey PRIMARY KEY (functional_access_id)
);

COMMENT ON TABLE public.rul_functional_access IS 'Справочник функциональных ролей';

COMMENT ON COLUMN public.rul_functional_access.deleted IS 'Удален или нет';
COMMENT ON COLUMN public.rul_functional_access.functional_access_name IS 'Название роли';
COMMENT ON COLUMN public.rul_functional_access.op_date IS 'Дата совершения действия';
COMMENT ON COLUMN public.rul_functional_access.op_user_id IS 'Ссылка на таблицу пользователи';
COMMENT ON COLUMN public.rul_functional_access.system_name IS 'Системное имя';

-- ======================================================================

-- Таблица: public.rul_group_consumption
CREATE TABLE public.rul_group_consumption (
    group_consumption_id bigint DEFAULT nextval('rul_group_consumption_group_consumption_id_seq'::regclass) NOT NULL,
    group_consumption_name character varying(256) NOT NULL,
    client_id bigint,
    description character varying(256) NOT NULL,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_group_consumption_pkey PRIMARY KEY (group_consumption_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);
COMMENT ON COLUMN public.rul_group_consumption.client_id IS 'Ссылка на контрагента';
COMMENT ON COLUMN public.rul_group_consumption.description IS 'Описание';
COMMENT ON COLUMN public.rul_group_consumption.group_consumption_name IS 'Наименование группы назначений потребления';

-- ======================================================================

-- Таблица: public.rul_group_purpose_consumption
CREATE TABLE public.rul_group_purpose_consumption (
    group_purpose_consumption_id bigint DEFAULT nextval('rul_group_purpose_consumption_group_purpose_consumption_id_seq'::regclass) NOT NULL,
    group_consumption_id bigint,
    purpose_consumption_id bigint,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_group_purpose_consumption_pkey PRIMARY KEY (group_purpose_consumption_id),
    CONSTRAINT fk_client_id FOREIGN KEY (purpose_consumption_id) REFERENCES rul_purpose_consumption(purpose_consumption_id),
    CONSTRAINT fk_consumption_group_id FOREIGN KEY (group_consumption_id) REFERENCES rul_group_consumption(group_consumption_id)
);

-- ======================================================================

-- Таблица: public.rul_group_recalculation_attitude
CREATE TABLE public.rul_group_recalculation_attitude (
    group_recalculation_attitude_id bigint DEFAULT nextval('rul_group_recalculation_attit_group_recalculation_attitude__seq'::regclass) NOT NULL,
    group_recalculation_attitude_name character varying(256)
    ,
    CONSTRAINT rul_group_recalculation_attitude_pkey PRIMARY KEY (group_recalculation_attitude_id)
);

-- ======================================================================

-- Таблица: public.rul_holiday
CREATE TABLE public.rul_holiday (
    holiday_id bigint DEFAULT nextval('rul_holiday_holiday_id_seq'::regclass) NOT NULL,
    holiday_name character varying(256),
    holiday_date timestamp without time zone NOT NULL,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_holiday_pkey PRIMARY KEY (holiday_id)
);
COMMENT ON COLUMN public.rul_holiday.holiday_date IS 'Дата праздника';
COMMENT ON COLUMN public.rul_holiday.holiday_name IS 'Название праздника';

-- ======================================================================

-- Таблица: public.rul_indexing
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
COMMENT ON COLUMN public.rul_indexing.currency_rate IS 'Курс ваюты на дату проведения индексации';
COMMENT ON COLUMN public.rul_indexing.index_amount IS 'Сумма индексации без НДС';
COMMENT ON COLUMN public.rul_indexing.index_coefficient IS 'Коэффициент индексации';
COMMENT ON COLUMN public.rul_indexing.index_date IS 'Дата индексации';
COMMENT ON COLUMN public.rul_indexing.index_nds IS 'НДС от суммы индексации';
COMMENT ON COLUMN public.rul_indexing.index_value IS 'Проидексированный тариф';
COMMENT ON COLUMN public.rul_indexing.invoice_id IS 'Ссылка на счет куда включено';
COMMENT ON COLUMN public.rul_indexing.is_clone IS 'Показывает, что индексация заведена как клонированная. Все клоннированые записи инвертируются с -.';
COMMENT ON COLUMN public.rul_indexing.percent_charge_repayment IS 'Погашено по начислению, процент';
COMMENT ON COLUMN public.rul_indexing.percent_index_consumption IS 'Индексируемый процент расхода';
COMMENT ON COLUMN public.rul_indexing.transaction_reversal_id IS 'Ссылка на сторнирующую запись, которая создала эту индексацию';

-- ======================================================================

-- Таблица: public.rul_indication_type
CREATE TABLE public.rul_indication_type (
    indication_type_id bigint DEFAULT nextval('rul_indication_type_indication_type_id_seq'::regclass) NOT NULL,
    indication_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_indication_type_pkey PRIMARY KEY (indication_type_id)
);
COMMENT ON COLUMN public.rul_indication_type.indication_type_name IS 'Название типа показания';

-- ======================================================================

-- Таблица: public.rul_inspector_route
CREATE TABLE public.rul_inspector_route (
    inspector_route_id bigint DEFAULT nextval('rul_inspector_route_inspector_route_id_seq'::regclass) NOT NULL,
    route character varying(64),
    user_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    object_id bigint
    ,
    CONSTRAINT rul_inspector_route_pkey PRIMARY KEY (inspector_route_id),
    CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES rul_object(object_id),
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES rul_user(user_id)
);
COMMENT ON COLUMN public.rul_inspector_route.object_id IS 'Ссылка на объект';
COMMENT ON COLUMN public.rul_inspector_route.route IS 'Нащвание маршрута';
COMMENT ON COLUMN public.rul_inspector_route.user_id IS 'Контроллер (только сотрудники, не пользователи)';

-- ======================================================================

-- Таблица: public.rul_invoice
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
COMMENT ON COLUMN public.rul_invoice.agreement_id IS 'Ссылка на договор';
COMMENT ON COLUMN public.rul_invoice.balance IS 'Сальдо на дату выставления (остаток)';
COMMENT ON COLUMN public.rul_invoice.billing_end_date IS 'Дата завершения расчетного периода';
COMMENT ON COLUMN public.rul_invoice.billing_start_date IS 'Дата начала расчетного периода';
COMMENT ON COLUMN public.rul_invoice.create_date IS 'Дата выставления(подтверждения?) счета';
COMMENT ON COLUMN public.rul_invoice.indexing_amount IS 'Индексация без НДС, облагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.indexing_amount_unnds IS 'Индексация необлагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.indexing_nds IS 'НДС по индексации';
COMMENT ON COLUMN public.rul_invoice.invoice_code IS 'Номер счета (Буквы/цифры)';
COMMENT ON COLUMN public.rul_invoice.invoice_confirm_status_id IS 'Ссылка на ид. статуса подтверждения счет-фактуры';
COMMENT ON COLUMN public.rul_invoice.invoice_group_index IS 'ИГС';
COMMENT ON COLUMN public.rul_invoice.invoice_type_id IS 'Ссылка на тип счета фактуры';
COMMENT ON COLUMN public.rul_invoice.pay_value IS 'Оплачено в течении месяца (без понятия точно ли такое поле нужно было)';
COMMENT ON COLUMN public.rul_invoice.penalty IS '(возможно не используется) Размер пени';
COMMENT ON COLUMN public.rul_invoice.penalty_amount IS 'Пеня без НДС, облагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.penalty_amount_unnds IS 'Пеня не облагаемая НДС';
COMMENT ON COLUMN public.rul_invoice.penalty_nds IS 'НДС по пене';
COMMENT ON COLUMN public.rul_invoice.sum_amount IS 'Начисления без НДС, облагаемые НДС';
COMMENT ON COLUMN public.rul_invoice.sum_amount_nds IS 'Не используется';
COMMENT ON COLUMN public.rul_invoice.sum_amount_unnds IS 'Начисления необлагаемые НДС';
COMMENT ON COLUMN public.rul_invoice.sum_nds IS 'НДС по начислениям';
COMMENT ON COLUMN public.rul_invoice.total_amount IS 'Итого к оплате';

-- ======================================================================

-- Таблица: public.rul_invoice_confirm_status
CREATE TABLE public.rul_invoice_confirm_status (
    invoice_confirm_status_id bigint DEFAULT nextval('rul_invoice_confirm_status_invoice_confirm_status_id_seq'::regclass) NOT NULL,
    invoice_confirm_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_invoice_confirm_status_pkey PRIMARY KEY (invoice_confirm_status_id)
);

COMMENT ON TABLE public.rul_invoice_confirm_status IS 'Таблица статусов подтверждения счет-фактуры';

COMMENT ON COLUMN public.rul_invoice_confirm_status.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_invoice_confirm_status.invoice_confirm_status_id IS 'Идентификатор статуса подтверждения счет-фактуры';
COMMENT ON COLUMN public.rul_invoice_confirm_status.invoice_confirm_status_name IS 'Название статуса подтверждения счет-фактуры';
COMMENT ON COLUMN public.rul_invoice_confirm_status.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';

-- ======================================================================

-- Таблица: public.rul_invoice_digital_signature_status
CREATE TABLE public.rul_invoice_digital_signature_status (
    invoice_digital_signature_status_id bigint DEFAULT nextval('rul_invoice_digital_signature_invoice_digital_signature_sta_seq'::regclass) NOT NULL,
    invoice_digital_signature_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_invoice_digital_signature_status_pkey PRIMARY KEY (invoice_digital_signature_status_id)
);

-- ======================================================================

-- Таблица: public.rul_invoice_type
CREATE TABLE public.rul_invoice_type (
    invoice_type_id bigint DEFAULT nextval('rul_invoice_type_invoice_type_id_seq'::regclass) NOT NULL,
    invoice_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_invoice_type_pkey PRIMARY KEY (invoice_type_id)
);
COMMENT ON COLUMN public.rul_invoice_type.invoice_type_name IS 'Тип счета';

-- ======================================================================

-- Таблица: public.rul_juristic_company
CREATE TABLE public.rul_juristic_company (
    juristic_company_id bigint DEFAULT nextval('rul_juristic_company_juristic_company_id_seq'::regclass) NOT NULL,
    fullname character varying(2048),
    shortname character varying(2048),
    street_id bigint,
    building character varying(128),
    housing character varying(128),
    room character varying(128),
    post_index character varying(128),
    stat_code character varying(2048),
    oked character varying(2048),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    client_id bigint,
    locality_id bigint,
    district_id bigint,
    region_id bigint,
    ogrn character varying(128),
    inn character varying(128),
    unp character varying(128)
    ,
    CONSTRAINT rul_juristic_company_pkey PRIMARY KEY (juristic_company_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_juristic_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_juristic_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_juristic_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id),
    CONSTRAINT fk_juristic_street_id FOREIGN KEY (street_id) REFERENCES rul_street(street_id)
);
COMMENT ON COLUMN public.rul_juristic_company.building IS 'Юр.адресс: Номер здания';
COMMENT ON COLUMN public.rul_juristic_company.district_id IS 'Юр.адресс: Ссылка на район(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.end_date IS 'Конец даты действия';
COMMENT ON COLUMN public.rul_juristic_company.fullname IS 'Полное название юр. лица';
COMMENT ON COLUMN public.rul_juristic_company.housing IS 'Юр.адресс: Номер корпуса';
COMMENT ON COLUMN public.rul_juristic_company.inn IS 'ИНН';
COMMENT ON COLUMN public.rul_juristic_company.locality_id IS 'Юр.адресс: Ссылка на населенный пункт(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.ogrn IS 'ОГРН';
COMMENT ON COLUMN public.rul_juristic_company.oked IS 'ОКЭД';
COMMENT ON COLUMN public.rul_juristic_company.post_index IS 'Юр.адресс: Почтовый индекс';
COMMENT ON COLUMN public.rul_juristic_company.region_id IS 'Юр.адресс: Ссылка на область(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.room IS 'Юр.адресс: Номер помещения';
COMMENT ON COLUMN public.rul_juristic_company.shortname IS 'Короткое название юр.лица / ФИО';
COMMENT ON COLUMN public.rul_juristic_company.start_date IS 'Начало даты действия';
COMMENT ON COLUMN public.rul_juristic_company.stat_code IS 'Статкод';
COMMENT ON COLUMN public.rul_juristic_company.street_id IS 'Юр.адресс: Ссылка на улицу(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.unp IS 'УНП';

-- ======================================================================

-- Таблица: public.rul_last_month_node_panel_value
CREATE TABLE public.rul_last_month_node_panel_value (
    node_panel_value_id bigint,
    value_number numeric,
    check_date timestamp without time zone,
    check_type_id bigint,
    node_panel_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint,
    is_correct numeric(1,0),
    changed_user_id bigint
);

-- ======================================================================

-- Таблица: public.rul_line
CREATE TABLE public.rul_line (
    line_id bigint DEFAULT nextval('rul_line_line_id_seq'::regclass) NOT NULL,
    line_name character varying(128),
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    node_id bigint,
    child_node_id bigint,
    network_fragment_id bigint
    ,
    CONSTRAINT rul_line_pkey PRIMARY KEY (line_id),
    CONSTRAINT fk_child_node_id FOREIGN KEY (child_node_id) REFERENCES rul_node(node_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_network_fragment FOREIGN KEY (network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id),
    CONSTRAINT fk_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id)
);
COMMENT ON COLUMN public.rul_line.child_node_id IS 'Дочерний узел';
COMMENT ON COLUMN public.rul_line.client_id IS 'Балансодержатель/контрагент';
COMMENT ON COLUMN public.rul_line.line_name IS 'Название линии';
COMMENT ON COLUMN public.rul_line.network_fragment_id IS 'Ссылка на фрагмент';
COMMENT ON COLUMN public.rul_line.node_id IS 'Родительский узел';

-- ======================================================================

-- Таблица: public.rul_line_parameter
CREATE TABLE public.rul_line_parameter (
    line_parameter_id bigint DEFAULT nextval('rul_line_parameter_line_parameter_id_seq'::regclass) NOT NULL,
    line_id bigint,
    parameter_id bigint,
    node_calculate_parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    formula_id bigint
    ,
    CONSTRAINT rul_line_parameter_pkey PRIMARY KEY (line_parameter_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_line_id FOREIGN KEY (line_id) REFERENCES rul_line(line_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);
COMMENT ON COLUMN public.rul_line_parameter.formula_id IS 'Ссылка на формулу расчета потерь';
COMMENT ON COLUMN public.rul_line_parameter.line_id IS 'Ссылка на линию';
COMMENT ON COLUMN public.rul_line_parameter.node_calculate_parameter_id IS 'Ссылка на расчетный параметр, от которого идут "листья"';
COMMENT ON COLUMN public.rul_line_parameter.parameter_id IS 'Ссылка на параметр';

-- ======================================================================

-- Таблица: public.rul_line_parameter_child
CREATE TABLE public.rul_line_parameter_child (
    line_parameter_child_id bigint DEFAULT nextval('rul_line_parameter_child_line_parameter_child_id_seq'::regclass) NOT NULL,
    line_parameter_id bigint,
    node_calculate_parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_line_parameter_child_pkey PRIMARY KEY (line_parameter_child_id),
    CONSTRAINT fk_line_parameter_id FOREIGN KEY (line_parameter_id) REFERENCES rul_line_parameter(line_parameter_id),
    CONSTRAINT fk_node_calculate_parameter_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id)
);
COMMENT ON COLUMN public.rul_line_parameter_child.line_parameter_id IS 'Ссылка на верхний узел(расчетный параметр) из дерева';
COMMENT ON COLUMN public.rul_line_parameter_child.node_calculate_parameter_id IS 'Ссылка на дочерний расчетный параметр';

-- ======================================================================

-- Таблица: public.rul_load_standard_value
CREATE TABLE public.rul_load_standard_value (
    load_standard_value_id bigint DEFAULT nextval('rul_load_standard_value_load_standard_value_id_seq'::regclass) NOT NULL,
    argument_formula_id bigint,
    value numeric,
    version_load_standard_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_load_standard_value_pkey PRIMARY KEY (load_standard_value_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_version_load_standard_id FOREIGN KEY (version_load_standard_id) REFERENCES rul_version_load_standard(version_load_standard_id)
);
COMMENT ON COLUMN public.rul_load_standard_value.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_load_standard_value.value IS 'Значение норматива/нагрузки';
COMMENT ON COLUMN public.rul_load_standard_value.version_load_standard_id IS 'Ссылка на версию показателя';

-- ======================================================================

-- Таблица: public.rul_locality
CREATE TABLE public.rul_locality (
    locality_id bigint DEFAULT nextval('rul_locality_locality_id_seq'::regclass) NOT NULL,
    locality_name character varying(2048),
    district_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    region_id bigint
    ,
    CONSTRAINT rul_locality_pkey PRIMARY KEY (locality_id),
    CONSTRAINT fk_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id)
);
COMMENT ON COLUMN public.rul_locality.district_id IS 'Ссылка на район';
COMMENT ON COLUMN public.rul_locality.locality_name IS 'Название населенного пункта';
COMMENT ON COLUMN public.rul_locality.region_id IS 'Ссылка на область';

-- ======================================================================

-- Таблица: public.rul_losses_params
CREATE TABLE public.rul_losses_params (
    losses_params_id bigint DEFAULT nextval('rul_losses_params_losses_params_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    mon_work_hours numeric,
    tue_work_hours numeric,
    wed_work_hours numeric,
    thu_work_hours numeric,
    fri_work_hours numeric,
    sat_work_hours numeric,
    sun_work_hours numeric,
    supply_temperature numeric,
    return_temperature numeric,
    recharge_temperature numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    generated numeric
    ,
    CONSTRAINT rul_losses_params_pkey PRIMARY KEY (losses_params_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id)
);
COMMENT ON COLUMN public.rul_losses_params.accounting_type_node_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_losses_params.fri_work_hours IS 'Кол-во рабочих часов в пятницу';
COMMENT ON COLUMN public.rul_losses_params.generated IS 'Выработано';
COMMENT ON COLUMN public.rul_losses_params.mon_work_hours IS 'Кол-во рабочих часов в понедельник';
COMMENT ON COLUMN public.rul_losses_params.recharge_temperature IS 'Температура подпитки';
COMMENT ON COLUMN public.rul_losses_params.return_temperature IS 'Температура обратки';
COMMENT ON COLUMN public.rul_losses_params.sat_work_hours IS 'Кол-во рабочих часов в субботу';
COMMENT ON COLUMN public.rul_losses_params.sun_work_hours IS 'Кол-во рабочих часов в воскресенье';
COMMENT ON COLUMN public.rul_losses_params.supply_temperature IS 'Температура подачи';
COMMENT ON COLUMN public.rul_losses_params.thu_work_hours IS 'Кол-во рабочих часов в четверг';
COMMENT ON COLUMN public.rul_losses_params.tue_work_hours IS 'Кол-во рабочих часов во вторник';
COMMENT ON COLUMN public.rul_losses_params.wed_work_hours IS 'Кол-во рабочих часов в среду';

-- ======================================================================

-- Таблица: public.rul_losses_policy
CREATE TABLE public.rul_losses_policy (
    losses_policy_id bigint DEFAULT nextval('rul_losses_policy_losses_policy_id_seq'::regclass) NOT NULL,
    losses_policy_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_losses_policy_pkey PRIMARY KEY (losses_policy_id)
);
COMMENT ON COLUMN public.rul_losses_policy.losses_policy_name IS 'Политика потерь';

-- ======================================================================

-- Таблица: public.rul_meter
CREATE TABLE public.rul_meter (
    meter_id bigint DEFAULT nextval('rul_meter_meter_id_seq'::regclass) NOT NULL,
    meter_name character varying(1024),
    description character varying(1024),
    serial_number character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    brand_id bigint,
    client_id bigint,
    manufacture_date timestamp without time zone,
    responsible_client_id bigint
    ,
    CONSTRAINT rul_meter_pkey PRIMARY KEY (meter_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_responsible_client_id FOREIGN KEY (responsible_client_id) REFERENCES rul_client(client_id)
);
COMMENT ON COLUMN public.rul_meter.brand_id IS 'Сслыка на марку производетеля';
COMMENT ON COLUMN public.rul_meter.client_id IS 'Держатель счетчика (у кого на балансе)';
COMMENT ON COLUMN public.rul_meter.description IS 'Описание прибора учета';
COMMENT ON COLUMN public.rul_meter.manufacture_date IS 'Дата производства';
COMMENT ON COLUMN public.rul_meter.meter_name IS 'Название прибора учета (счетчика)';
COMMENT ON COLUMN public.rul_meter.responsible_client_id IS 'Ссылка на ответсвенного поставщика';
COMMENT ON COLUMN public.rul_meter.serial_number IS 'Серийный номер';

-- ======================================================================

-- Таблица: public.rul_meter_check
CREATE TABLE public.rul_meter_check (
    meter_check_id bigint DEFAULT nextval('rul_meter_check_meter_check_id_seq'::regclass) NOT NULL,
    check_date timestamp without time zone,
    next_check_date timestamp without time zone,
    comment character varying(256),
    meter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_meter_check_pkey PRIMARY KEY (meter_check_id),
    CONSTRAINT fk_meter_id FOREIGN KEY (meter_id) REFERENCES rul_meter(meter_id)
);
COMMENT ON COLUMN public.rul_meter_check.check_date IS 'Дата проведения поверки';
COMMENT ON COLUMN public.rul_meter_check.comment IS 'Примечание';
COMMENT ON COLUMN public.rul_meter_check.meter_id IS 'Ссылка на прибор учета';
COMMENT ON COLUMN public.rul_meter_check.next_check_date IS 'Дата следующей поверки';

-- ======================================================================

-- Таблица: public.rul_method_tubing
CREATE TABLE public.rul_method_tubing (
    method_tubing_id bigint DEFAULT nextval('rul_method_tubing_method_tubing_id_seq'::regclass) NOT NULL,
    method_tubing_name character varying(128),
    parameter_target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_method_tubing_pkey PRIMARY KEY (method_tubing_id),
    CONSTRAINT fk_parameter_target_use_id FOREIGN KEY (parameter_target_use_id) REFERENCES rul_parameter_target_use(parameter_target_use_id)
);
COMMENT ON COLUMN public.rul_method_tubing.method_tubing_name IS 'Название способа прокладки';
COMMENT ON COLUMN public.rul_method_tubing.parameter_target_use_id IS 'Ссылка на назначение';

-- ======================================================================

-- Таблица: public.rul_network_fragment
CREATE TABLE public.rul_network_fragment (
    network_fragment_id bigint DEFAULT nextval('rul_network_fragment_network_fragment_id_seq'::regclass) NOT NULL,
    network_fragment_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    network_fragment_name character varying(64),
    client_id bigint
    ,
    CONSTRAINT rul_network_fragment_pkey PRIMARY KEY (network_fragment_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_network_fragment_type_id FOREIGN KEY (network_fragment_type_id) REFERENCES rul_network_fragment_type(network_fragment_type_id)
);
COMMENT ON COLUMN public.rul_network_fragment.client_id IS 'Ссылка на клиента';
COMMENT ON COLUMN public.rul_network_fragment.network_fragment_name IS 'Название фрагмента';
COMMENT ON COLUMN public.rul_network_fragment.network_fragment_type_id IS 'Тип фрагмента';

-- ======================================================================

-- Таблица: public.rul_network_fragment_link
CREATE TABLE public.rul_network_fragment_link (
    parent_network_fragment_id bigint,
    child_network_fragment_id bigint
    ,
    CONSTRAINT fk_child_network_fragment_id FOREIGN KEY (child_network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id),
    CONSTRAINT fk_parent_network_fragment_id FOREIGN KEY (parent_network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id)
);
COMMENT ON COLUMN public.rul_network_fragment_link.child_network_fragment_id IS 'Дочерний фрагмент';
COMMENT ON COLUMN public.rul_network_fragment_link.parent_network_fragment_id IS 'Фрагмент родитель';

-- ======================================================================

-- Таблица: public.rul_network_fragment_type
CREATE TABLE public.rul_network_fragment_type (
    network_fragment_type_id bigint DEFAULT nextval('rul_network_fragment_type_network_fragment_type_id_seq'::regclass) NOT NULL,
    network_fragment_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_network_fragment_type_pkey PRIMARY KEY (network_fragment_type_id)
);
COMMENT ON COLUMN public.rul_network_fragment_type.network_fragment_type_name IS 'Тип фрагмента';

-- ======================================================================

-- Таблица: public.rul_node
CREATE TABLE public.rul_node (
    node_id bigint DEFAULT nextval('rul_node_node_id_seq'::regclass) NOT NULL,
    node_name character varying(1024),
    code character varying(256),
    placement character varying(1024),
    object_id bigint,
    service_type_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    node_type_id bigint,
    responsible_client_id bigint
    ,
    CONSTRAINT rul_node_pkey PRIMARY KEY (node_id),
    CONSTRAINT fk_node_type_id FOREIGN KEY (node_type_id) REFERENCES rul_node_type(node_type_id),
    CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES rul_object(object_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);
COMMENT ON COLUMN public.rul_node.code IS 'Номер узла';
COMMENT ON COLUMN public.rul_node.end_date IS 'Завершение даты действия';
COMMENT ON COLUMN public.rul_node.node_name IS 'Название узла';
COMMENT ON COLUMN public.rul_node.object_id IS 'Ссылка на объект на котором находится узел';
COMMENT ON COLUMN public.rul_node.placement IS 'Место размещения узла';
COMMENT ON COLUMN public.rul_node.service_type_id IS 'Ссылка на тип услуги (энергоресурс)';
COMMENT ON COLUMN public.rul_node.start_date IS 'Начало даты действия';

-- ======================================================================

-- Таблица: public.rul_node_calculate_parameter
CREATE TABLE public.rul_node_calculate_parameter (
    node_calculate_parameter_id bigint DEFAULT nextval('rul_node_calculate_parameter_node_calculate_parameter_id_seq'::regclass) NOT NULL,
    parameter_id bigint,
    node_id bigint,
    target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    balancing_node_calculate_parameter_id bigint,
    commercial_node_calculate_parameter_id bigint
    ,
    CONSTRAINT rul_node_calculate_parameter_pkey PRIMARY KEY (node_calculate_parameter_id),
    CONSTRAINT fk_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_target_use_id FOREIGN KEY (target_use_id) REFERENCES rul_target_use(target_use_id)
);
COMMENT ON COLUMN public.rul_node_calculate_parameter.balancing_node_calculate_parameter_id IS 'Ссылка на баланскный узел черз РП';
COMMENT ON COLUMN public.rul_node_calculate_parameter.commercial_node_calculate_parameter_id IS 'Указывает на верхний коммерческий узел';
COMMENT ON COLUMN public.rul_node_calculate_parameter.node_id IS 'Ссылка на узел учета';
COMMENT ON COLUMN public.rul_node_calculate_parameter.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_node_calculate_parameter.target_use_id IS 'Ссылка на назначение';

-- ======================================================================

-- Таблица: public.rul_node_meter
CREATE TABLE public.rul_node_meter (
    node_meter_id bigint DEFAULT nextval('rul_node_meter_node_meter_id_seq'::regclass) NOT NULL,
    meter_id bigint,
    node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_node_meter_pkey PRIMARY KEY (node_meter_id),
    CONSTRAINT fk_meter_id FOREIGN KEY (meter_id) REFERENCES rul_meter(meter_id),
    CONSTRAINT fk_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id)
);
COMMENT ON COLUMN public.rul_node_meter.end_date IS 'Конец даты действия';
COMMENT ON COLUMN public.rul_node_meter.meter_id IS 'Ссылка на прибор учета';
COMMENT ON COLUMN public.rul_node_meter.node_id IS 'Ссылка на узел установки';
COMMENT ON COLUMN public.rul_node_meter.start_date IS 'Начало даты действия';

-- ======================================================================

-- Таблица: public.rul_node_panel
CREATE TABLE public.rul_node_panel (
    node_panel_id bigint DEFAULT nextval('rul_node_panel_node_panel_id_seq'::regclass) NOT NULL,
    panel_id bigint,
    parameter_id bigint,
    node_meter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_node_panel_pkey PRIMARY KEY (node_panel_id),
    CONSTRAINT fk_node_meter_id FOREIGN KEY (node_meter_id) REFERENCES rul_node_meter(node_meter_id),
    CONSTRAINT fk_panel_id FOREIGN KEY (panel_id) REFERENCES rul_panel(panel_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);
COMMENT ON COLUMN public.rul_node_panel.node_meter_id IS 'Ссылка на размещение счетчика в узле';
COMMENT ON COLUMN public.rul_node_panel.panel_id IS 'Ссылка на панель прибора учета';
COMMENT ON COLUMN public.rul_node_panel.parameter_id IS 'Ссылка на измеряемый параметр';

-- ======================================================================

-- Таблица: public.rul_node_panel_argument
CREATE TABLE public.rul_node_panel_argument (
    node_panel_argument_id bigint DEFAULT nextval('rul_node_panel_argument_node_panel_argument_id_seq'::regclass) NOT NULL,
    node_panel_id bigint,
    argument_formula_id bigint,
    conversion_factor numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    accounting_type_node_id bigint
    ,
    CONSTRAINT rul_node_panel_argument_pkey PRIMARY KEY (node_panel_argument_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id),
    CONSTRAINT fk_node_panel_id FOREIGN KEY (node_panel_id) REFERENCES rul_node_panel(node_panel_id)
);
COMMENT ON COLUMN public.rul_node_panel_argument.accounting_type_node_id IS 'Ссылка на конкретный способ учета в узле';
COMMENT ON COLUMN public.rul_node_panel_argument.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_node_panel_argument.conversion_factor IS 'переводной коэффициент';
COMMENT ON COLUMN public.rul_node_panel_argument.node_panel_id IS 'Ссылка на параметр измеряемый в узле';

-- ======================================================================

-- Таблица: public.rul_node_panel_value
CREATE TABLE public.rul_node_panel_value (
    node_panel_value_id bigint DEFAULT nextval('rul_node_panel_value_node_panel_value_id_seq'::regclass) NOT NULL,
    value_number numeric,
    check_date timestamp without time zone NOT NULL,
    check_type_id bigint,
    node_panel_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    is_correct numeric(1,0) DEFAULT 0 NOT NULL,
    changed_user_id bigint
    ,
    CONSTRAINT rul_meter_value_pkey_tmp PRIMARY KEY (node_panel_value_id, check_date),
    CONSTRAINT fk_changed_user_id FOREIGN KEY (changed_user_id) REFERENCES rul_user(user_id),
    CONSTRAINT fk_check_type_id FOREIGN KEY (check_type_id) REFERENCES rul_check_type(check_type_id),
    CONSTRAINT fk_node_panel_id FOREIGN KEY (node_panel_id) REFERENCES rul_node_panel(node_panel_id)
);
COMMENT ON COLUMN public.rul_node_panel_value.changed_user_id IS 'Пользователь добавивший изменения';
COMMENT ON COLUMN public.rul_node_panel_value.check_date IS 'Дата получения показания';
COMMENT ON COLUMN public.rul_node_panel_value.check_type_id IS 'Сслыка на способ снятия показания';
COMMENT ON COLUMN public.rul_node_panel_value.is_correct IS 'Флаг корректности';
COMMENT ON COLUMN public.rul_node_panel_value.node_panel_id IS 'Ссылка параметр измеряемый панелью в узле';
COMMENT ON COLUMN public.rul_node_panel_value.value_number IS 'Показание прибора учета';

-- ======================================================================

-- Таблица: public.rul_node_type
CREATE TABLE public.rul_node_type (
    node_type_id bigint DEFAULT nextval('rul_node_type_node_type_id_seq'::regclass) NOT NULL,
    node_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_node_type_pkey PRIMARY KEY (node_type_id)
);

-- ======================================================================

-- Таблица: public.rul_object
CREATE TABLE public.rul_object (
    object_id bigint DEFAULT nextval('rul_object_object_id_seq'::regclass) NOT NULL,
    object_type_id bigint,
    object_name character varying(256),
    street_id bigint,
    building character varying(128),
    housing character varying(128),
    room character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    locality_id bigint,
    district_id bigint,
    region_id bigint,
    latitude numeric(9,6),
    longitude numeric(9,6)
    ,
    CONSTRAINT rul_object_pkey PRIMARY KEY (object_id),
    CONSTRAINT fk_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_object_type_id FOREIGN KEY (object_type_id) REFERENCES rul_object_type(object_type_id),
    CONSTRAINT fk_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id),
    CONSTRAINT fk_street_id FOREIGN KEY (street_id) REFERENCES rul_street(street_id)
);
COMMENT ON COLUMN public.rul_object.building IS 'Номер здания';
COMMENT ON COLUMN public.rul_object.housing IS 'Номер корпуса';
COMMENT ON COLUMN public.rul_object.latitude IS 'Широта';
COMMENT ON COLUMN public.rul_object.longitude IS 'Долгота';
COMMENT ON COLUMN public.rul_object.object_name IS 'Название объекта';
COMMENT ON COLUMN public.rul_object.object_type_id IS 'Ссылка на тип/назначение объекта';
COMMENT ON COLUMN public.rul_object.room IS 'Номер помещения';
COMMENT ON COLUMN public.rul_object.street_id IS 'Ссылка на улицу(справочник)';

-- ======================================================================

-- Таблица: public.rul_object_type
CREATE TABLE public.rul_object_type (
    object_type_id bigint DEFAULT nextval('rul_object_type_object_type_id_seq'::regclass) NOT NULL,
    client_id bigint,
    object_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_object_type_pkey PRIMARY KEY (object_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);
COMMENT ON COLUMN public.rul_object_type.client_id IS 'Сслыка на клиента';
COMMENT ON COLUMN public.rul_object_type.object_type_name IS 'Название типа/назначения объекта';

-- ======================================================================

-- Таблица: public.rul_observation
CREATE TABLE public.rul_observation (
    observation_id bigint DEFAULT nextval('rul_observation_observation_id_seq'::regclass) NOT NULL,
    client_id bigint,
    locality_id bigint,
    observation_period_id bigint,
    observation_type_id bigint,
    observation_date timestamp without time zone,
    temperature numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_observation_pkey PRIMARY KEY (observation_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_observation_period_id FOREIGN KEY (observation_period_id) REFERENCES rul_observation_period(observation_period_id),
    CONSTRAINT observation_type_id FOREIGN KEY (observation_type_id) REFERENCES rul_observation_type(observation_type_id)
);

-- ======================================================================

-- Таблица: public.rul_observation_period
CREATE TABLE public.rul_observation_period (
    observation_period_id bigint DEFAULT nextval('rul_observation_period_observation_period_id_seq'::regclass) NOT NULL,
    observation_period_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_observation_period_pkey PRIMARY KEY (observation_period_id)
);

-- ======================================================================

-- Таблица: public.rul_observation_type
CREATE TABLE public.rul_observation_type (
    observation_type_id bigint DEFAULT nextval('rul_observation_type_observation_type_id_seq'::regclass) NOT NULL,
    observation_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_observation_type_pkey PRIMARY KEY (observation_type_id)
);

-- ======================================================================

-- Таблица: public.rul_operation
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
COMMENT ON COLUMN public.rul_operation.operation_date IS 'Дата операции';
COMMENT ON COLUMN public.rul_operation.operation_template_id IS 'Ссылка на шаблон операции';

-- ======================================================================

-- Таблица: public.rul_operation_template
CREATE TABLE public.rul_operation_template (
    operation_template_id bigint DEFAULT nextval('rul_operation_template_operation_template_id_seq'::regclass) NOT NULL,
    operation_template_name character varying(128),
    code character varying(64),
    client_id bigint,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    operation_type_id bigint,
    main_subinvoice character varying(128),
    category_code character varying(10),
    destination_code character varying(10)
    ,
    CONSTRAINT rul_operation_template_pkey PRIMARY KEY (operation_template_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_operation_type FOREIGN KEY (operation_type_id) REFERENCES rul_operation_type(operation_type_id)
);
COMMENT ON COLUMN public.rul_operation_template.category_code IS 'Код категории назначения платежа';
COMMENT ON COLUMN public.rul_operation_template.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_operation_template.code IS 'Код';
COMMENT ON COLUMN public.rul_operation_template.description IS 'Примечание';
COMMENT ON COLUMN public.rul_operation_template.destination_code IS 'Код назначения платежа';
COMMENT ON COLUMN public.rul_operation_template.main_subinvoice IS 'Отслеживаемый счет';
COMMENT ON COLUMN public.rul_operation_template.operation_template_name IS 'Название шаблона операции';
COMMENT ON COLUMN public.rul_operation_template.operation_type_id IS 'Ссылка на тип операции';

-- ======================================================================

-- Таблица: public.rul_operation_type
CREATE TABLE public.rul_operation_type (
    operation_type_id bigint DEFAULT nextval('rul_operation_type_operation_type_id_seq'::regclass) NOT NULL,
    operation_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_operation_type_pkey PRIMARY KEY (operation_type_id)
);
COMMENT ON COLUMN public.rul_operation_type.operation_type_name IS 'Тип операции';

-- ======================================================================

-- Таблица: public.rul_panel
CREATE TABLE public.rul_panel (
    panel_id bigint DEFAULT nextval('rul_panel_panel_id_seq'::regclass) NOT NULL,
    brand_id bigint,
    panel_name character varying(256),
    accuracy_class numeric,
    standard_size numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    indication_type_id bigint
    ,
    CONSTRAINT rul_panel_pkey PRIMARY KEY (panel_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_indication_type FOREIGN KEY (indication_type_id) REFERENCES rul_indication_type(indication_type_id)
);
COMMENT ON COLUMN public.rul_panel.accuracy_class IS 'Класс точности';
COMMENT ON COLUMN public.rul_panel.brand_id IS 'Сслыка на марку производетеля';
COMMENT ON COLUMN public.rul_panel.indication_type_id IS 'Ссылка на тип показания(Справочная т.к. на рассчеты не влияет)';
COMMENT ON COLUMN public.rul_panel.panel_name IS 'Название панели прибора учета (счетчика)';
COMMENT ON COLUMN public.rul_panel.standard_size IS 'Типоразмер';

-- ======================================================================

-- Таблица: public.rul_panel_parameter
CREATE TABLE public.rul_panel_parameter (
    panel_parameter_id bigint DEFAULT nextval('rul_panel_parameter_panel_parameter_id_seq'::regclass) NOT NULL,
    panel_id bigint,
    parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_panel_parameter_pkey PRIMARY KEY (panel_parameter_id),
    CONSTRAINT fk_panel_id FOREIGN KEY (panel_id) REFERENCES rul_panel(panel_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);
COMMENT ON COLUMN public.rul_panel_parameter.panel_id IS 'Ссылка на панель бренда';
COMMENT ON COLUMN public.rul_panel_parameter.parameter_id IS 'Ссылка на параметр';

-- ======================================================================

-- Таблица: public.rul_parameter
CREATE TABLE public.rul_parameter (
    parameter_id bigint DEFAULT nextval('rul_parameter_parameter_id_seq'::regclass) NOT NULL,
    unit_id bigint,
    parameter_type_id bigint,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    parameter_name character varying(256),
    in_calculate_parameters smallint DEFAULT 0
    ,
    CONSTRAINT rul_parameter_pkey PRIMARY KEY (parameter_id),
    CONSTRAINT fk_parameter_type_id FOREIGN KEY (parameter_type_id) REFERENCES rul_parameter_type(parameter_type_id),
    CONSTRAINT fk_unit_id FOREIGN KEY (unit_id) REFERENCES rul_unit(unit_id)
);
COMMENT ON COLUMN public.rul_parameter.description IS 'Описание параметра';
COMMENT ON COLUMN public.rul_parameter.parameter_type_id IS 'Ссылка на тип параметра';
COMMENT ON COLUMN public.rul_parameter.unit_id IS 'Ссылка на единицу измерения';

-- ======================================================================

-- Таблица: public.rul_parameter_service_type
CREATE TABLE public.rul_parameter_service_type (
    parameter_service_type_id bigint DEFAULT nextval('rul_parameter_service_type_parameter_service_type_id_seq'::regclass) NOT NULL,
    parameter_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_parameter_service_type_pkey PRIMARY KEY (parameter_service_type_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);
COMMENT ON COLUMN public.rul_parameter_service_type.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_parameter_service_type.service_type_id IS 'Ссылка на вид услуги';

-- ======================================================================

-- Таблица: public.rul_parameter_target_use
CREATE TABLE public.rul_parameter_target_use (
    parameter_target_use_id bigint DEFAULT nextval('rul_parameter_target_use_parameter_target_use_id_seq'::regclass) NOT NULL,
    parameter_id bigint,
    target_use_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_parameter_target_use_pkey PRIMARY KEY (parameter_target_use_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_target_use_id FOREIGN KEY (target_use_id) REFERENCES rul_target_use(target_use_id)
);
COMMENT ON COLUMN public.rul_parameter_target_use.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_parameter_target_use.target_use_id IS 'Ссылка на назначение';

-- ======================================================================

-- Таблица: public.rul_parameter_type
CREATE TABLE public.rul_parameter_type (
    parameter_type_id bigint DEFAULT nextval('rul_parameter_type_parameter_type_id_seq'::regclass) NOT NULL,
    parameter_type_name character varying(256),
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_parameter_type_pkey PRIMARY KEY (parameter_type_id)
);
COMMENT ON COLUMN public.rul_parameter_type.description IS 'Описание типа параметра';
COMMENT ON COLUMN public.rul_parameter_type.parameter_type_name IS 'Название типа параметра';

-- ======================================================================

-- Таблица: public.rul_payment_mechanism
CREATE TABLE public.rul_payment_mechanism (
    payment_mechanism_id bigint DEFAULT nextval('rul_payment_mechanism_payment_mechanism_id_seq'::regclass) NOT NULL,
    payment_mechanism_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_payment_mechanism_pkey PRIMARY KEY (payment_mechanism_id)
);
COMMENT ON COLUMN public.rul_payment_mechanism.payment_mechanism_name IS 'Платежный механизм';

-- ======================================================================

-- Таблица: public.rul_penalty
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
COMMENT ON COLUMN public.rul_penalty.amount IS 'Расчетная сумма (что это?)';
COMMENT ON COLUMN public.rul_penalty.end_date IS 'Дата завершения штрафного периода';
COMMENT ON COLUMN public.rul_penalty.invoice_id IS 'Ссылка на счет куда включено';
COMMENT ON COLUMN public.rul_penalty.is_clone IS 'Показывает, что пеня созданна в виде клона с инвертированной суммой';
COMMENT ON COLUMN public.rul_penalty.penalty IS 'Ставка по пене (Наследуется из договора)';
COMMENT ON COLUMN public.rul_penalty.penalty_nds_value IS 'Размер ндс пени (деньги)';
COMMENT ON COLUMN public.rul_penalty.penalty_type_id IS 'Тип пени';
COMMENT ON COLUMN public.rul_penalty.penalty_value IS 'Размер пени';
COMMENT ON COLUMN public.rul_penalty.source_invoice_id IS 'Ссылка на счет на основе которого рассчитана пеня';
COMMENT ON COLUMN public.rul_penalty.start_date IS 'Дата начала штрафного периода';
COMMENT ON COLUMN public.rul_penalty.transaction_transaction_id IS 'Ссылка на основание выставления пени';

-- ======================================================================

-- Таблица: public.rul_penalty_type
CREATE TABLE public.rul_penalty_type (
    penalty_type_id bigint DEFAULT nextval('rul_penalty_type_penalty_type_id_seq'::regclass) NOT NULL,
    penalty_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_penalty_type_pkey PRIMARY KEY (penalty_type_id)
);
COMMENT ON COLUMN public.rul_penalty_type.penalty_type_name IS 'Тип пени';

-- ======================================================================

-- Таблица: public.rul_pipe_value
CREATE TABLE public.rul_pipe_value (
    pipe_value_id bigint DEFAULT nextval('rul_pipe_value_pipe_value_id_seq'::regclass) NOT NULL,
    value numeric,
    argument_formula_id bigint,
    accounting_type_node_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_pipe_value_pkey PRIMARY KEY (pipe_value_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id),
    CONSTRAINT fk_argument_formula_id FOREIGN KEY (argument_formula_id) REFERENCES rul_argument_formula(argument_formula_id)
);
COMMENT ON COLUMN public.rul_pipe_value.accounting_type_node_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_pipe_value.argument_formula_id IS 'Ссылка на аргумент формулы';
COMMENT ON COLUMN public.rul_pipe_value.value IS 'Значение аргумента по способу учета "по сечению"';

-- ======================================================================

-- Таблица: public.rul_planned_consumption
CREATE TABLE public.rul_planned_consumption (
    planned_consumption_id bigint DEFAULT nextval('rul_planned_consumption_planned_consumption_id_seq'::regclass) NOT NULL,
    connection_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    planned_consumption_value numeric,
    advance_payment_percent numeric,
    payment_date timestamp without time zone,
    description character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_planned_consumption_pkey PRIMARY KEY (planned_consumption_id),
    CONSTRAINT fk_connection_id FOREIGN KEY (connection_id) REFERENCES rul_connection(connection_id)
);
COMMENT ON COLUMN public.rul_planned_consumption.advance_payment_percent IS 'Плановый авансовый платеж';
COMMENT ON COLUMN public.rul_planned_consumption.connection_id IS 'Ссылка на подключение';
COMMENT ON COLUMN public.rul_planned_consumption.description IS 'Обоснование';
COMMENT ON COLUMN public.rul_planned_consumption.end_date IS 'Дата отчетного месяца';
COMMENT ON COLUMN public.rul_planned_consumption.payment_date IS 'Дата платежа';
COMMENT ON COLUMN public.rul_planned_consumption.planned_consumption_value IS 'Плановое месячное потребление';
COMMENT ON COLUMN public.rul_planned_consumption.start_date IS 'Дата отчетного месяца';

-- ======================================================================

-- Таблица: public.rul_precipitation
CREATE TABLE public.rul_precipitation (
    precipitation_id bigint DEFAULT nextval('rul_precipitation_precipitation_id_seq'::regclass) NOT NULL,
    client_id bigint,
    locality_id bigint,
    precipitation_period_id bigint,
    precipitation_type_id bigint,
    precipitation_date timestamp without time zone,
    level_precipitation numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_precipitation_pkey PRIMARY KEY (precipitation_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_precipitation_period_id FOREIGN KEY (precipitation_period_id) REFERENCES rul_precipitation_period(precipitation_period_id),
    CONSTRAINT precipitation_type_id FOREIGN KEY (precipitation_type_id) REFERENCES rul_precipitation_type(precipitation_type_id)
);
COMMENT ON COLUMN public.rul_precipitation.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_precipitation.level_precipitation IS 'Уровень осадков';
COMMENT ON COLUMN public.rul_precipitation.locality_id IS 'Ссылка на населенный пункт';
COMMENT ON COLUMN public.rul_precipitation.precipitation_date IS 'Дата осадков';
COMMENT ON COLUMN public.rul_precipitation.precipitation_period_id IS 'Ссылка на периода осадков (пока только среднемесячные)';
COMMENT ON COLUMN public.rul_precipitation.precipitation_type_id IS 'Ссылка на вид осадков';

-- ======================================================================

-- Таблица: public.rul_precipitation_period
CREATE TABLE public.rul_precipitation_period (
    precipitation_period_id bigint DEFAULT nextval('rul_precipitation_period_precipitation_period_id_seq'::regclass) NOT NULL,
    precipitation_period_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_precipitation_period_pkey PRIMARY KEY (precipitation_period_id)
);

-- ======================================================================

-- Таблица: public.rul_precipitation_type
CREATE TABLE public.rul_precipitation_type (
    precipitation_type_id bigint DEFAULT nextval('rul_precipitation_type_precipitation_type_id_seq'::regclass) NOT NULL,
    precipitation_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_precipitation_type_pkey PRIMARY KEY (precipitation_type_id)
);

-- ======================================================================

-- Таблица: public.rul_preconsumption
CREATE TABLE public.rul_preconsumption (
    accounting_type_node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    consumption numeric,
    node_panel_id bigint,
    node_panel_argument_id bigint,
    value_number numeric
);
COMMENT ON COLUMN public.rul_preconsumption.value_number IS 'Показание, используется для обоснований';

-- ======================================================================

-- Таблица: public.rul_purpose_consumption
CREATE TABLE public.rul_purpose_consumption (
    purpose_consumption_id bigint DEFAULT nextval('rul_purpose_consumption_purpose_consumption_id_seq'::regclass) NOT NULL,
    purpose_consumption_name character varying(40) NOT NULL,
    client_id bigint,
    description character varying(256) NOT NULL,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_purpose_consumption_pkey PRIMARY KEY (purpose_consumption_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);
COMMENT ON COLUMN public.rul_purpose_consumption.client_id IS 'Ссылка на клиента(контрагента)';
COMMENT ON COLUMN public.rul_purpose_consumption.description IS 'Описание';
COMMENT ON COLUMN public.rul_purpose_consumption.purpose_consumption_name IS 'Наименование административного назначения потребления';

-- ======================================================================

-- Таблица: public.rul_rate
CREATE TABLE public.rul_rate (
    rate_id bigint DEFAULT nextval('rul_rate_rate_id_seq'::regclass) NOT NULL,
    rate_name character varying(256),
    target_use_id bigint,
    service_type_id bigint,
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    comitet_id bigint,
    parameter_id bigint,
    description character varying(10)
    ,
    CONSTRAINT rul_rate_pkey PRIMARY KEY (rate_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_comitet_id FOREIGN KEY (comitet_id) REFERENCES rul_comitet(comitet_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id),
    CONSTRAINT fk_target_use_id FOREIGN KEY (target_use_id) REFERENCES rul_target_use(target_use_id)
);
COMMENT ON COLUMN public.rul_rate.client_id IS 'Ссылка на клиента(контрагента)';
COMMENT ON COLUMN public.rul_rate.comitet_id IS 'Ссылка на облисполком';
COMMENT ON COLUMN public.rul_rate.description IS 'Описание/Обозначение';
COMMENT ON COLUMN public.rul_rate.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_rate.rate_name IS 'Название тарифа';
COMMENT ON COLUMN public.rul_rate.service_type_id IS 'Ссылка на вид услуги';
COMMENT ON COLUMN public.rul_rate.target_use_id IS 'Ссылка на назначение тарифа';

-- ======================================================================

-- Таблица: public.rul_rate_value
CREATE TABLE public.rul_rate_value (
    rate_value_id bigint DEFAULT nextval('rul_rate_value_rate_value_id_seq'::regclass) NOT NULL,
    rate_id bigint,
    base_value numeric,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    comitet_resolution character varying(256),
    currency_rate numeric,
    cost_factor numeric,
    nds numeric DEFAULT 0
    ,
    CONSTRAINT rul_rate_value_pkey PRIMARY KEY (rate_value_id),
    CONSTRAINT fk_rate_id FOREIGN KEY (rate_id) REFERENCES rul_rate(rate_id)
);
COMMENT ON COLUMN public.rul_rate_value.base_value IS 'Базовая цена';
COMMENT ON COLUMN public.rul_rate_value.comitet_resolution IS 'Решение исполкома';
COMMENT ON COLUMN public.rul_rate_value.cost_factor IS 'Удельный вес затрат';
COMMENT ON COLUMN public.rul_rate_value.currency_rate IS 'Курс валюты';
COMMENT ON COLUMN public.rul_rate_value.end_date IS 'Дата завершения действия тарифа';
COMMENT ON COLUMN public.rul_rate_value.nds IS 'НДС';
COMMENT ON COLUMN public.rul_rate_value.rate_id IS 'Ссылка на тариф';
COMMENT ON COLUMN public.rul_rate_value.start_date IS 'Дата начала действия тарифа';

-- ======================================================================

-- Таблица: public.rul_region
CREATE TABLE public.rul_region (
    region_id bigint DEFAULT nextval('rul_region_region_id_seq'::regclass) NOT NULL,
    region_name character varying(2048),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_region_pkey PRIMARY KEY (region_id)
);
COMMENT ON COLUMN public.rul_region.region_name IS 'Название области';

-- ======================================================================

-- Таблица: public.rul_report_entity
CREATE TABLE public.rul_report_entity (
    report_entity_id bigint DEFAULT nextval('rul_report_entity_entity_id_seq'::regclass) NOT NULL,
    report_entity_name character varying(255) NOT NULL,
    system_name character varying(255) NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint
    ,
    CONSTRAINT rul_report_entity_pkey PRIMARY KEY (report_entity_id)
);

-- ======================================================================

-- Таблица: public.rul_report_type
CREATE TABLE public.rul_report_type (
    report_type_id bigint DEFAULT nextval('rul_report_type_report_type_id_seq'::regclass) NOT NULL,
    report_type_name character varying(256) NOT NULL,
    description character varying(1024),
    functional_access_id bigint,
    client_id bigint,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    task_type_id bigint,
    method character varying(256)
    ,
    CONSTRAINT rul_report_type_pkey PRIMARY KEY (report_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_functional_access_id FOREIGN KEY (functional_access_id) REFERENCES rul_functional_access(functional_access_id)
);
COMMENT ON COLUMN public.rul_report_type.client_id IS 'Ссылка на контрагента (если пустое, то для всех)';
COMMENT ON COLUMN public.rul_report_type.description IS 'Описание';
COMMENT ON COLUMN public.rul_report_type.functional_access_id IS 'Ссылка на функциональное право?';
COMMENT ON COLUMN public.rul_report_type.report_type_name IS 'Название отчета (типа отчета)';

-- ======================================================================

-- Таблица: public.rul_resource_balance_attitude
CREATE TABLE public.rul_resource_balance_attitude (
    resource_balance_attitude_id bigint DEFAULT nextval('rul_resource_balance_attitude_resource_balance_attitude_id_seq'::regclass) NOT NULL,
    resource_balance_attitude_name character varying(256)
    ,
    CONSTRAINT rul_resource_balance_attitude_pkey PRIMARY KEY (resource_balance_attitude_id)
);

-- ======================================================================

-- Таблица: public.rul_section
CREATE TABLE public.rul_section (
    section_id bigint DEFAULT nextval('rul_section_section_id_seq'::regclass) NOT NULL,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    section_name character varying(256),
    installation_date timestamp without time zone,
    comment character varying(256),
    method_tubing_id bigint,
    construction_id bigint,
    line_id bigint
    ,
    CONSTRAINT rul_section_pkey PRIMARY KEY (section_id),
    CONSTRAINT fk_construction_id FOREIGN KEY (construction_id) REFERENCES rul_construction(construction_id),
    CONSTRAINT fk_line_id FOREIGN KEY (line_id) REFERENCES rul_line(line_id),
    CONSTRAINT fk_method_tubing_id FOREIGN KEY (method_tubing_id) REFERENCES rul_method_tubing(method_tubing_id)
);
COMMENT ON COLUMN public.rul_section.comment IS 'Примечание';
COMMENT ON COLUMN public.rul_section.construction_id IS 'Ссылка на конструкцию';
COMMENT ON COLUMN public.rul_section.end_date IS 'Актуален для расчетов по';
COMMENT ON COLUMN public.rul_section.installation_date IS 'Дата монтажа';
COMMENT ON COLUMN public.rul_section.line_id IS 'Ссылка на принадлежность линии';
COMMENT ON COLUMN public.rul_section.method_tubing_id IS 'Ссылка на способ прокладки';
COMMENT ON COLUMN public.rul_section.section_name IS 'Описание участка';
COMMENT ON COLUMN public.rul_section.start_date IS 'Актуален для рассчетов с';

-- ======================================================================

-- Таблица: public.rul_service
CREATE TABLE public.rul_service (
    service_id bigint DEFAULT nextval('rul_service_service_id_seq'::regclass) NOT NULL,
    service_name character varying(64),
    client_id bigint,
    service_code character varying(32),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_service_pkey PRIMARY KEY (service_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);
COMMENT ON COLUMN public.rul_service.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_service.service_code IS 'Код услуги';
COMMENT ON COLUMN public.rul_service.service_name IS 'Название услуги';

-- ======================================================================

-- Таблица: public.rul_service_type
CREATE TABLE public.rul_service_type (
    service_type_id bigint DEFAULT nextval('rul_service_type_service_type_id_seq'::regclass) NOT NULL,
    service_type_name character varying(1024),
    description character varying(1024),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_service_type_pkey PRIMARY KEY (service_type_id)
);
COMMENT ON COLUMN public.rul_service_type.description IS 'Описание энергоресурса';
COMMENT ON COLUMN public.rul_service_type.service_type_name IS 'Энергоресурс(вода/тепло/элекстрическтво и т.д.)';

-- ======================================================================

-- Таблица: public.rul_source
CREATE TABLE public.rul_source (
    source_id bigint DEFAULT nextval('rul_source_source_id_seq'::regclass) NOT NULL,
    source_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_source_pkey PRIMARY KEY (source_id)
);

-- ======================================================================

-- Таблица: public.rul_source_consumption
CREATE TABLE public.rul_source_consumption (
    source_consumption_id bigint DEFAULT nextval('rul_source_consumption_source_consumption_id_seq'::regclass) NOT NULL,
    source_consumption_name character varying(256),
    accounting_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_source_consumption_pkey PRIMARY KEY (source_consumption_id),
    CONSTRAINT fk_accounting_type_id FOREIGN KEY (accounting_type_id) REFERENCES rul_accounting_type(accounting_type_id)
);
COMMENT ON COLUMN public.rul_source_consumption.accounting_type_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_source_consumption.source_consumption_name IS 'Источник данных о расходах';

-- ======================================================================

-- Таблица: public.rul_source_data_transaction
CREATE TABLE public.rul_source_data_transaction (
    source_data_transaction_id bigint DEFAULT nextval('rul_source_data_transaction_source_data_transaction_id_seq'::regclass) NOT NULL,
    source_data_transaction_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_source_data_transaction_pkey PRIMARY KEY (source_data_transaction_id)
);
COMMENT ON COLUMN public.rul_source_data_transaction.source_data_transaction_name IS 'Ссылка на шаблон операции';

-- ======================================================================

-- Таблица: public.rul_standard
CREATE TABLE public.rul_standard (
    standard_id bigint DEFAULT nextval('rul_standard_standard_id_seq'::regclass) NOT NULL,
    comitet_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    client_id bigint,
    formula_id bigint
    ,
    CONSTRAINT rul_standard_pkey PRIMARY KEY (standard_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_comitet_id FOREIGN KEY (comitet_id) REFERENCES rul_comitet(comitet_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);
COMMENT ON COLUMN public.rul_standard.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_standard.comitet_id IS 'Ссылка на облисполком';
COMMENT ON COLUMN public.rul_standard.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_standard.service_type_id IS 'Ссылка на вид услуги(энергоресурс)';

-- ======================================================================

-- Таблица: public.rul_standard_value
CREATE TABLE public.rul_standard_value (
    standard_value_id bigint DEFAULT nextval('rul_standard_value_standard_value_id_seq'::regclass) NOT NULL,
    value numeric,
    comitet_resolution character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    standard_id bigint
    ,
    CONSTRAINT rul_standard__value_pkey PRIMARY KEY (standard_value_id),
    CONSTRAINT fk_standard_id FOREIGN KEY (standard_id) REFERENCES rul_standard(standard_id)
);
COMMENT ON COLUMN public.rul_standard_value.comitet_resolution IS 'Решение облисполкома';
COMMENT ON COLUMN public.rul_standard_value.end_date IS 'Дата завершения действия норматива';
COMMENT ON COLUMN public.rul_standard_value.standard_id IS 'Ссылка на норматив';
COMMENT ON COLUMN public.rul_standard_value.start_date IS 'Дата начала действия норматива';
COMMENT ON COLUMN public.rul_standard_value.value IS 'Значение норматива';

-- ======================================================================

-- Таблица: public.rul_street
CREATE TABLE public.rul_street (
    street_id bigint DEFAULT nextval('rul_street_street_id_seq'::regclass) NOT NULL,
    street_name character varying(2048),
    locality_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_street_pkey PRIMARY KEY (street_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id)
);
COMMENT ON COLUMN public.rul_street.locality_id IS 'Ссыклка на населенный пункт';
COMMENT ON COLUMN public.rul_street.street_name IS 'Название улицы';

-- ======================================================================

-- Таблица: public.rul_subconto_type
CREATE TABLE public.rul_subconto_type (
    subconto_type_id bigint DEFAULT nextval('rul_subconto_type_subconto_type_id_seq'::regclass) NOT NULL,
    subconto_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_subconto_type_pkey PRIMARY KEY (subconto_type_id)
);
COMMENT ON COLUMN public.rul_subconto_type.subconto_type_name IS 'Вид субконто';

-- ======================================================================

-- Таблица: public.rul_system_message
CREATE TABLE public.rul_system_message (
    system_message_code character varying(64),
    system_message_lang character varying(3),
    system_message_text character varying(256)
);
COMMENT ON COLUMN public.rul_system_message.system_message_code IS 'Код системного сообщения';
COMMENT ON COLUMN public.rul_system_message.system_message_lang IS 'Язык сисемного сообщения';
COMMENT ON COLUMN public.rul_system_message.system_message_text IS 'Текст системного сообщения';

-- ======================================================================

-- Таблица: public.rul_target_use
CREATE TABLE public.rul_target_use (
    target_use_id bigint DEFAULT nextval('rul_target_use_target_use_id_seq'::regclass) NOT NULL,
    target_use_name character varying(1024),
    description character varying(1024),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_target_use_pkey PRIMARY KEY (target_use_id)
);
COMMENT ON COLUMN public.rul_target_use.description IS 'Описание назначения';
COMMENT ON COLUMN public.rul_target_use.target_use_name IS 'Назначение энергоресурса';

-- ======================================================================

-- Таблица: public.rul_task
CREATE TABLE public.rul_task (
    task_id bigint DEFAULT nextval('rul_task_task_id_seq'::regclass) NOT NULL,
    task_type_id integer NOT NULL,
    task_status_id integer NOT NULL,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    user_id bigint NOT NULL,
    op_user_id bigint,
    deleted smallint DEFAULT 0 NOT NULL,
    op_date timestamp without time zone,
    create_date timestamp without time zone
    ,
    CONSTRAINT rul_task_pkey PRIMARY KEY (task_id),
    CONSTRAINT rul_task_task_status_id_fkey FOREIGN KEY (task_status_id) REFERENCES rul_task_status(task_status_id),
    CONSTRAINT rul_task_task_type_id_fkey FOREIGN KEY (task_type_id) REFERENCES rul_task_type(task_type_id),
    CONSTRAINT rul_task_user_id_fkey FOREIGN KEY (user_id) REFERENCES rul_user(user_id)
);

COMMENT ON TABLE public.rul_task IS 'Таблица задач';

COMMENT ON COLUMN public.rul_task.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_task.end_date IS 'Дата окончания выполнения задачи';
COMMENT ON COLUMN public.rul_task.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task.start_date IS 'Дата начала выполнения задачи';
COMMENT ON COLUMN public.rul_task.task_id IS 'Идентификатор задачи';
COMMENT ON COLUMN public.rul_task.task_status_id IS 'Идентификатор статуса задачи';
COMMENT ON COLUMN public.rul_task.task_type_id IS 'Идентификатор типа задачи';
COMMENT ON COLUMN public.rul_task.user_id IS 'Идентификатор пользователя, запустившего задачу';

-- ======================================================================

-- Таблица: public.rul_task_detalization
CREATE TABLE public.rul_task_detalization (
    task_detalization_id integer DEFAULT nextval('rul_task_detalization_task_detalization_id_seq'::regclass) NOT NULL,
    task_id integer NOT NULL,
    entity_item_id integer NOT NULL,
    task_detalization_status_id integer NOT NULL,
    status_date timestamp without time zone,
    op_user_id bigint,
    deleted smallint DEFAULT 0 NOT NULL,
    op_date timestamp without time zone
    ,
    CONSTRAINT rul_task_detalization_pkey PRIMARY KEY (task_detalization_id),
    CONSTRAINT rul_task_detalization_task_detalization_status_id_fkey FOREIGN KEY (task_detalization_status_id) REFERENCES rul_task_detalization_status(task_detalization_status_id),
    CONSTRAINT rul_task_detalization_task_id_fkey FOREIGN KEY (task_id) REFERENCES rul_task(task_id)
);

COMMENT ON TABLE public.rul_task_detalization IS 'Таблица детализаций задач';

COMMENT ON COLUMN public.rul_task_detalization.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_task_detalization.entity_item_id IS 'Идентификатор сущности, над которой осуществляется действие';
COMMENT ON COLUMN public.rul_task_detalization.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_detalization.status_date IS 'Дата установки статуса';
COMMENT ON COLUMN public.rul_task_detalization.task_detalization_id IS 'Идентификатор детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization.task_detalization_status_id IS 'Идентификатор статуса детализации';
COMMENT ON COLUMN public.rul_task_detalization.task_id IS 'Идентификатор задачи';

-- ======================================================================

-- Таблица: public.rul_task_detalization_result
CREATE TABLE public.rul_task_detalization_result (
    task_detalization_result_id bigint DEFAULT nextval('rul_task_detalization_result_task_detalization_result_id_seq'::regclass) NOT NULL,
    task_detalization_id bigint NOT NULL,
    result character varying(1024),
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_task_detalization_result_pkey PRIMARY KEY (task_detalization_result_id),
    CONSTRAINT rul_task_detalization_result_task_detalization_id_fkey FOREIGN KEY (task_detalization_id) REFERENCES rul_task_detalization(task_detalization_id)
);

COMMENT ON TABLE public.rul_task_detalization_result IS 'Таблица результата выполнения по детализации';

COMMENT ON COLUMN public.rul_task_detalization_result.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_task_detalization_result.op_date IS 'Дата последней операции';
COMMENT ON COLUMN public.rul_task_detalization_result.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_detalization_result.result IS 'Результат (в общем случае — ошибка, но не обязательно)';
COMMENT ON COLUMN public.rul_task_detalization_result.task_detalization_id IS 'Идентификатор детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization_result.task_detalization_result_id IS 'Идентификатор результата выполнения детализации задачи';

-- ======================================================================

-- Таблица: public.rul_task_detalization_status
CREATE TABLE public.rul_task_detalization_status (
    task_detalization_status_id integer DEFAULT nextval('rul_task_detalization_status_task_detalization_status_id_seq'::regclass) NOT NULL,
    task_detalization_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_task_detalization_status_pkey PRIMARY KEY (task_detalization_status_id)
);

COMMENT ON TABLE public.rul_task_detalization_status IS 'Таблица статусов детализаций задач';

COMMENT ON COLUMN public.rul_task_detalization_status.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_detalization_status.task_detalization_status_id IS 'Идентификатор статуса детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization_status.task_detalization_status_name IS 'Название статуса детализации задачи';

-- ======================================================================

-- Таблица: public.rul_task_file_generate
CREATE TABLE public.rul_task_file_generate (
    task_file_generate_id integer DEFAULT nextval('rul_task_file_generate_task_file_generate_id_seq'::regclass) NOT NULL,
    task_id bigint NOT NULL,
    file_id bigint NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_task_file_generate_pkey PRIMARY KEY (task_file_generate_id),
    CONSTRAINT rul_task_file_generate_file_id_fkey FOREIGN KEY (file_id) REFERENCES rul_file(file_id),
    CONSTRAINT rul_task_file_generate_task_id_fkey FOREIGN KEY (task_id) REFERENCES rul_task(task_id)
);

COMMENT ON TABLE public.rul_task_file_generate IS 'Таблица сгенерированных архивов счет-фактур';

COMMENT ON COLUMN public.rul_task_file_generate.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_task_file_generate.file_id IS 'Идентификатор файла в хранилище';
COMMENT ON COLUMN public.rul_task_file_generate.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_file_generate.task_file_generate_id IS 'Идентификатор сгенерированного архива счет-фактур';
COMMENT ON COLUMN public.rul_task_file_generate.task_id IS 'Идентификатор задачи';

-- ======================================================================

-- Таблица: public.rul_task_filter
CREATE TABLE public.rul_task_filter (
    task_filter_id bigint DEFAULT nextval('rul_task_filter_task_filter_id_seq'::regclass) NOT NULL,
    filters json,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    task_id bigint NOT NULL
    ,
    CONSTRAINT rul_task_filter_pkey PRIMARY KEY (task_filter_id)
);
COMMENT ON COLUMN public.rul_task_filter.deleted IS 'признак удаления';
COMMENT ON COLUMN public.rul_task_filter.filters IS 'фильтры в формате json (поле типа json)';
COMMENT ON COLUMN public.rul_task_filter.op_user_id IS 'ид. пользователя, последнее операции';
COMMENT ON COLUMN public.rul_task_filter.task_filter_id IS 'ид. фильтра задачи';

-- ======================================================================

-- Таблица: public.rul_task_result
CREATE TABLE public.rul_task_result (
    task_result_id integer DEFAULT nextval('rul_task_result_task_result_id_seq'::regclass) NOT NULL,
    task_id bigint NOT NULL,
    result text,
    op_user_id bigint,
    deleted smallint DEFAULT 0 NOT NULL,
    op_date timestamp without time zone
    ,
    CONSTRAINT rul_task_result_pkey PRIMARY KEY (task_result_id),
    CONSTRAINT rul_task_result_task_id_fkey FOREIGN KEY (task_id) REFERENCES rul_task(task_id)
);

COMMENT ON TABLE public.rul_task_result IS 'Таблица результатов выполнения задач';

COMMENT ON COLUMN public.rul_task_result.deleted IS 'Признак удаления записи';
COMMENT ON COLUMN public.rul_task_result.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_result.result IS 'Результат (в общем случае — ошибка, но не обязательно)';
COMMENT ON COLUMN public.rul_task_result.task_id IS 'Идентификатор задачи';
COMMENT ON COLUMN public.rul_task_result.task_result_id IS 'Идентификатор результата выполнения задачи';

-- ======================================================================

-- Таблица: public.rul_task_status
CREATE TABLE public.rul_task_status (
    task_status_id integer DEFAULT nextval('rul_task_status_task_status_id_seq'::regclass) NOT NULL,
    task_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_task_status_pkey PRIMARY KEY (task_status_id)
);

COMMENT ON TABLE public.rul_task_status IS 'Таблица статусов задач';

COMMENT ON COLUMN public.rul_task_status.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_status.task_status_id IS 'Идентификатор статуса задачи';
COMMENT ON COLUMN public.rul_task_status.task_status_name IS 'Название статуса задачи';

-- ======================================================================

-- Таблица: public.rul_task_type
CREATE TABLE public.rul_task_type (
    task_type_id integer DEFAULT nextval('rul_task_type_task_type_id_seq'::regclass) NOT NULL,
    task_type_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_task_type_pkey PRIMARY KEY (task_type_id)
);

COMMENT ON TABLE public.rul_task_type IS 'Таблица типов задач';

COMMENT ON COLUMN public.rul_task_type.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_type.task_type_id IS 'Идентификатор типа задачи';
COMMENT ON COLUMN public.rul_task_type.task_type_name IS 'Название типа задачи';

-- ======================================================================

-- Таблица: public.rul_transaction
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
COMMENT ON COLUMN public.rul_transaction.accept_date IS 'Дата подтверждения';
COMMENT ON COLUMN public.rul_transaction.amount IS 'Сумма';
COMMENT ON COLUMN public.rul_transaction.calculated_date IS 'Расчетная дата';
COMMENT ON COLUMN public.rul_transaction.client_id IS 'Поставщик';
COMMENT ON COLUMN public.rul_transaction.code_ay IS 'Код АУ';
COMMENT ON COLUMN public.rul_transaction.content IS 'Содержание';
COMMENT ON COLUMN public.rul_transaction.correlation_transaction_id IS 'Сквозной айди (будем проставлять айди дебетовой проводки при ее размножении на кредит и дебет)';
COMMENT ON COLUMN public.rul_transaction.create_date IS 'Дата регистрации';
COMMENT ON COLUMN public.rul_transaction.credit_subinvoice IS 'Кредитовый счет';
COMMENT ON COLUMN public.rul_transaction.debit_subinvoice IS 'Дебетовый счет';
COMMENT ON COLUMN public.rul_transaction.document IS 'Документ';
COMMENT ON COLUMN public.rul_transaction.external_id IS 'ID внешней системы';
COMMENT ON COLUMN public.rul_transaction.is_debit IS 'Указатель на проводку (1 - дебетовая, 0 - кредитовая)';
COMMENT ON COLUMN public.rul_transaction.is_system IS 'Системная ли проводка';
COMMENT ON COLUMN public.rul_transaction.op_date IS 'Дата изменения';
COMMENT ON COLUMN public.rul_transaction.operation_code IS 'Код операции';
COMMENT ON COLUMN public.rul_transaction.operation_date IS 'Дата операции';
COMMENT ON COLUMN public.rul_transaction.operation_id IS 'Ссылка на операцию';
COMMENT ON COLUMN public.rul_transaction.subconto_type_id IS 'Вид субконто';
COMMENT ON COLUMN public.rul_transaction.transaction_template_id IS 'Ссылка на шаблон проводки';
COMMENT ON COLUMN public.rul_transaction.transaction_type_id IS 'Ссылка на тип проводки';

-- ======================================================================

-- Таблица: public.rul_transaction_reversal
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
COMMENT ON COLUMN public.rul_transaction_reversal.amount IS 'Сумма, возможно лишняя т.к. Сумма сторнирования есть в проводке';
COMMENT ON COLUMN public.rul_transaction_reversal.source_correlation_transaction_id IS 'Ссылка на проводки, источники по correlation_id';
COMMENT ON COLUMN public.rul_transaction_reversal.storn_correlation_transaction_id IS 'Ссылка на те проводки, которые надо сторнировать correlation_id';

-- ======================================================================

-- Таблица: public.rul_transaction_template
CREATE TABLE public.rul_transaction_template (
    transaction_template_id bigint DEFAULT nextval('rul_transaction_template_transaction_template_id_seq'::regclass) NOT NULL,
    operation_template_id bigint,
    debit_subinvoice character varying(128),
    credit_subinvoice character varying(128),
    source_data_transaction_id bigint NOT NULL,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_transaction_template_pkey PRIMARY KEY (transaction_template_id),
    CONSTRAINT fk_operation_template_id FOREIGN KEY (operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_source_data_transaction_id FOREIGN KEY (source_data_transaction_id) REFERENCES rul_source_data_transaction(source_data_transaction_id)
);
COMMENT ON COLUMN public.rul_transaction_template.credit_subinvoice IS 'СчетКредит';
COMMENT ON COLUMN public.rul_transaction_template.debit_subinvoice IS 'СчетДебет';
COMMENT ON COLUMN public.rul_transaction_template.description IS 'Примечание';
COMMENT ON COLUMN public.rul_transaction_template.operation_template_id IS 'Ссылка на шаблон операции';
COMMENT ON COLUMN public.rul_transaction_template.source_data_transaction_id IS 'Источник данных о сумме проводки';

-- ======================================================================

-- Таблица: public.rul_transaction_transaction
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
COMMENT ON COLUMN public.rul_transaction_transaction.amount IS 'Сумма';
COMMENT ON COLUMN public.rul_transaction_transaction.credit_transaction_id IS 'Ссылка на версию кредитной проводки';
COMMENT ON COLUMN public.rul_transaction_transaction.debit_transaction_id IS 'Ссылка на версию дебетовой проводки';

-- ======================================================================

-- Таблица: public.rul_transaction_type
CREATE TABLE public.rul_transaction_type (
    transaction_type_id bigint DEFAULT nextval('rul_transaction_type_transaction_type_id_seq'::regclass) NOT NULL,
    transaction_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_transaction_type_pkey PRIMARY KEY (transaction_type_id)
);
COMMENT ON COLUMN public.rul_transaction_type.transaction_type_name IS 'Тип проводки';

-- ======================================================================

-- Таблица: public.rul_transaction_version
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
COMMENT ON COLUMN public.rul_transaction_version.create_date IS 'Дата когда создана была версия (по ней актуальность определяется)';
COMMENT ON COLUMN public.rul_transaction_version.is_actual IS 'Храним знак актуальности';
COMMENT ON COLUMN public.rul_transaction_version.month IS 'Месяц для которого новая версия';
COMMENT ON COLUMN public.rul_transaction_version.payment_percent IS 'Процент погашения проводки на начало месяца';
COMMENT ON COLUMN public.rul_transaction_version.transaction_id IS 'Ссылка на проводку';
COMMENT ON COLUMN public.rul_transaction_version.transaction_reversal_id IS 'Ссылка на сторнирующую проводку из-за которой появилась версия';

-- ======================================================================

-- Таблица: public.rul_unit
CREATE TABLE public.rul_unit (
    unit_id bigint DEFAULT nextval('rul_unit_unit_id_seq'::regclass) NOT NULL,
    unit_name character varying(256),
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_unit_pkey PRIMARY KEY (unit_id)
);
COMMENT ON COLUMN public.rul_unit.description IS 'Описание единицы измерения';
COMMENT ON COLUMN public.rul_unit.unit_name IS 'Название единицы измерения';

-- ======================================================================

-- Таблица: public.rul_unit_conversion
CREATE TABLE public.rul_unit_conversion (
    target_unit_id bigint NOT NULL,
    source_unit_id bigint NOT NULL,
    conversion_factor numeric(12,5) NOT NULL
    ,
    CONSTRAINT fk_rul_unit_conv_source FOREIGN KEY (source_unit_id) REFERENCES rul_unit(unit_id),
    CONSTRAINT fk_rul_unit_conv_target FOREIGN KEY (target_unit_id) REFERENCES rul_unit(unit_id)
);
COMMENT ON COLUMN public.rul_unit_conversion.conversion_factor IS 'Переводной коэффициент';
COMMENT ON COLUMN public.rul_unit_conversion.source_unit_id IS 'Единица измерения, из которой нужно перевести значение по параметру';
COMMENT ON COLUMN public.rul_unit_conversion.target_unit_id IS 'Единица измерения, в которую нужно перевести значение по параметру';

-- ======================================================================

-- Таблица: public.rul_user
CREATE TABLE public.rul_user (
    user_id bigint DEFAULT nextval('rul_user_user_id_seq'::regclass) NOT NULL,
    client_id bigint,
    user_name character varying(256),
    email character varying(256),
    firstname character varying(256),
    surname character varying(256),
    lastname character varying(256),
    phone1 character varying(256),
    password_hash character varying(256),
    password_expire_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    user_type_id bigint,
    position character varying(255),
    department character varying(255),
    date_create timestamp without time zone,
    user_status_id bigint,
    digital_signature_token character varying(64)
    ,
    CONSTRAINT rul_user_pkey PRIMARY KEY (user_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_user_status_id FOREIGN KEY (user_status_id) REFERENCES rul_user_status(user_status_id),
    CONSTRAINT fk_user_type_id FOREIGN KEY (user_type_id) REFERENCES rul_user_type(user_type_id)
);
COMMENT ON COLUMN public.rul_user.client_id IS 'Сслыка на клиента';
COMMENT ON COLUMN public.rul_user.department IS 'Подразделение';
COMMENT ON COLUMN public.rul_user.firstname IS 'Имя пользователя';
COMMENT ON COLUMN public.rul_user.lastname IS 'Отчество пользователя';
COMMENT ON COLUMN public.rul_user.password_expire_date IS 'Срок действия пароля';
COMMENT ON COLUMN public.rul_user.password_hash IS 'Хэш пароля';
COMMENT ON COLUMN public.rul_user.phone1 IS 'Телефон пользователя';
COMMENT ON COLUMN public.rul_user.position IS 'Должность';
COMMENT ON COLUMN public.rul_user.surname IS 'Фамилия пользователя';
COMMENT ON COLUMN public.rul_user.user_name IS 'Логин пользователя';
COMMENT ON COLUMN public.rul_user.user_type_id IS 'Сслыка на тип пользователя';

-- ======================================================================

-- Таблица: public.rul_user_status
CREATE TABLE public.rul_user_status (
    user_status_id bigint DEFAULT nextval('rul_user_status_user_status_id_seq'::regclass) NOT NULL,
    user_status_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_user_status_pkey PRIMARY KEY (user_status_id)
);

-- ======================================================================

-- Таблица: public.rul_user_type
CREATE TABLE public.rul_user_type (
    user_type_id bigint DEFAULT nextval('rul_user_type_user_type_id_seq'::regclass) NOT NULL,
    user_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_user_type_pkey PRIMARY KEY (user_type_id)
);

-- ======================================================================

-- Таблица: public.rul_user_type_functional_access
CREATE TABLE public.rul_user_type_functional_access (
    user_type_functional_access_id bigint DEFAULT nextval('rul_user_type_functional_acce_user_type_functional_access_i_seq'::regclass) NOT NULL,
    functional_access_id bigint,
    user_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_user_type_functional_access_pkey PRIMARY KEY (user_type_functional_access_id),
    CONSTRAINT fk_functional_access_id FOREIGN KEY (functional_access_id) REFERENCES rul_functional_access(functional_access_id),
    CONSTRAINT fk_user_type_id FOREIGN KEY (user_type_id) REFERENCES rul_user_type(user_type_id)
);

COMMENT ON TABLE public.rul_user_type_functional_access IS 'Маппинг для функциональных типов доступов  и юзеров';

COMMENT ON COLUMN public.rul_user_type_functional_access.deleted IS 'Удален или нет';
COMMENT ON COLUMN public.rul_user_type_functional_access.functional_access_id IS 'Ссылка на тип функциональный доступ';
COMMENT ON COLUMN public.rul_user_type_functional_access.op_date IS 'Дата совершения действия';
COMMENT ON COLUMN public.rul_user_type_functional_access.op_user_id IS 'Ссылка на таблицу пользователи';
COMMENT ON COLUMN public.rul_user_type_functional_access.user_type_id IS 'Ссылка на тип доступа юзера';

-- ======================================================================

-- Таблица: public.rul_version_constant
CREATE TABLE public.rul_version_constant (
    version_constant_id bigint DEFAULT nextval('rul_version_constant_version_constant_id_seq'::regclass) NOT NULL,
    formula_id bigint,
    description character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_version_constant_pkey PRIMARY KEY (version_constant_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id)
);
COMMENT ON COLUMN public.rul_version_constant.description IS 'Обоснование';
COMMENT ON COLUMN public.rul_version_constant.end_date IS 'Дата действия по';
COMMENT ON COLUMN public.rul_version_constant.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_version_constant.start_date IS 'Дата действия с';

-- ======================================================================

-- Таблица: public.rul_version_load_standard
CREATE TABLE public.rul_version_load_standard (
    version_load_standard_id bigint DEFAULT nextval('rul_version_load_standard_version_load_standard_id_seq'::regclass) NOT NULL,
    formula_connection_id bigint,
    description character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_version_load_standard_pkey PRIMARY KEY (version_load_standard_id),
    CONSTRAINT fk_formula_connection_id FOREIGN KEY (formula_connection_id) REFERENCES rul_formula_connection(formula_connection_id)
);
COMMENT ON COLUMN public.rul_version_load_standard.description IS 'Обоснование';
COMMENT ON COLUMN public.rul_version_load_standard.end_date IS 'Дата действия по';
COMMENT ON COLUMN public.rul_version_load_standard.formula_connection_id IS 'Ссылка на связь подключения и формулы';
COMMENT ON COLUMN public.rul_version_load_standard.start_date IS 'Дата действия с';

-- ======================================================================

-- Таблица: public.rul_version_specific_load
CREATE TABLE public.rul_version_specific_load (
    version_specific_load_id bigint DEFAULT nextval('rul_version_specific_load_version_specific_load_id_seq'::regclass) NOT NULL,
    version_load_standard_id bigint,
    value numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_version_specific_load_pkey PRIMARY KEY (version_specific_load_id),
    CONSTRAINT fk_version_load_standard_id FOREIGN KEY (version_load_standard_id) REFERENCES rul_version_load_standard(version_load_standard_id)
);
COMMENT ON COLUMN public.rul_version_specific_load.value IS 'Предельная удельная нагрузка';
COMMENT ON COLUMN public.rul_version_specific_load.version_load_standard_id IS 'Сслыка на версию нагрузки';

-- ======================================================================

-- Таблица: public.temp_rul_node_panel_value
CREATE TABLE public.temp_rul_node_panel_value (
    node_panel_value_id bigint,
    value_number numeric,
    check_date timestamp without time zone,
    check_type_id bigint,
    node_panel_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint,
    is_correct numeric(1,0),
    changed_user_id bigint
);

-- ======================================================================

-- Таблица: public.test
CREATE TABLE public.test (
    a character varying(2048)
);
