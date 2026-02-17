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

COMMENT ON COLUMN public.rul_charge.connection_id IS 'Ссылка на подключение';
COMMENT ON COLUMN public.rul_charge.sum_consumption IS 'Итоговый расход';
COMMENT ON COLUMN public.rul_charge.base_value IS 'Сам тариф';
COMMENT ON COLUMN public.rul_charge.amount IS 'Итоговая стоимость (без ндс)';
COMMENT ON COLUMN public.rul_charge.nds_percent IS 'НДС, проценты';
COMMENT ON COLUMN public.rul_charge.note IS 'Примечание';
COMMENT ON COLUMN public.rul_charge.start_date IS 'Дата с (расхода)';
COMMENT ON COLUMN public.rul_charge.end_date IS 'Дата по (расход)';
COMMENT ON COLUMN public.rul_charge.billing_start_date IS 'Начало рассчетного периода';
COMMENT ON COLUMN public.rul_charge.billing_end_date IS 'Дата завершения рассчетного периода';
COMMENT ON COLUMN public.rul_charge.charge_type_id IS 'Ссылка на тип начисления';
COMMENT ON COLUMN public.rul_charge.amount_nds IS 'Сумма с НДС';
COMMENT ON COLUMN public.rul_charge.nds_rub IS 'НДС, рубли';
COMMENT ON COLUMN public.rul_charge.charge_checked IS 'Флаг подтверждения начисления';
COMMENT ON COLUMN public.rul_charge.need_recount IS 'Требует пересчета';
COMMENT ON COLUMN public.rul_charge.invoice_id IS 'Ссылка на счет, если он создан.';
COMMENT ON COLUMN public.rul_charge.source_id IS 'Источник того, откуда пришел расход по начислению';
COMMENT ON COLUMN public.rul_charge.currency_rate IS 'Базовый курс (курс валюты)';
COMMENT ON COLUMN public.rul_charge.cost_factor IS 'Удельный вес затрат';
COMMENT ON COLUMN public.rul_charge.invoice_group_index IS 'ИГС (индекс группировки счетов)';
COMMENT ON COLUMN public.rul_charge.balancing_id IS 'Ссылка на то, какой балансировкой исправлено';
COMMENT ON COLUMN public.rul_charge.comitet_resolution IS 'Решение исполкома взятое из тарифа';
