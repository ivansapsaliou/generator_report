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

COMMENT ON COLUMN public.rul_rate_value.rate_id IS 'Ссылка на тариф';
COMMENT ON COLUMN public.rul_rate_value.base_value IS 'Базовая цена';
COMMENT ON COLUMN public.rul_rate_value.start_date IS 'Дата начала действия тарифа';
COMMENT ON COLUMN public.rul_rate_value.end_date IS 'Дата завершения действия тарифа';
COMMENT ON COLUMN public.rul_rate_value.comitet_resolution IS 'Решение исполкома';
COMMENT ON COLUMN public.rul_rate_value.currency_rate IS 'Курс валюты';
COMMENT ON COLUMN public.rul_rate_value.cost_factor IS 'Удельный вес затрат';
COMMENT ON COLUMN public.rul_rate_value.nds IS 'НДС';
