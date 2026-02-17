CREATE TABLE public.rul_currency_rate (
    currency_code character varying(16) NOT NULL,
    currency_rate_date timestamp(0) without time zone NOT NULL,
    currency_rate numeric NOT NULL,
    currency_rate_id bigint DEFAULT nextval('rul_currency_rate_currency_rate_id_seq'::regclass) NOT NULL
    ,
    CONSTRAINT bas_currency_rate_pkey PRIMARY KEY (currency_code, currency_rate_date)
);

COMMENT ON COLUMN public.rul_currency_rate.currency_code IS 'Код валюты';
COMMENT ON COLUMN public.rul_currency_rate.currency_rate_date IS 'Дата курса валюты';
COMMENT ON COLUMN public.rul_currency_rate.currency_rate IS 'Курс валюты';
