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
COMMENT ON COLUMN public.rul_currency.currency_name IS 'Название валюты';
COMMENT ON COLUMN public.rul_currency.currency_fract_name IS 'Название копеек (цент и т.д.)';
COMMENT ON COLUMN public.rul_currency.currency_symbol IS 'Символ, которым обозначаются деньги в данной валюте';
