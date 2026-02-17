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
COMMENT ON COLUMN public.rul_bank_account.start_date IS 'Дата начала действия счета';
COMMENT ON COLUMN public.rul_bank_account.end_date IS 'Дата завершения действия счета';
COMMENT ON COLUMN public.rul_bank_account.is_main IS 'Флаг основной счет или дополнительный';
COMMENT ON COLUMN public.rul_bank_account.bank_name IS 'Наименование банка';
COMMENT ON COLUMN public.rul_bank_account.bank_address IS 'Адресс банка';
COMMENT ON COLUMN public.rul_bank_account.unp IS 'УНП';
COMMENT ON COLUMN public.rul_bank_account.okpo IS 'ОКПО';
COMMENT ON COLUMN public.rul_bank_account.client_id IS 'Ссылка на клиента(контрагента)';
COMMENT ON COLUMN public.rul_bank_account.bank_code IS 'Код банка';
