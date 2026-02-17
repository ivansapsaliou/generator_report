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
