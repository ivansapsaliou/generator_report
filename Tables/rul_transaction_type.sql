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
