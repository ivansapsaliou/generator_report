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
