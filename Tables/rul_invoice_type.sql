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
