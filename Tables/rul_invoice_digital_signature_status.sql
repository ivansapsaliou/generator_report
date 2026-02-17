CREATE TABLE public.rul_invoice_digital_signature_status (
    invoice_digital_signature_status_id bigint DEFAULT nextval('rul_invoice_digital_signature_invoice_digital_signature_sta_seq'::regclass) NOT NULL,
    invoice_digital_signature_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_invoice_digital_signature_status_pkey PRIMARY KEY (invoice_digital_signature_status_id)
);
