CREATE TABLE public.rul_payment_mechanism (
    payment_mechanism_id bigint DEFAULT nextval('rul_payment_mechanism_payment_mechanism_id_seq'::regclass) NOT NULL,
    payment_mechanism_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_payment_mechanism_pkey PRIMARY KEY (payment_mechanism_id)
);

COMMENT ON COLUMN public.rul_payment_mechanism.payment_mechanism_name IS 'Платежный механизм';
