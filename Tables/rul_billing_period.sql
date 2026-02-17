CREATE TABLE public.rul_billing_period (
    billing_period_id bigint DEFAULT nextval('rul_billing_period_billing_period_id_seq'::regclass) NOT NULL,
    billing_period_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_billing_period_pkey PRIMARY KEY (billing_period_id)
);
