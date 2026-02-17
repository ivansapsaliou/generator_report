CREATE TABLE public.rul_agreement_type (
    agreement_type_id bigint DEFAULT nextval('rul_agreement_type_agreement_type_id_seq'::regclass) NOT NULL,
    agreement_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_agreement_type_pkey PRIMARY KEY (agreement_type_id)
);

COMMENT ON COLUMN public.rul_agreement_type.agreement_type_name IS 'Вид договора';
