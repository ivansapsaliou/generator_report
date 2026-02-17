CREATE TABLE public.rul_agreement_service_type (
    agreement_service_type_id bigint DEFAULT nextval('rul_agreement_service_type_agreement_service_type_id_seq'::regclass) NOT NULL,
    agreement_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_agreement_service_type_pkey PRIMARY KEY (agreement_service_type_id),
    CONSTRAINT fk_agreement_id FOREIGN KEY (agreement_id) REFERENCES rul_agreement(agreement_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON COLUMN public.rul_agreement_service_type.agreement_id IS 'Ссылка на договор';
COMMENT ON COLUMN public.rul_agreement_service_type.service_type_id IS 'Ссылка на вид услуги(энергоресурс)';
