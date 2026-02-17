CREATE TABLE public.rul_brand_service_type (
    brand_service_type_id bigint DEFAULT nextval('rul_brand_service_type_brand_service_type_id_seq'::regclass) NOT NULL,
    brand_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_brand_service_type_pkey PRIMARY KEY (brand_service_type_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON COLUMN public.rul_brand_service_type.brand_id IS 'Сслыка с маркой производителя';
COMMENT ON COLUMN public.rul_brand_service_type.service_type_id IS 'Сслыка на тип услуги(энергоресурс)';
