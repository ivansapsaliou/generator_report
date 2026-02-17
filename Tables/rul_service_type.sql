CREATE TABLE public.rul_service_type (
    service_type_id bigint DEFAULT nextval('rul_service_type_service_type_id_seq'::regclass) NOT NULL,
    service_type_name character varying(1024),
    description character varying(1024),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_service_type_pkey PRIMARY KEY (service_type_id)
);

COMMENT ON COLUMN public.rul_service_type.service_type_name IS 'Энергоресурс(вода/тепло/элекстрическтво и т.д.)';
COMMENT ON COLUMN public.rul_service_type.description IS 'Описание энергоресурса';
