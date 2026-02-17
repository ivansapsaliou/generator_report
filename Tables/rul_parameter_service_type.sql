CREATE TABLE public.rul_parameter_service_type (
    parameter_service_type_id bigint DEFAULT nextval('rul_parameter_service_type_parameter_service_type_id_seq'::regclass) NOT NULL,
    parameter_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_parameter_service_type_pkey PRIMARY KEY (parameter_service_type_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON COLUMN public.rul_parameter_service_type.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_parameter_service_type.service_type_id IS 'Ссылка на вид услуги';
