CREATE TABLE public.rul_rate (
    rate_id bigint DEFAULT nextval('rul_rate_rate_id_seq'::regclass) NOT NULL,
    rate_name character varying(256),
    target_use_id bigint,
    service_type_id bigint,
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    comitet_id bigint,
    parameter_id bigint,
    description character varying(10)
    ,
    CONSTRAINT rul_rate_pkey PRIMARY KEY (rate_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_comitet_id FOREIGN KEY (comitet_id) REFERENCES rul_comitet(comitet_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id),
    CONSTRAINT fk_target_use_id FOREIGN KEY (target_use_id) REFERENCES rul_target_use(target_use_id)
);

COMMENT ON COLUMN public.rul_rate.rate_name IS 'Название тарифа';
COMMENT ON COLUMN public.rul_rate.target_use_id IS 'Ссылка на назначение тарифа';
COMMENT ON COLUMN public.rul_rate.service_type_id IS 'Ссылка на вид услуги';
COMMENT ON COLUMN public.rul_rate.client_id IS 'Ссылка на клиента(контрагента)';
COMMENT ON COLUMN public.rul_rate.comitet_id IS 'Ссылка на облисполком';
COMMENT ON COLUMN public.rul_rate.parameter_id IS 'Ссылка на параметр';
COMMENT ON COLUMN public.rul_rate.description IS 'Описание/Обозначение';
