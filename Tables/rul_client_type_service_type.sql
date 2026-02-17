CREATE TABLE public.rul_client_type_service_type (
    client_type_service_type_id bigint DEFAULT nextval('rul_client_type_service_type_client_type_service_type_id_seq'::regclass) NOT NULL,
    service_type_id bigint,
    client_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_type_service_type_pkey PRIMARY KEY (client_type_service_type_id),
    CONSTRAINT fk_client_type_id FOREIGN KEY (client_type_id) REFERENCES rul_client_type(client_type_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON TABLE public.rul_client_type_service_type IS 'Таблица, которая показывает какие виды улгуг могут быть доступны определенным типам контрагентов';

COMMENT ON COLUMN public.rul_client_type_service_type.service_type_id IS 'Сслыка на Вид услуги';
COMMENT ON COLUMN public.rul_client_type_service_type.client_type_id IS 'Ссылка на Тип контрагента';
