CREATE TABLE public.rul_client_service_type (
    client_service_type_id bigint DEFAULT nextval('rul_client_service_type_client_service_type_id_seq'::regclass) NOT NULL,
    service_type_id bigint,
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_service_type_pkey PRIMARY KEY (client_service_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON TABLE public.rul_client_service_type IS 'Таблица связка которая указывает какие виды услуг выбраны для конкретного контрагента';

COMMENT ON COLUMN public.rul_client_service_type.service_type_id IS 'Сслыка на Вид услуги';
COMMENT ON COLUMN public.rul_client_service_type.client_id IS 'Ссылка на контрагента';
