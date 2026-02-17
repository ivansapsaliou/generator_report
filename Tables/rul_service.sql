CREATE TABLE public.rul_service (
    service_id bigint DEFAULT nextval('rul_service_service_id_seq'::regclass) NOT NULL,
    service_name character varying(64),
    client_id bigint,
    service_code character varying(32),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_service_pkey PRIMARY KEY (service_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);

COMMENT ON COLUMN public.rul_service.service_name IS 'Название услуги';
COMMENT ON COLUMN public.rul_service.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_service.service_code IS 'Код услуги';
