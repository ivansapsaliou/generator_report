CREATE TABLE public.rul_client_client_type (
    client_client_type_id bigint DEFAULT nextval('rul_client_client_type_client_client_type_id_seq'::regclass) NOT NULL,
    client_type_id bigint,
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_client_type_pkey PRIMARY KEY (client_client_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);

COMMENT ON TABLE public.rul_client_client_type IS 'Таблица связка, которая описывает какого типа конрагент';

COMMENT ON COLUMN public.rul_client_client_type.client_type_id IS 'Сслыка на Тип контрагента';
COMMENT ON COLUMN public.rul_client_client_type.client_id IS 'Ссылка на контрагента';
