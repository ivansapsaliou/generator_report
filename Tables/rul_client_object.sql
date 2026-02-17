CREATE TABLE public.rul_client_object (
    client_object_id bigint DEFAULT nextval('rul_client_object_client_object_id_seq'::regclass) NOT NULL,
    object_id bigint,
    client_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    room character varying(128)
    ,
    CONSTRAINT rul_client_object_pkey PRIMARY KEY (client_object_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES rul_object(object_id)
);

COMMENT ON COLUMN public.rul_client_object.object_id IS 'Ссылка на объекта';
COMMENT ON COLUMN public.rul_client_object.client_id IS 'Сслыка на клиента';
COMMENT ON COLUMN public.rul_client_object.start_date IS 'Начало даты действия';
COMMENT ON COLUMN public.rul_client_object.end_date IS 'Конец даты действия';
COMMENT ON COLUMN public.rul_client_object.room IS 'Арендуемое помещение';
