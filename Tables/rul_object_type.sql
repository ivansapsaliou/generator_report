CREATE TABLE public.rul_object_type (
    object_type_id bigint DEFAULT nextval('rul_object_type_object_type_id_seq'::regclass) NOT NULL,
    client_id bigint,
    object_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_object_type_pkey PRIMARY KEY (object_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);

COMMENT ON COLUMN public.rul_object_type.client_id IS 'Сслыка на клиента';
COMMENT ON COLUMN public.rul_object_type.object_type_name IS 'Название типа/назначения объекта';
