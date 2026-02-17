CREATE TABLE public.rul_client_type (
    client_type_id bigint DEFAULT nextval('rul_client_type_client_type_id_seq'::regclass) NOT NULL,
    client_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_type_pkey PRIMARY KEY (client_type_id)
);

COMMENT ON TABLE public.rul_client_type IS 'Таблица справочник для типов контрагентов';

COMMENT ON COLUMN public.rul_client_type.client_type_name IS 'Тип контрагента';
