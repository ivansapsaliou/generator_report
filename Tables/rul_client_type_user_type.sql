CREATE TABLE public.rul_client_type_user_type (
    client_type_user_type_id bigint DEFAULT nextval('rul_client_type_user_type_client_type_user_type_id_seq'::regclass) NOT NULL,
    client_type_id bigint,
    user_type_id bigint,
    position smallint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_type_user_type_pkey PRIMARY KEY (client_type_user_type_id),
    CONSTRAINT fk_functional_access_id FOREIGN KEY (client_type_id) REFERENCES rul_functional_access(functional_access_id),
    CONSTRAINT fk_user_type_id FOREIGN KEY (user_type_id) REFERENCES rul_user_type(user_type_id)
);

COMMENT ON TABLE public.rul_client_type_user_type IS 'Маппинг для типов клиентов и типов юзеров';

COMMENT ON COLUMN public.rul_client_type_user_type.client_type_id IS 'Ссылка на тип клиента';
COMMENT ON COLUMN public.rul_client_type_user_type.user_type_id IS 'Ссылка на тип доступа юзера';
COMMENT ON COLUMN public.rul_client_type_user_type.position IS 'Позиция для фронта';
COMMENT ON COLUMN public.rul_client_type_user_type.op_user_id IS 'Ссылка на таблицу пользователи';
COMMENT ON COLUMN public.rul_client_type_user_type.op_date IS 'Дата совершения действия';
COMMENT ON COLUMN public.rul_client_type_user_type.deleted IS 'Удален или нет';
