CREATE TABLE public.rul_user_type_functional_access (
    user_type_functional_access_id bigint DEFAULT nextval('rul_user_type_functional_acce_user_type_functional_access_i_seq'::regclass) NOT NULL,
    functional_access_id bigint,
    user_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_user_type_functional_access_pkey PRIMARY KEY (user_type_functional_access_id),
    CONSTRAINT fk_functional_access_id FOREIGN KEY (functional_access_id) REFERENCES rul_functional_access(functional_access_id),
    CONSTRAINT fk_user_type_id FOREIGN KEY (user_type_id) REFERENCES rul_user_type(user_type_id)
);

COMMENT ON TABLE public.rul_user_type_functional_access IS 'Маппинг для функциональных типов доступов  и юзеров';

COMMENT ON COLUMN public.rul_user_type_functional_access.functional_access_id IS 'Ссылка на тип функциональный доступ';
COMMENT ON COLUMN public.rul_user_type_functional_access.user_type_id IS 'Ссылка на тип доступа юзера';
COMMENT ON COLUMN public.rul_user_type_functional_access.op_user_id IS 'Ссылка на таблицу пользователи';
COMMENT ON COLUMN public.rul_user_type_functional_access.op_date IS 'Дата совершения действия';
COMMENT ON COLUMN public.rul_user_type_functional_access.deleted IS 'Удален или нет';
