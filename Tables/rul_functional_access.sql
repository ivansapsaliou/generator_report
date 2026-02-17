CREATE TABLE public.rul_functional_access (
    functional_access_id bigint DEFAULT nextval('rul_functional_access_functional_access_id_seq'::regclass) NOT NULL,
    functional_access_name character varying(250) NOT NULL,
    system_name character varying(124),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    weight smallint
    ,
    CONSTRAINT rul_functional_access_pkey PRIMARY KEY (functional_access_id)
);

COMMENT ON TABLE public.rul_functional_access IS 'Справочник функциональных ролей';

COMMENT ON COLUMN public.rul_functional_access.functional_access_name IS 'Название роли';
COMMENT ON COLUMN public.rul_functional_access.system_name IS 'Системное имя';
COMMENT ON COLUMN public.rul_functional_access.op_user_id IS 'Ссылка на таблицу пользователи';
COMMENT ON COLUMN public.rul_functional_access.op_date IS 'Дата совершения действия';
COMMENT ON COLUMN public.rul_functional_access.deleted IS 'Удален или нет';
