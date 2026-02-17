CREATE TABLE public.rul_client_group (
    client_group_id bigint DEFAULT nextval('rul_client_group_client_group_id_seq'::regclass) NOT NULL,
    client_group_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    bank_subject_status character varying(16)
    ,
    CONSTRAINT rul_client_group_pkey PRIMARY KEY (client_group_id)
);

COMMENT ON COLUMN public.rul_client_group.client_group_id IS 'Айди группы клиентов';
COMMENT ON COLUMN public.rul_client_group.client_group_name IS 'Название группы клиентов';
COMMENT ON COLUMN public.rul_client_group.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_client_group.deleted IS 'Признак удаления записи';
