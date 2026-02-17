CREATE TABLE public.rul_user (
    user_id bigint DEFAULT nextval('rul_user_user_id_seq'::regclass) NOT NULL,
    client_id bigint,
    user_name character varying(256),
    email character varying(256),
    firstname character varying(256),
    surname character varying(256),
    lastname character varying(256),
    phone1 character varying(256),
    password_hash character varying(256),
    password_expire_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    user_type_id bigint,
    position character varying(255),
    department character varying(255),
    date_create timestamp without time zone,
    user_status_id bigint,
    digital_signature_token character varying(64)
    ,
    CONSTRAINT rul_user_pkey PRIMARY KEY (user_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_user_status_id FOREIGN KEY (user_status_id) REFERENCES rul_user_status(user_status_id),
    CONSTRAINT fk_user_type_id FOREIGN KEY (user_type_id) REFERENCES rul_user_type(user_type_id)
);

COMMENT ON COLUMN public.rul_user.client_id IS 'Сслыка на клиента';
COMMENT ON COLUMN public.rul_user.user_name IS 'Логин пользователя';
COMMENT ON COLUMN public.rul_user.firstname IS 'Имя пользователя';
COMMENT ON COLUMN public.rul_user.surname IS 'Фамилия пользователя';
COMMENT ON COLUMN public.rul_user.lastname IS 'Отчество пользователя';
COMMENT ON COLUMN public.rul_user.phone1 IS 'Телефон пользователя';
COMMENT ON COLUMN public.rul_user.password_hash IS 'Хэш пароля';
COMMENT ON COLUMN public.rul_user.password_expire_date IS 'Срок действия пароля';
COMMENT ON COLUMN public.rul_user.user_type_id IS 'Сслыка на тип пользователя';
COMMENT ON COLUMN public.rul_user.position IS 'Должность';
COMMENT ON COLUMN public.rul_user.department IS 'Подразделение';
