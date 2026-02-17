CREATE TABLE public.rul_file_matter_type (
    file_matter_type_id bigint DEFAULT nextval('rul_file_matter_type_file_matter_type_id_seq'::regclass) NOT NULL,
    file_matter_type_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_file_content_type_pkey PRIMARY KEY (file_matter_type_id)
);

COMMENT ON TABLE public.rul_file_matter_type IS 'Таблица типов контента в файле';

COMMENT ON COLUMN public.rul_file_matter_type.file_matter_type_id IS 'Идентификатор типа контента в файле';
COMMENT ON COLUMN public.rul_file_matter_type.file_matter_type_name IS 'Название типа контента в файле';
COMMENT ON COLUMN public.rul_file_matter_type.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_file_matter_type.deleted IS 'Признак удаления записи';
