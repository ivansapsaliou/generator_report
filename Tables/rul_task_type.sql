CREATE TABLE public.rul_task_type (
    task_type_id integer DEFAULT nextval('rul_task_type_task_type_id_seq'::regclass) NOT NULL,
    task_type_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_task_type_pkey PRIMARY KEY (task_type_id)
);

COMMENT ON TABLE public.rul_task_type IS 'Таблица типов задач';

COMMENT ON COLUMN public.rul_task_type.task_type_id IS 'Идентификатор типа задачи';
COMMENT ON COLUMN public.rul_task_type.task_type_name IS 'Название типа задачи';
COMMENT ON COLUMN public.rul_task_type.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
