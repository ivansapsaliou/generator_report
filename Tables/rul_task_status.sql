CREATE TABLE public.rul_task_status (
    task_status_id integer DEFAULT nextval('rul_task_status_task_status_id_seq'::regclass) NOT NULL,
    task_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_task_status_pkey PRIMARY KEY (task_status_id)
);

COMMENT ON TABLE public.rul_task_status IS 'Таблица статусов задач';

COMMENT ON COLUMN public.rul_task_status.task_status_id IS 'Идентификатор статуса задачи';
COMMENT ON COLUMN public.rul_task_status.task_status_name IS 'Название статуса задачи';
COMMENT ON COLUMN public.rul_task_status.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
