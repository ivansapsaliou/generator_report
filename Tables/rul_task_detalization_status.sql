CREATE TABLE public.rul_task_detalization_status (
    task_detalization_status_id integer DEFAULT nextval('rul_task_detalization_status_task_detalization_status_id_seq'::regclass) NOT NULL,
    task_detalization_status_name character varying(255) NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_task_detalization_status_pkey PRIMARY KEY (task_detalization_status_id)
);

COMMENT ON TABLE public.rul_task_detalization_status IS 'Таблица статусов детализаций задач';

COMMENT ON COLUMN public.rul_task_detalization_status.task_detalization_status_id IS 'Идентификатор статуса детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization_status.task_detalization_status_name IS 'Название статуса детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization_status.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
