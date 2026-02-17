CREATE TABLE public.rul_task_result (
    task_result_id integer DEFAULT nextval('rul_task_result_task_result_id_seq'::regclass) NOT NULL,
    task_id bigint NOT NULL,
    result text,
    op_user_id bigint,
    deleted smallint DEFAULT 0 NOT NULL,
    op_date timestamp without time zone
    ,
    CONSTRAINT rul_task_result_pkey PRIMARY KEY (task_result_id),
    CONSTRAINT rul_task_result_task_id_fkey FOREIGN KEY (task_id) REFERENCES rul_task(task_id)
);

COMMENT ON TABLE public.rul_task_result IS 'Таблица результатов выполнения задач';

COMMENT ON COLUMN public.rul_task_result.task_result_id IS 'Идентификатор результата выполнения задачи';
COMMENT ON COLUMN public.rul_task_result.task_id IS 'Идентификатор задачи';
COMMENT ON COLUMN public.rul_task_result.result IS 'Результат (в общем случае — ошибка, но не обязательно)';
COMMENT ON COLUMN public.rul_task_result.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_result.deleted IS 'Признак удаления записи';
