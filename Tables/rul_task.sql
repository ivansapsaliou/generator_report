CREATE TABLE public.rul_task (
    task_id bigint DEFAULT nextval('rul_task_task_id_seq'::regclass) NOT NULL,
    task_type_id integer NOT NULL,
    task_status_id integer NOT NULL,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    user_id bigint NOT NULL,
    op_user_id bigint,
    deleted smallint DEFAULT 0 NOT NULL,
    op_date timestamp without time zone,
    create_date timestamp without time zone
    ,
    CONSTRAINT rul_task_pkey PRIMARY KEY (task_id),
    CONSTRAINT rul_task_task_status_id_fkey FOREIGN KEY (task_status_id) REFERENCES rul_task_status(task_status_id),
    CONSTRAINT rul_task_task_type_id_fkey FOREIGN KEY (task_type_id) REFERENCES rul_task_type(task_type_id),
    CONSTRAINT rul_task_user_id_fkey FOREIGN KEY (user_id) REFERENCES rul_user(user_id)
);

COMMENT ON TABLE public.rul_task IS 'Таблица задач';

COMMENT ON COLUMN public.rul_task.task_id IS 'Идентификатор задачи';
COMMENT ON COLUMN public.rul_task.task_type_id IS 'Идентификатор типа задачи';
COMMENT ON COLUMN public.rul_task.task_status_id IS 'Идентификатор статуса задачи';
COMMENT ON COLUMN public.rul_task.start_date IS 'Дата начала выполнения задачи';
COMMENT ON COLUMN public.rul_task.end_date IS 'Дата окончания выполнения задачи';
COMMENT ON COLUMN public.rul_task.user_id IS 'Идентификатор пользователя, запустившего задачу';
COMMENT ON COLUMN public.rul_task.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task.deleted IS 'Признак удаления записи';
