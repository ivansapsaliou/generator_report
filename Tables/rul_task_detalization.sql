CREATE TABLE public.rul_task_detalization (
    task_detalization_id integer DEFAULT nextval('rul_task_detalization_task_detalization_id_seq'::regclass) NOT NULL,
    task_id integer NOT NULL,
    entity_item_id integer NOT NULL,
    task_detalization_status_id integer NOT NULL,
    status_date timestamp without time zone,
    op_user_id bigint,
    deleted smallint DEFAULT 0 NOT NULL,
    op_date timestamp without time zone
    ,
    CONSTRAINT rul_task_detalization_pkey PRIMARY KEY (task_detalization_id),
    CONSTRAINT rul_task_detalization_task_detalization_status_id_fkey FOREIGN KEY (task_detalization_status_id) REFERENCES rul_task_detalization_status(task_detalization_status_id),
    CONSTRAINT rul_task_detalization_task_id_fkey FOREIGN KEY (task_id) REFERENCES rul_task(task_id)
);

COMMENT ON TABLE public.rul_task_detalization IS 'Таблица детализаций задач';

COMMENT ON COLUMN public.rul_task_detalization.task_detalization_id IS 'Идентификатор детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization.task_id IS 'Идентификатор задачи';
COMMENT ON COLUMN public.rul_task_detalization.entity_item_id IS 'Идентификатор сущности, над которой осуществляется действие';
COMMENT ON COLUMN public.rul_task_detalization.task_detalization_status_id IS 'Идентификатор статуса детализации';
COMMENT ON COLUMN public.rul_task_detalization.status_date IS 'Дата установки статуса';
COMMENT ON COLUMN public.rul_task_detalization.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_detalization.deleted IS 'Признак удаления записи';
