CREATE TABLE public.rul_task_detalization_result (
    task_detalization_result_id bigint DEFAULT nextval('rul_task_detalization_result_task_detalization_result_id_seq'::regclass) NOT NULL,
    task_detalization_id bigint NOT NULL,
    result character varying(1024),
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_task_detalization_result_pkey PRIMARY KEY (task_detalization_result_id),
    CONSTRAINT rul_task_detalization_result_task_detalization_id_fkey FOREIGN KEY (task_detalization_id) REFERENCES rul_task_detalization(task_detalization_id)
);

COMMENT ON TABLE public.rul_task_detalization_result IS 'Таблица результата выполнения по детализации';

COMMENT ON COLUMN public.rul_task_detalization_result.task_detalization_result_id IS 'Идентификатор результата выполнения детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization_result.task_detalization_id IS 'Идентификатор детализации задачи';
COMMENT ON COLUMN public.rul_task_detalization_result.result IS 'Результат (в общем случае — ошибка, но не обязательно)';
COMMENT ON COLUMN public.rul_task_detalization_result.op_date IS 'Дата последней операции';
COMMENT ON COLUMN public.rul_task_detalization_result.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_detalization_result.deleted IS 'Признак удаления записи';
