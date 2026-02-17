CREATE TABLE public.rul_task_file_generate (
    task_file_generate_id integer DEFAULT nextval('rul_task_file_generate_task_file_generate_id_seq'::regclass) NOT NULL,
    task_id bigint NOT NULL,
    file_id bigint NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_task_file_generate_pkey PRIMARY KEY (task_file_generate_id),
    CONSTRAINT rul_task_file_generate_file_id_fkey FOREIGN KEY (file_id) REFERENCES rul_file(file_id),
    CONSTRAINT rul_task_file_generate_task_id_fkey FOREIGN KEY (task_id) REFERENCES rul_task(task_id)
);

COMMENT ON TABLE public.rul_task_file_generate IS 'Таблица сгенерированных архивов счет-фактур';

COMMENT ON COLUMN public.rul_task_file_generate.task_file_generate_id IS 'Идентификатор сгенерированного архива счет-фактур';
COMMENT ON COLUMN public.rul_task_file_generate.task_id IS 'Идентификатор задачи';
COMMENT ON COLUMN public.rul_task_file_generate.file_id IS 'Идентификатор файла в хранилище';
COMMENT ON COLUMN public.rul_task_file_generate.op_user_id IS 'Идентификатор пользователя, выполнившего последнюю операцию';
COMMENT ON COLUMN public.rul_task_file_generate.deleted IS 'Признак удаления записи';
