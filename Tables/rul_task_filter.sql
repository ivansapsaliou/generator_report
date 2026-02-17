CREATE TABLE public.rul_task_filter (
    task_filter_id bigint DEFAULT nextval('rul_task_filter_task_filter_id_seq'::regclass) NOT NULL,
    filters json,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    task_id bigint NOT NULL
    ,
    CONSTRAINT rul_task_filter_pkey PRIMARY KEY (task_filter_id)
);

COMMENT ON COLUMN public.rul_task_filter.task_filter_id IS 'ид. фильтра задачи';
COMMENT ON COLUMN public.rul_task_filter.filters IS 'фильтры в формате json (поле типа json)';
COMMENT ON COLUMN public.rul_task_filter.op_user_id IS 'ид. пользователя, последнее операции';
COMMENT ON COLUMN public.rul_task_filter.deleted IS 'признак удаления';
