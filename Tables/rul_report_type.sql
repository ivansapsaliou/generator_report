CREATE TABLE public.rul_report_type (
    report_type_id bigint DEFAULT nextval('rul_report_type_report_type_id_seq'::regclass) NOT NULL,
    report_type_name character varying(256) NOT NULL,
    description character varying(1024),
    functional_access_id bigint,
    client_id bigint,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    task_type_id bigint,
    method character varying(256)
    ,
    CONSTRAINT rul_report_type_pkey PRIMARY KEY (report_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_functional_access_id FOREIGN KEY (functional_access_id) REFERENCES rul_functional_access(functional_access_id)
);

COMMENT ON COLUMN public.rul_report_type.report_type_name IS 'Название отчета (типа отчета)';
COMMENT ON COLUMN public.rul_report_type.description IS 'Описание';
COMMENT ON COLUMN public.rul_report_type.functional_access_id IS 'Ссылка на функциональное право?';
COMMENT ON COLUMN public.rul_report_type.client_id IS 'Ссылка на контрагента (если пустое, то для всех)';
