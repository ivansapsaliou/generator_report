CREATE TABLE public.rul_group_consumption (
    group_consumption_id bigint DEFAULT nextval('rul_group_consumption_group_consumption_id_seq'::regclass) NOT NULL,
    group_consumption_name character varying(256) NOT NULL,
    client_id bigint,
    description character varying(256) NOT NULL,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_group_consumption_pkey PRIMARY KEY (group_consumption_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);

COMMENT ON COLUMN public.rul_group_consumption.group_consumption_name IS 'Наименование группы назначений потребления';
COMMENT ON COLUMN public.rul_group_consumption.client_id IS 'Ссылка на контрагента';
COMMENT ON COLUMN public.rul_group_consumption.description IS 'Описание';
