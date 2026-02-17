CREATE TABLE public.rul_purpose_consumption (
    purpose_consumption_id bigint DEFAULT nextval('rul_purpose_consumption_purpose_consumption_id_seq'::regclass) NOT NULL,
    purpose_consumption_name character varying(40) NOT NULL,
    client_id bigint,
    description character varying(256) NOT NULL,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_purpose_consumption_pkey PRIMARY KEY (purpose_consumption_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);

COMMENT ON COLUMN public.rul_purpose_consumption.purpose_consumption_name IS 'Наименование административного назначения потребления';
COMMENT ON COLUMN public.rul_purpose_consumption.client_id IS 'Ссылка на клиента(контрагента)';
COMMENT ON COLUMN public.rul_purpose_consumption.description IS 'Описание';
