CREATE TABLE public.rul_operation_template (
    operation_template_id bigint DEFAULT nextval('rul_operation_template_operation_template_id_seq'::regclass) NOT NULL,
    operation_template_name character varying(128),
    code character varying(64),
    client_id bigint,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    operation_type_id bigint,
    main_subinvoice character varying(128),
    category_code character varying(10),
    destination_code character varying(10)
    ,
    CONSTRAINT rul_operation_template_pkey PRIMARY KEY (operation_template_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_operation_type FOREIGN KEY (operation_type_id) REFERENCES rul_operation_type(operation_type_id)
);

COMMENT ON COLUMN public.rul_operation_template.operation_template_name IS 'Название шаблона операции';
COMMENT ON COLUMN public.rul_operation_template.code IS 'Код';
COMMENT ON COLUMN public.rul_operation_template.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_operation_template.description IS 'Примечание';
COMMENT ON COLUMN public.rul_operation_template.operation_type_id IS 'Ссылка на тип операции';
COMMENT ON COLUMN public.rul_operation_template.main_subinvoice IS 'Отслеживаемый счет';
COMMENT ON COLUMN public.rul_operation_template.category_code IS 'Код категории назначения платежа';
COMMENT ON COLUMN public.rul_operation_template.destination_code IS 'Код назначения платежа';
