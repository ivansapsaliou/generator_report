CREATE TABLE public.rul_operation_type (
    operation_type_id bigint DEFAULT nextval('rul_operation_type_operation_type_id_seq'::regclass) NOT NULL,
    operation_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_operation_type_pkey PRIMARY KEY (operation_type_id)
);

COMMENT ON COLUMN public.rul_operation_type.operation_type_name IS 'Тип операции';
