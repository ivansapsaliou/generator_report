CREATE TABLE public.rul_argument_type (
    argument_type_id bigint DEFAULT nextval('rul_argument_type_argument_type_id_seq'::regclass) NOT NULL,
    argument_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_argument_type_pkey PRIMARY KEY (argument_type_id)
);

COMMENT ON COLUMN public.rul_argument_type.argument_type_name IS 'Название типа аргумента';
