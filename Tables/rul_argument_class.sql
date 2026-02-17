CREATE TABLE public.rul_argument_class (
    argument_class_id bigint DEFAULT nextval('rul_argument_class_argument_class_id_seq'::regclass) NOT NULL,
    argument_class_name character varying(2048),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_argument_class_pkey PRIMARY KEY (argument_class_id)
);

COMMENT ON COLUMN public.rul_argument_class.argument_class_name IS 'Названия класса аргумента';
