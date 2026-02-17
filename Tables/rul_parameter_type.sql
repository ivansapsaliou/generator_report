CREATE TABLE public.rul_parameter_type (
    parameter_type_id bigint DEFAULT nextval('rul_parameter_type_parameter_type_id_seq'::regclass) NOT NULL,
    parameter_type_name character varying(256),
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_parameter_type_pkey PRIMARY KEY (parameter_type_id)
);

COMMENT ON COLUMN public.rul_parameter_type.parameter_type_name IS 'Название типа параметра';
COMMENT ON COLUMN public.rul_parameter_type.description IS 'Описание типа параметра';
