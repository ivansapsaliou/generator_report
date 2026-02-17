CREATE TABLE public.rul_subconto_type (
    subconto_type_id bigint DEFAULT nextval('rul_subconto_type_subconto_type_id_seq'::regclass) NOT NULL,
    subconto_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_subconto_type_pkey PRIMARY KEY (subconto_type_id)
);

COMMENT ON COLUMN public.rul_subconto_type.subconto_type_name IS 'Вид субконто';
