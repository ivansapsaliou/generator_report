CREATE TABLE public.rul_indication_type (
    indication_type_id bigint DEFAULT nextval('rul_indication_type_indication_type_id_seq'::regclass) NOT NULL,
    indication_type_name character varying(64),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_indication_type_pkey PRIMARY KEY (indication_type_id)
);

COMMENT ON COLUMN public.rul_indication_type.indication_type_name IS 'Название типа показания';
