CREATE TABLE public.rul_check_type (
    check_type_id bigint DEFAULT nextval('rul_check_type_check_type_id_seq'::regclass) NOT NULL,
    check_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    choise smallint DEFAULT 0
    ,
    CONSTRAINT rul_check_type_pkey PRIMARY KEY (check_type_id)
);

COMMENT ON COLUMN public.rul_check_type.check_type_name IS 'Название способа снятия показания';
COMMENT ON COLUMN public.rul_check_type.choise IS '0 - нет, 1 - да';
