CREATE TABLE public.rul_unit (
    unit_id bigint DEFAULT nextval('rul_unit_unit_id_seq'::regclass) NOT NULL,
    unit_name character varying(256),
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_unit_pkey PRIMARY KEY (unit_id)
);

COMMENT ON COLUMN public.rul_unit.unit_name IS 'Название единицы измерения';
COMMENT ON COLUMN public.rul_unit.description IS 'Описание единицы измерения';
