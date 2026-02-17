CREATE TABLE public.rul_charge_type (
    charge_type_id bigint DEFAULT nextval('rul_charge_type_charge_type_id_seq'::regclass) NOT NULL,
    charge_type_name character varying(2048),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_charge_type_pkey PRIMARY KEY (charge_type_id)
);

COMMENT ON COLUMN public.rul_charge_type.charge_type_name IS 'Названия типа начисления';
