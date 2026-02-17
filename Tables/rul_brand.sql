CREATE TABLE public.rul_brand (
    brand_id bigint DEFAULT nextval('rul_brand_brand_id_seq'::regclass) NOT NULL,
    brand_name character varying(1024),
    brand_holder character varying(256),
    interval bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    diameter character varying(128)
    ,
    CONSTRAINT rul_brand_pkey PRIMARY KEY (brand_id)
);

COMMENT ON COLUMN public.rul_brand.brand_name IS 'Название марки производителя';
COMMENT ON COLUMN public.rul_brand.brand_holder IS 'Владелец/производитель марки';
COMMENT ON COLUMN public.rul_brand.interval IS 'Межповерочный интервал';
COMMENT ON COLUMN public.rul_brand.diameter IS 'Диаметер';
