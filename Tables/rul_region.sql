CREATE TABLE public.rul_region (
    region_id bigint DEFAULT nextval('rul_region_region_id_seq'::regclass) NOT NULL,
    region_name character varying(2048),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_region_pkey PRIMARY KEY (region_id)
);

COMMENT ON COLUMN public.rul_region.region_name IS 'Название области';
