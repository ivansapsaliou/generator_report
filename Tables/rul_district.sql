CREATE TABLE public.rul_district (
    district_id bigint DEFAULT nextval('rul_district_district_id_seq'::regclass) NOT NULL,
    district_name character varying(2048),
    region_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_district_pkey PRIMARY KEY (district_id),
    CONSTRAINT fk_op_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id)
);

COMMENT ON COLUMN public.rul_district.district_name IS 'Название района';
COMMENT ON COLUMN public.rul_district.region_id IS 'Ссылка на область';
