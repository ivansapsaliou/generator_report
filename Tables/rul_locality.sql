CREATE TABLE public.rul_locality (
    locality_id bigint DEFAULT nextval('rul_locality_locality_id_seq'::regclass) NOT NULL,
    locality_name character varying(2048),
    district_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    region_id bigint
    ,
    CONSTRAINT rul_locality_pkey PRIMARY KEY (locality_id),
    CONSTRAINT fk_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id)
);

COMMENT ON COLUMN public.rul_locality.locality_name IS 'Название населенного пункта';
COMMENT ON COLUMN public.rul_locality.district_id IS 'Ссылка на район';
COMMENT ON COLUMN public.rul_locality.region_id IS 'Ссылка на область';
