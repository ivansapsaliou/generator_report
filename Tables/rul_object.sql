CREATE TABLE public.rul_object (
    object_id bigint DEFAULT nextval('rul_object_object_id_seq'::regclass) NOT NULL,
    object_type_id bigint,
    object_name character varying(256),
    street_id bigint,
    building character varying(128),
    housing character varying(128),
    room character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    locality_id bigint,
    district_id bigint,
    region_id bigint,
    latitude numeric(9,6),
    longitude numeric(9,6)
    ,
    CONSTRAINT rul_object_pkey PRIMARY KEY (object_id),
    CONSTRAINT fk_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_object_type_id FOREIGN KEY (object_type_id) REFERENCES rul_object_type(object_type_id),
    CONSTRAINT fk_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id),
    CONSTRAINT fk_street_id FOREIGN KEY (street_id) REFERENCES rul_street(street_id)
);

COMMENT ON COLUMN public.rul_object.object_type_id IS 'Ссылка на тип/назначение объекта';
COMMENT ON COLUMN public.rul_object.object_name IS 'Название объекта';
COMMENT ON COLUMN public.rul_object.street_id IS 'Ссылка на улицу(справочник)';
COMMENT ON COLUMN public.rul_object.building IS 'Номер здания';
COMMENT ON COLUMN public.rul_object.housing IS 'Номер корпуса';
COMMENT ON COLUMN public.rul_object.room IS 'Номер помещения';
COMMENT ON COLUMN public.rul_object.latitude IS 'Широта';
COMMENT ON COLUMN public.rul_object.longitude IS 'Долгота';
