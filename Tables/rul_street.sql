CREATE TABLE public.rul_street (
    street_id bigint DEFAULT nextval('rul_street_street_id_seq'::regclass) NOT NULL,
    street_name character varying(2048),
    locality_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0
    ,
    CONSTRAINT rul_street_pkey PRIMARY KEY (street_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id)
);

COMMENT ON COLUMN public.rul_street.street_name IS 'Название улицы';
COMMENT ON COLUMN public.rul_street.locality_id IS 'Ссыклка на населенный пункт';
