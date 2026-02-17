CREATE TABLE public.rul_comitet (
    comitet_id bigint DEFAULT nextval('rul_comitet_comitet_id_seq'::regclass) NOT NULL,
    comitet_name character varying(256),
    region_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_comitet_pkey PRIMARY KEY (comitet_id),
    CONSTRAINT fk_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id)
);

COMMENT ON COLUMN public.rul_comitet.comitet_name IS 'Название Облисполкома';
COMMENT ON COLUMN public.rul_comitet.region_id IS 'Ссылка на область';
