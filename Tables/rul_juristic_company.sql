CREATE TABLE public.rul_juristic_company (
    juristic_company_id bigint DEFAULT nextval('rul_juristic_company_juristic_company_id_seq'::regclass) NOT NULL,
    fullname character varying(2048),
    shortname character varying(2048),
    street_id bigint,
    building character varying(128),
    housing character varying(128),
    room character varying(128),
    post_index character varying(128),
    stat_code character varying(2048),
    oked character varying(2048),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted integer DEFAULT 0,
    client_id bigint,
    locality_id bigint,
    district_id bigint,
    region_id bigint,
    ogrn character varying(128),
    inn character varying(128),
    unp character varying(128)
    ,
    CONSTRAINT rul_juristic_company_pkey PRIMARY KEY (juristic_company_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_juristic_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_juristic_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_juristic_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id),
    CONSTRAINT fk_juristic_street_id FOREIGN KEY (street_id) REFERENCES rul_street(street_id)
);

COMMENT ON COLUMN public.rul_juristic_company.fullname IS 'Полное название юр. лица';
COMMENT ON COLUMN public.rul_juristic_company.shortname IS 'Короткое название юр.лица / ФИО';
COMMENT ON COLUMN public.rul_juristic_company.street_id IS 'Юр.адресс: Ссылка на улицу(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.building IS 'Юр.адресс: Номер здания';
COMMENT ON COLUMN public.rul_juristic_company.housing IS 'Юр.адресс: Номер корпуса';
COMMENT ON COLUMN public.rul_juristic_company.room IS 'Юр.адресс: Номер помещения';
COMMENT ON COLUMN public.rul_juristic_company.post_index IS 'Юр.адресс: Почтовый индекс';
COMMENT ON COLUMN public.rul_juristic_company.stat_code IS 'Статкод';
COMMENT ON COLUMN public.rul_juristic_company.oked IS 'ОКЭД';
COMMENT ON COLUMN public.rul_juristic_company.start_date IS 'Начало даты действия';
COMMENT ON COLUMN public.rul_juristic_company.end_date IS 'Конец даты действия';
COMMENT ON COLUMN public.rul_juristic_company.locality_id IS 'Юр.адресс: Ссылка на населенный пункт(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.district_id IS 'Юр.адресс: Ссылка на район(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.region_id IS 'Юр.адресс: Ссылка на область(справочник)';
COMMENT ON COLUMN public.rul_juristic_company.ogrn IS 'ОГРН';
COMMENT ON COLUMN public.rul_juristic_company.inn IS 'ИНН';
COMMENT ON COLUMN public.rul_juristic_company.unp IS 'УНП';
