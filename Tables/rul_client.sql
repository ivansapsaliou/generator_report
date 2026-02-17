CREATE TABLE public.rul_client (
    client_id bigint DEFAULT nextval('rul_client_client_id_seq'::regclass) NOT NULL,
    client_name character varying(2048),
    email character varying(256),
    description character varying(8000),
    is_active numeric(1,0) DEFAULT 1 NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    locality_id bigint,
    district_id bigint,
    region_id bigint,
    building character varying(128),
    housing character varying(128),
    room character varying(128),
    post_index character varying(128),
    street_id bigint,
    filial character varying(2048),
    payment_order_index bigint DEFAULT 0 NOT NULL,
    client_group_id bigint
    ,
    CONSTRAINT rul_client_pkey PRIMARY KEY (client_id),
    CONSTRAINT fk_client_group_id FOREIGN KEY (client_group_id) REFERENCES rul_client_group(client_group_id),
    CONSTRAINT fk_postal_district_id FOREIGN KEY (district_id) REFERENCES rul_district(district_id),
    CONSTRAINT fk_postal_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_postal_region_id FOREIGN KEY (region_id) REFERENCES rul_region(region_id),
    CONSTRAINT fk_postal_street_id FOREIGN KEY (street_id) REFERENCES rul_street(street_id)
);

COMMENT ON TABLE public.rul_client IS 'Таблица контрагентов с почтовым адресом';

COMMENT ON COLUMN public.rul_client.client_name IS 'Название клиента (контрагента)';
COMMENT ON COLUMN public.rul_client.email IS 'Адрес электронной почты';
COMMENT ON COLUMN public.rul_client.description IS 'Описание клиента';
COMMENT ON COLUMN public.rul_client.locality_id IS 'Почт.адресс: Ссылка на населенный пункт(справочник)';
COMMENT ON COLUMN public.rul_client.district_id IS 'Почт.адресс: Ссылка на район(справочник)';
COMMENT ON COLUMN public.rul_client.region_id IS 'Почт.адресс: Ссылка на область(справочник)';
COMMENT ON COLUMN public.rul_client.building IS 'Почт.адресс: Номер здания';
COMMENT ON COLUMN public.rul_client.housing IS 'Почт.адресс: Номер корпуса';
COMMENT ON COLUMN public.rul_client.room IS 'Почт.адресс: Номер помещения';
COMMENT ON COLUMN public.rul_client.post_index IS 'Почт.адресс: Почтовый индекс';
COMMENT ON COLUMN public.rul_client.street_id IS 'Почт.адресс: Ссылка на улицу(справочник)';
COMMENT ON COLUMN public.rul_client.filial IS 'Филиал';
COMMENT ON COLUMN public.rul_client.payment_order_index IS 'Индекс для платежных требований (Типо сиквенса, указывает на следующий номер для платежного требования в рамках одного клиента)';
COMMENT ON COLUMN public.rul_client.client_group_id IS 'Ссылка на группу контрагентов (клиентов)';
