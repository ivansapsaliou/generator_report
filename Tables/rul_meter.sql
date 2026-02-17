CREATE TABLE public.rul_meter (
    meter_id bigint DEFAULT nextval('rul_meter_meter_id_seq'::regclass) NOT NULL,
    meter_name character varying(1024),
    description character varying(1024),
    serial_number character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    brand_id bigint,
    client_id bigint,
    manufacture_date timestamp without time zone,
    responsible_client_id bigint
    ,
    CONSTRAINT rul_meter_pkey PRIMARY KEY (meter_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_responsible_client_id FOREIGN KEY (responsible_client_id) REFERENCES rul_client(client_id)
);

COMMENT ON COLUMN public.rul_meter.meter_name IS 'Название прибора учета (счетчика)';
COMMENT ON COLUMN public.rul_meter.description IS 'Описание прибора учета';
COMMENT ON COLUMN public.rul_meter.serial_number IS 'Серийный номер';
COMMENT ON COLUMN public.rul_meter.brand_id IS 'Сслыка на марку производетеля';
COMMENT ON COLUMN public.rul_meter.client_id IS 'Держатель счетчика (у кого на балансе)';
COMMENT ON COLUMN public.rul_meter.manufacture_date IS 'Дата производства';
COMMENT ON COLUMN public.rul_meter.responsible_client_id IS 'Ссылка на ответсвенного поставщика';
