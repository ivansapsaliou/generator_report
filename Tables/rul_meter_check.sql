CREATE TABLE public.rul_meter_check (
    meter_check_id bigint DEFAULT nextval('rul_meter_check_meter_check_id_seq'::regclass) NOT NULL,
    check_date timestamp without time zone,
    next_check_date timestamp without time zone,
    comment character varying(256),
    meter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_meter_check_pkey PRIMARY KEY (meter_check_id),
    CONSTRAINT fk_meter_id FOREIGN KEY (meter_id) REFERENCES rul_meter(meter_id)
);

COMMENT ON COLUMN public.rul_meter_check.check_date IS 'Дата проведения поверки';
COMMENT ON COLUMN public.rul_meter_check.next_check_date IS 'Дата следующей поверки';
COMMENT ON COLUMN public.rul_meter_check.comment IS 'Примечание';
COMMENT ON COLUMN public.rul_meter_check.meter_id IS 'Ссылка на прибор учета';
