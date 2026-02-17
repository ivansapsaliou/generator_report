CREATE TABLE public.rul_panel (
    panel_id bigint DEFAULT nextval('rul_panel_panel_id_seq'::regclass) NOT NULL,
    brand_id bigint,
    panel_name character varying(256),
    accuracy_class numeric,
    standard_size numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    indication_type_id bigint
    ,
    CONSTRAINT rul_panel_pkey PRIMARY KEY (panel_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_indication_type FOREIGN KEY (indication_type_id) REFERENCES rul_indication_type(indication_type_id)
);

COMMENT ON COLUMN public.rul_panel.brand_id IS 'Сслыка на марку производетеля';
COMMENT ON COLUMN public.rul_panel.panel_name IS 'Название панели прибора учета (счетчика)';
COMMENT ON COLUMN public.rul_panel.accuracy_class IS 'Класс точности';
COMMENT ON COLUMN public.rul_panel.standard_size IS 'Типоразмер';
COMMENT ON COLUMN public.rul_panel.indication_type_id IS 'Ссылка на тип показания(Справочная т.к. на рассчеты не влияет)';
