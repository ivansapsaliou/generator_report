CREATE TABLE public.rul_precipitation (
    precipitation_id bigint DEFAULT nextval('rul_precipitation_precipitation_id_seq'::regclass) NOT NULL,
    client_id bigint,
    locality_id bigint,
    precipitation_period_id bigint,
    precipitation_type_id bigint,
    precipitation_date timestamp without time zone,
    level_precipitation numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_precipitation_pkey PRIMARY KEY (precipitation_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_precipitation_period_id FOREIGN KEY (precipitation_period_id) REFERENCES rul_precipitation_period(precipitation_period_id),
    CONSTRAINT precipitation_type_id FOREIGN KEY (precipitation_type_id) REFERENCES rul_precipitation_type(precipitation_type_id)
);

COMMENT ON COLUMN public.rul_precipitation.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_precipitation.locality_id IS 'Ссылка на населенный пункт';
COMMENT ON COLUMN public.rul_precipitation.precipitation_period_id IS 'Ссылка на периода осадков (пока только среднемесячные)';
COMMENT ON COLUMN public.rul_precipitation.precipitation_type_id IS 'Ссылка на вид осадков';
COMMENT ON COLUMN public.rul_precipitation.precipitation_date IS 'Дата осадков';
COMMENT ON COLUMN public.rul_precipitation.level_precipitation IS 'Уровень осадков';
