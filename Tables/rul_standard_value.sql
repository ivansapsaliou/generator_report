CREATE TABLE public.rul_standard_value (
    standard_value_id bigint DEFAULT nextval('rul_standard_value_standard_value_id_seq'::regclass) NOT NULL,
    value numeric,
    comitet_resolution character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    standard_id bigint
    ,
    CONSTRAINT rul_standard__value_pkey PRIMARY KEY (standard_value_id),
    CONSTRAINT fk_standard_id FOREIGN KEY (standard_id) REFERENCES rul_standard(standard_id)
);

COMMENT ON COLUMN public.rul_standard_value.value IS 'Значение норматива';
COMMENT ON COLUMN public.rul_standard_value.comitet_resolution IS 'Решение облисполкома';
COMMENT ON COLUMN public.rul_standard_value.start_date IS 'Дата начала действия норматива';
COMMENT ON COLUMN public.rul_standard_value.end_date IS 'Дата завершения действия норматива';
COMMENT ON COLUMN public.rul_standard_value.standard_id IS 'Ссылка на норматив';
