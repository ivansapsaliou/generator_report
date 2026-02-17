CREATE TABLE public.rul_version_load_standard (
    version_load_standard_id bigint DEFAULT nextval('rul_version_load_standard_version_load_standard_id_seq'::regclass) NOT NULL,
    formula_connection_id bigint,
    description character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_version_load_standard_pkey PRIMARY KEY (version_load_standard_id),
    CONSTRAINT fk_formula_connection_id FOREIGN KEY (formula_connection_id) REFERENCES rul_formula_connection(formula_connection_id)
);

COMMENT ON COLUMN public.rul_version_load_standard.formula_connection_id IS 'Ссылка на связь подключения и формулы';
COMMENT ON COLUMN public.rul_version_load_standard.description IS 'Обоснование';
COMMENT ON COLUMN public.rul_version_load_standard.start_date IS 'Дата действия с';
COMMENT ON COLUMN public.rul_version_load_standard.end_date IS 'Дата действия по';
