CREATE TABLE public.rul_version_constant (
    version_constant_id bigint DEFAULT nextval('rul_version_constant_version_constant_id_seq'::regclass) NOT NULL,
    formula_id bigint,
    description character varying(256),
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_version_constant_pkey PRIMARY KEY (version_constant_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id)
);

COMMENT ON COLUMN public.rul_version_constant.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_version_constant.description IS 'Обоснование';
COMMENT ON COLUMN public.rul_version_constant.start_date IS 'Дата действия с';
COMMENT ON COLUMN public.rul_version_constant.end_date IS 'Дата действия по';
