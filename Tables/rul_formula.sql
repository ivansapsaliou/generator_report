CREATE TABLE public.rul_formula (
    formula_id bigint DEFAULT nextval('rul_formula_formula_id_seq'::regclass) NOT NULL,
    formula_name character varying(256),
    file_id bigint,
    parameter_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    formula_text character varying(512),
    source_consumption_id bigint,
    is_template numeric(1,0) DEFAULT 0,
    method_id bigint
    ,
    CONSTRAINT rul_formula_pkey PRIMARY KEY (formula_id),
    CONSTRAINT fk_file_id FOREIGN KEY (file_id) REFERENCES rul_file(file_id),
    CONSTRAINT fk_parameter_id FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id),
    CONSTRAINT fk_source_consumption_id FOREIGN KEY (source_consumption_id) REFERENCES rul_source_consumption(source_consumption_id)
);

COMMENT ON COLUMN public.rul_formula.formula_name IS 'Название формулы';
COMMENT ON COLUMN public.rul_formula.file_id IS 'Ссылка на Изображение??';
COMMENT ON COLUMN public.rul_formula.parameter_id IS 'Параметр энергоресурса, который получается в результе вычисления';
COMMENT ON COLUMN public.rul_formula.formula_text IS 'Формула отображающаяся в интерфейсе';
COMMENT ON COLUMN public.rul_formula.source_consumption_id IS 'Ссылка на источник данных о расходах';
COMMENT ON COLUMN public.rul_formula.is_template IS 'Флаг, является ли шаблоном формула';
COMMENT ON COLUMN public.rul_formula.method_id IS 'Метод, не ссылка';
