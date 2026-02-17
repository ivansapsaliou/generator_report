CREATE TABLE public.rul_formula_connection (
    formula_connection_id bigint DEFAULT nextval('rul_formula_connection_formula_connection_id_seq'::regclass) NOT NULL,
    formula_id bigint,
    connection_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_formula_connection_pkey PRIMARY KEY (formula_connection_id),
    CONSTRAINT fk_connection_id FOREIGN KEY (connection_id) REFERENCES rul_connection(connection_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id)
);

COMMENT ON COLUMN public.rul_formula_connection.formula_id IS 'Ссылка на формулу';
COMMENT ON COLUMN public.rul_formula_connection.connection_id IS 'Ссылка на подключение';
