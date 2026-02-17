CREATE TABLE public.rul_connection_connection (
    connection_connection_id bigint DEFAULT nextval('rul_connection_connection_connection_connection_id_seq'::regclass) NOT NULL,
    destination_connection_id bigint,
    source_connection_id bigint,
    formula_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_connection_connection_pkey PRIMARY KEY (connection_connection_id),
    CONSTRAINT fk_destination_connection_id FOREIGN KEY (destination_connection_id) REFERENCES rul_connection(connection_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_source_connection_id FOREIGN KEY (source_connection_id) REFERENCES rul_connection(connection_id)
);

COMMENT ON COLUMN public.rul_connection_connection.destination_connection_id IS 'То подключение, к которому заведен расчет по внешнему узлу';
COMMENT ON COLUMN public.rul_connection_connection.source_connection_id IS 'То подключение, из которого будут выбираться расходы';
COMMENT ON COLUMN public.rul_connection_connection.formula_id IS 'Ссылка на формулу';
