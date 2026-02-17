CREATE TABLE public.rul_standard (
    standard_id bigint DEFAULT nextval('rul_standard_standard_id_seq'::regclass) NOT NULL,
    comitet_id bigint,
    service_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    client_id bigint,
    formula_id bigint
    ,
    CONSTRAINT rul_standard_pkey PRIMARY KEY (standard_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_comitet_id FOREIGN KEY (comitet_id) REFERENCES rul_comitet(comitet_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON COLUMN public.rul_standard.comitet_id IS 'Ссылка на облисполком';
COMMENT ON COLUMN public.rul_standard.service_type_id IS 'Ссылка на вид услуги(энергоресурс)';
COMMENT ON COLUMN public.rul_standard.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_standard.formula_id IS 'Ссылка на формулу';
