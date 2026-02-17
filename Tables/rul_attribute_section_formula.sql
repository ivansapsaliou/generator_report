CREATE TABLE public.rul_attribute_section_formula (
    attribute_section_formula_id bigint DEFAULT nextval('rul_attribute_section_formula_attribute_section_formula_id_seq'::regclass) NOT NULL,
    attribute_section_id bigint,
    formula_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_section_formula_pkey PRIMARY KEY (attribute_section_formula_id),
    CONSTRAINT fk_attribute_section_id FOREIGN KEY (attribute_section_id) REFERENCES rul_attribute_section(attribute_section_id),
    CONSTRAINT fk_formula_id FOREIGN KEY (formula_id) REFERENCES rul_formula(formula_id)
);

COMMENT ON COLUMN public.rul_attribute_section_formula.attribute_section_id IS 'Ссылка на атрибут участка';
COMMENT ON COLUMN public.rul_attribute_section_formula.formula_id IS 'Ссылка на формулу';
