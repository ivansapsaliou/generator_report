CREATE TABLE public.rul_attribute_section_value (
    attribute_section_value_id bigint DEFAULT nextval('rul_attribute_section_value_attribute_section_value_id_seq'::regclass) NOT NULL,
    section_id bigint,
    attribute_section_id bigint,
    value numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_section_value_pkey PRIMARY KEY (attribute_section_value_id),
    CONSTRAINT fk_section_id FOREIGN KEY (section_id) REFERENCES rul_section(section_id)
);

COMMENT ON COLUMN public.rul_attribute_section_value.section_id IS 'Ссылка на участок';
COMMENT ON COLUMN public.rul_attribute_section_value.attribute_section_id IS 'Ссылка на атрибут участка';
COMMENT ON COLUMN public.rul_attribute_section_value.value IS 'Значение атрибута';
