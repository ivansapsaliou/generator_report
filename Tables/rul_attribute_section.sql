CREATE TABLE public.rul_attribute_section (
    attribute_section_id bigint DEFAULT nextval('rul_attribute_section_attribute_section_id_seq'::regclass) NOT NULL,
    attribute_section_name character varying(128),
    attribute_section_code character varying(64),
    attribute_section_type_id bigint,
    attribute_block_id bigint,
    position_string smallint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    position_number smallint,
    unit_id bigint
    ,
    CONSTRAINT rul_attribute_section_pkey PRIMARY KEY (attribute_section_id),
    CONSTRAINT fk_attribute_block_id FOREIGN KEY (attribute_block_id) REFERENCES rul_attribute_block(attribute_block_id),
    CONSTRAINT fk_attribute_section_type_id FOREIGN KEY (attribute_section_type_id) REFERENCES rul_attribute_section_type(attribute_section_type_id),
    CONSTRAINT fk_unit_id FOREIGN KEY (unit_id) REFERENCES rul_unit(unit_id)
);

COMMENT ON COLUMN public.rul_attribute_section.attribute_section_name IS 'Название атрибута участка';
COMMENT ON COLUMN public.rul_attribute_section.attribute_section_code IS 'Обозначение атрибута участка';
COMMENT ON COLUMN public.rul_attribute_section.attribute_section_type_id IS 'Тип атрибута участка';
COMMENT ON COLUMN public.rul_attribute_section.attribute_block_id IS 'Блок на вэб форме';
COMMENT ON COLUMN public.rul_attribute_section.position_string IS 'Номер сроки в блоке';
COMMENT ON COLUMN public.rul_attribute_section.position_number IS 'Номер позиции в строке';
COMMENT ON COLUMN public.rul_attribute_section.unit_id IS 'Ссылка на единицу измерения';
