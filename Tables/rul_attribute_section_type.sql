CREATE TABLE public.rul_attribute_section_type (
    attribute_section_type_id bigint DEFAULT nextval('rul_attribute_section_type_attribute_section_type_id_seq'::regclass) NOT NULL,
    attribute_section_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_section_type_pkey PRIMARY KEY (attribute_section_type_id)
);

COMMENT ON COLUMN public.rul_attribute_section_type.attribute_section_type_name IS 'Название типа атрибута';
