CREATE TABLE public.rul_attribute_block (
    attribute_block_id bigint DEFAULT nextval('rul_attribute_block_attribute_block_id_seq'::regclass) NOT NULL,
    attribute_block_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_attribute_block_pkey PRIMARY KEY (attribute_block_id)
);

COMMENT ON COLUMN public.rul_attribute_block.attribute_block_name IS 'Название блока атрибута';
