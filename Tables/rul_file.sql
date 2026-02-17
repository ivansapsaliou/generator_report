CREATE TABLE public.rul_file (
    file_id bigint DEFAULT nextval('rul_file_file_id_seq'::regclass) NOT NULL,
    real_name character varying(256),
    label_name character varying(256),
    file_extension_id bigint,
    size integer,
    width integer,
    height integer,
    file_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    entity_id bigint,
    entity_item_id bigint,
    file_matter_type_id bigint,
    hash character varying(255)
    ,
    CONSTRAINT pk_file PRIMARY KEY (file_id),
    CONSTRAINT fk_entity_id FOREIGN KEY (entity_id) REFERENCES rul_entity(entity_id),
    CONSTRAINT fk_file__file_extension FOREIGN KEY (file_extension_id) REFERENCES rul_file_extension(file_extension_id),
    CONSTRAINT fk_file__file_type FOREIGN KEY (file_type_id) REFERENCES rul_file_type(file_type_id),
    CONSTRAINT fk_file_content_type_id FOREIGN KEY (file_matter_type_id) REFERENCES rul_file_matter_type(file_matter_type_id)
);

COMMENT ON COLUMN public.rul_file.file_matter_type_id IS 'Ссылка на ид. типа контента в файле';
