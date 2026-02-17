CREATE TABLE public.rul_file_extension (
    file_extension_id bigint DEFAULT nextval('rul_file_extension_file_extension_id_seq'::regclass) NOT NULL,
    file_type_id bigint,
    name character varying(256),
    content_type character varying(255),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT pk_file_extension PRIMARY KEY (file_extension_id),
    CONSTRAINT fk_file_extension_file_type FOREIGN KEY (file_type_id) REFERENCES rul_file_type(file_type_id)
);
