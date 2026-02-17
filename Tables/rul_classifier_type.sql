CREATE TABLE public.rul_classifier_type (
    classifier_type_id bigint DEFAULT nextval('rul_classifier_type_classifier_type_id_seq'::regclass) NOT NULL,
    classifier_type_name character varying(256),
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_classifier_type_pkey PRIMARY KEY (classifier_type_id)
);

COMMENT ON COLUMN public.rul_classifier_type.classifier_type_name IS 'Тип классификатора';
