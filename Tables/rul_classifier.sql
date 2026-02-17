CREATE TABLE public.rul_classifier (
    classifier_id bigint DEFAULT nextval('rul_classifier_classifier_id_seq'::regclass) NOT NULL,
    classifier_name character varying(128),
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    classifier_type_id bigint DEFAULT 1 NOT NULL
    ,
    CONSTRAINT rul_classifier_pkey PRIMARY KEY (classifier_id),
    CONSTRAINT fk_classifier_type FOREIGN KEY (classifier_type_id) REFERENCES rul_classifier_type(classifier_type_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id)
);

COMMENT ON COLUMN public.rul_classifier.classifier_name IS 'Название классификатора';
COMMENT ON COLUMN public.rul_classifier.client_id IS 'Ссылка на поставщика';
COMMENT ON COLUMN public.rul_classifier.classifier_type_id IS 'Ссылка на тип классификатора. (На данный момент будет использоваться для агрегации данных в отчетах)';
