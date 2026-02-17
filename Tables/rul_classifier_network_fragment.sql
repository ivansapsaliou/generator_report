CREATE TABLE public.rul_classifier_network_fragment (
    classifier_network_fragment_id bigint DEFAULT nextval('rul_classifier_network_fragme_classifier_network_fragment_i_seq'::regclass) NOT NULL,
    classifier_id bigint,
    network_fragment_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_classifier_network_fragment_pkey PRIMARY KEY (classifier_network_fragment_id),
    CONSTRAINT fk_classifier_id FOREIGN KEY (classifier_id) REFERENCES rul_classifier(classifier_id),
    CONSTRAINT fk_network_fragment_id FOREIGN KEY (network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id)
);

COMMENT ON COLUMN public.rul_classifier_network_fragment.classifier_id IS 'Ссылка на классификатор';
COMMENT ON COLUMN public.rul_classifier_network_fragment.network_fragment_id IS 'Ссылка на фрагмент сети';
