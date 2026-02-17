CREATE TABLE public.rul_network_fragment (
    network_fragment_id bigint DEFAULT nextval('rul_network_fragment_network_fragment_id_seq'::regclass) NOT NULL,
    network_fragment_type_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    network_fragment_name character varying(64),
    client_id bigint
    ,
    CONSTRAINT rul_network_fragment_pkey PRIMARY KEY (network_fragment_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_network_fragment_type_id FOREIGN KEY (network_fragment_type_id) REFERENCES rul_network_fragment_type(network_fragment_type_id)
);

COMMENT ON COLUMN public.rul_network_fragment.network_fragment_type_id IS 'Тип фрагмента';
COMMENT ON COLUMN public.rul_network_fragment.network_fragment_name IS 'Название фрагмента';
COMMENT ON COLUMN public.rul_network_fragment.client_id IS 'Ссылка на клиента';
