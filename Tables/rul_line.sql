CREATE TABLE public.rul_line (
    line_id bigint DEFAULT nextval('rul_line_line_id_seq'::regclass) NOT NULL,
    line_name character varying(128),
    client_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    node_id bigint,
    child_node_id bigint,
    network_fragment_id bigint
    ,
    CONSTRAINT rul_line_pkey PRIMARY KEY (line_id),
    CONSTRAINT fk_child_node_id FOREIGN KEY (child_node_id) REFERENCES rul_node(node_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_network_fragment FOREIGN KEY (network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id),
    CONSTRAINT fk_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id)
);

COMMENT ON COLUMN public.rul_line.line_name IS 'Название линии';
COMMENT ON COLUMN public.rul_line.client_id IS 'Балансодержатель/контрагент';
COMMENT ON COLUMN public.rul_line.node_id IS 'Родительский узел';
COMMENT ON COLUMN public.rul_line.child_node_id IS 'Дочерний узел';
COMMENT ON COLUMN public.rul_line.network_fragment_id IS 'Ссылка на фрагмент';
