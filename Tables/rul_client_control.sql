CREATE TABLE public.rul_client_control (
    client_control_id bigint DEFAULT nextval('rul_client_control_client_control_id_seq'::regclass) NOT NULL,
    curator_client_id bigint NOT NULL,
    dependent_client_id bigint NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_client_control_pkey PRIMARY KEY (client_control_id),
    CONSTRAINT chk_self_reference CHECK ((curator_client_id <> dependent_client_id)),
    CONSTRAINT uq_curator_dependent UNIQUE (curator_client_id, dependent_client_id),
    CONSTRAINT fk_curator FOREIGN KEY (curator_client_id) REFERENCES rul_client(client_id) ON DELETE CASCADE,
    CONSTRAINT fk_dependent FOREIGN KEY (dependent_client_id) REFERENCES rul_client(client_id) ON DELETE CASCADE
);
