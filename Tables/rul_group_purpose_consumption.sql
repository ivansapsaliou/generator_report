CREATE TABLE public.rul_group_purpose_consumption (
    group_purpose_consumption_id bigint DEFAULT nextval('rul_group_purpose_consumption_group_purpose_consumption_id_seq'::regclass) NOT NULL,
    group_consumption_id bigint,
    purpose_consumption_id bigint,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id integer NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_group_purpose_consumption_pkey PRIMARY KEY (group_purpose_consumption_id),
    CONSTRAINT fk_client_id FOREIGN KEY (purpose_consumption_id) REFERENCES rul_purpose_consumption(purpose_consumption_id),
    CONSTRAINT fk_consumption_group_id FOREIGN KEY (group_consumption_id) REFERENCES rul_group_consumption(group_consumption_id)
);
