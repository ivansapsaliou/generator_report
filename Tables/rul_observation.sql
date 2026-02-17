CREATE TABLE public.rul_observation (
    observation_id bigint DEFAULT nextval('rul_observation_observation_id_seq'::regclass) NOT NULL,
    client_id bigint,
    locality_id bigint,
    observation_period_id bigint,
    observation_type_id bigint,
    observation_date timestamp without time zone,
    temperature numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_observation_pkey PRIMARY KEY (observation_id),
    CONSTRAINT fk_client_id FOREIGN KEY (client_id) REFERENCES rul_client(client_id),
    CONSTRAINT fk_locality_id FOREIGN KEY (locality_id) REFERENCES rul_locality(locality_id),
    CONSTRAINT fk_observation_period_id FOREIGN KEY (observation_period_id) REFERENCES rul_observation_period(observation_period_id),
    CONSTRAINT observation_type_id FOREIGN KEY (observation_type_id) REFERENCES rul_observation_type(observation_type_id)
);
