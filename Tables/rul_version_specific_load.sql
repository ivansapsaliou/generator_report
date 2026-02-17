CREATE TABLE public.rul_version_specific_load (
    version_specific_load_id bigint DEFAULT nextval('rul_version_specific_load_version_specific_load_id_seq'::regclass) NOT NULL,
    version_load_standard_id bigint,
    value numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0
    ,
    CONSTRAINT rul_version_specific_load_pkey PRIMARY KEY (version_specific_load_id),
    CONSTRAINT fk_version_load_standard_id FOREIGN KEY (version_load_standard_id) REFERENCES rul_version_load_standard(version_load_standard_id)
);

COMMENT ON COLUMN public.rul_version_specific_load.version_load_standard_id IS 'Сслыка на версию нагрузки';
COMMENT ON COLUMN public.rul_version_specific_load.value IS 'Предельная удельная нагрузка';
