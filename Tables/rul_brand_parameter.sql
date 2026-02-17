CREATE TABLE public.rul_brand_parameter (
    brand_parameter_id bigint DEFAULT nextval('rul_brand_parameter_brand_parameter_id_seq'::regclass) NOT NULL,
    brand_id bigint,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    parameter_id bigint
    ,
    CONSTRAINT rul_kind_parameter_pkey PRIMARY KEY (brand_parameter_id),
    CONSTRAINT fk_brand_id FOREIGN KEY (brand_id) REFERENCES rul_brand(brand_id),
    CONSTRAINT fk_parameter_fk FOREIGN KEY (parameter_id) REFERENCES rul_parameter(parameter_id)
);

COMMENT ON COLUMN public.rul_brand_parameter.brand_id IS 'Ссылка на марку производителя';
COMMENT ON COLUMN public.rul_brand_parameter.description IS 'Описание';
COMMENT ON COLUMN public.rul_brand_parameter.parameter_id IS 'Ссылка на параметр';
