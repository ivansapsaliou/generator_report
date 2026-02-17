CREATE TABLE public.rul_parameter (
    parameter_id bigint DEFAULT nextval('rul_parameter_parameter_id_seq'::regclass) NOT NULL,
    unit_id bigint,
    parameter_type_id bigint,
    description character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    parameter_name character varying(256),
    in_calculate_parameters smallint DEFAULT 0
    ,
    CONSTRAINT rul_parameter_pkey PRIMARY KEY (parameter_id),
    CONSTRAINT fk_parameter_type_id FOREIGN KEY (parameter_type_id) REFERENCES rul_parameter_type(parameter_type_id),
    CONSTRAINT fk_unit_id FOREIGN KEY (unit_id) REFERENCES rul_unit(unit_id)
);

COMMENT ON COLUMN public.rul_parameter.unit_id IS 'Ссылка на единицу измерения';
COMMENT ON COLUMN public.rul_parameter.parameter_type_id IS 'Ссылка на тип параметра';
COMMENT ON COLUMN public.rul_parameter.description IS 'Описание параметра';
