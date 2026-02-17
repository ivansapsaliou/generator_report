CREATE TABLE public.rul_node (
    node_id bigint DEFAULT nextval('rul_node_node_id_seq'::regclass) NOT NULL,
    node_name character varying(1024),
    code character varying(256),
    placement character varying(1024),
    object_id bigint,
    service_type_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    node_type_id bigint,
    responsible_client_id bigint
    ,
    CONSTRAINT rul_node_pkey PRIMARY KEY (node_id),
    CONSTRAINT fk_node_type_id FOREIGN KEY (node_type_id) REFERENCES rul_node_type(node_type_id),
    CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES rul_object(object_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id)
);

COMMENT ON COLUMN public.rul_node.node_name IS 'Название узла';
COMMENT ON COLUMN public.rul_node.code IS 'Номер узла';
COMMENT ON COLUMN public.rul_node.placement IS 'Место размещения узла';
COMMENT ON COLUMN public.rul_node.object_id IS 'Ссылка на объект на котором находится узел';
COMMENT ON COLUMN public.rul_node.service_type_id IS 'Ссылка на тип услуги (энергоресурс)';
COMMENT ON COLUMN public.rul_node.start_date IS 'Начало даты действия';
COMMENT ON COLUMN public.rul_node.end_date IS 'Завершение даты действия';
