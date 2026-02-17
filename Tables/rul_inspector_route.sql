CREATE TABLE public.rul_inspector_route (
    inspector_route_id bigint DEFAULT nextval('rul_inspector_route_inspector_route_id_seq'::regclass) NOT NULL,
    route character varying(64),
    user_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    object_id bigint
    ,
    CONSTRAINT rul_inspector_route_pkey PRIMARY KEY (inspector_route_id),
    CONSTRAINT fk_object_id FOREIGN KEY (object_id) REFERENCES rul_object(object_id),
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES rul_user(user_id)
);

COMMENT ON COLUMN public.rul_inspector_route.route IS 'Нащвание маршрута';
COMMENT ON COLUMN public.rul_inspector_route.user_id IS 'Контроллер (только сотрудники, не пользователи)';
COMMENT ON COLUMN public.rul_inspector_route.object_id IS 'Ссылка на объект';
