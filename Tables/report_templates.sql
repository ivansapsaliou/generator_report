CREATE TABLE public.report_templates (
    id integer DEFAULT nextval('report_templates_id_seq'::regclass) NOT NULL,
    name text NOT NULL,
    config jsonb NOT NULL,
    created_by text,
    created_at timestamp with time zone DEFAULT now()
    ,
    CONSTRAINT report_templates_pkey PRIMARY KEY (id)
);
