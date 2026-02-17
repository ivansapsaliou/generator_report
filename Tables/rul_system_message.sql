CREATE TABLE public.rul_system_message (
    system_message_code character varying(64),
    system_message_lang character varying(3),
    system_message_text character varying(256)
);

COMMENT ON COLUMN public.rul_system_message.system_message_code IS 'Код системного сообщения';
COMMENT ON COLUMN public.rul_system_message.system_message_lang IS 'Язык сисемного сообщения';
COMMENT ON COLUMN public.rul_system_message.system_message_text IS 'Текст системного сообщения';
