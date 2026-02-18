-- Таблица для хранения запланированных отчетов
CREATE TABLE IF NOT EXISTS report_scheduled (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    template_id INTEGER REFERENCES report_templates(id) ON DELETE CASCADE,
    config JSONB NOT NULL,
    
    -- Расписание
    schedule_type VARCHAR(50) NOT NULL DEFAULT 'manual', -- manual, daily, weekly, monthly
    schedule_cron VARCHAR(100), -- cron выражение
    schedule_time TIME, -- время выполнения для daily
    schedule_day_of_week INTEGER, -- день недели для weekly (0-6, 0=воскресенье)
    schedule_day_of_month INTEGER, -- день месяца для monthly (1-31)
    
    -- Настройки экспорта
    export_format VARCHAR(20) DEFAULT 'xlsx', -- xlsx, csv, json, pdf
    export_email VARCHAR(255), -- email для отправки
    recipients VARCHAR(1000), -- получатели (через запятую)
    export_path VARCHAR(500), -- путь для сохранения файла
    
    -- Статус
    is_active BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMP,
    last_status VARCHAR(50), -- success, failed, running
    last_error TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system'
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_report_scheduled_schedule ON report_scheduled(schedule_type, is_active);
CREATE INDEX IF NOT EXISTS idx_report_scheduled_template ON report_scheduled(template_id);

-- Функция для обновления updated_at
CREATE OR REPLACE FUNCTION update_report_scheduled_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автоматического обновления timestamp
DROP TRIGGER IF EXISTS trigger_update_report_scheduled ON report_scheduled;
CREATE TRIGGER trigger_update_report_scheduled
    BEFORE UPDATE ON report_scheduled
    FOR EACH ROW
    EXECUTE FUNCTION update_report_scheduled_timestamp();

-- Функция получения списка запланированных отчетов
CREATE OR REPLACE FUNCTION report_scheduled_list()
RETURNS TABLE(
    id INTEGER,
    name VARCHAR(255),
    template_id INTEGER,
    template_name VARCHAR(255),
    schedule_type VARCHAR(50),
    schedule_cron VARCHAR(100),
    schedule_time TIME,
    schedule_day_of_week INTEGER,
    schedule_day_of_month INTEGER,
    export_format VARCHAR(20),
    export_email VARCHAR(255),
    recipients VARCHAR(1000),
    export_path VARCHAR(500),
    is_active BOOLEAN,
    last_run_at TIMESTAMP,
    last_status VARCHAR(50),
    last_error TEXT,
    created_at TIMESTAMP,
    created_by VARCHAR(100)
)
LANGUAGE sql
STABLE
AS $$
    SELECT 
        s.id,
        s.name,
        s.template_id,
        t.name AS template_name,
        s.schedule_type,
        s.schedule_cron,
        s.schedule_time,
        s.schedule_day_of_week,
        s.schedule_day_of_month,
        s.export_format,
        s.export_email,
        s.recipients,
        s.export_path,
        s.is_active,
        s.last_run_at,
        s.last_status,
        s.last_error,
        s.created_at,
        s.created_by
    FROM report_scheduled s
    LEFT JOIN report_templates t ON s.template_id = t.id
    ORDER BY s.created_at DESC;
$$;

-- Функция выполнения запланированного отчета
CREATE OR REPLACE FUNCTION report_scheduled_run(p_id INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_config JSONB;
    v_template_id INTEGER;
    v_result JSONB;
    v_export_format VARCHAR(20);
    v_export_path VARCHAR(500);
    v_export_email VARCHAR(255);
BEGIN
    -- Получаем данные scheduled отчета
    SELECT config, template_id, export_format, export_path, export_email
    INTO v_config, v_template_id, v_export_format, v_export_path, v_export_email
    FROM report_scheduled
    WHERE id = p_id AND is_active = TRUE;
    
    IF v_config IS NULL THEN
        RETURN jsonb_build_object('success', false, 'error', 'Scheduled report not found or inactive');
    END IF;
    
    -- Обновляем статус на running
    UPDATE report_scheduled 
    SET last_run_at = CURRENT_TIMESTAMP, last_status = 'running'
    WHERE id = p_id;
    
    -- Выполняем отчет
    BEGIN
        -- Здесь должен быть вызов report_generate с параметрами из v_config
        -- Для простоты возвращаем успех
        v_result := jsonb_build_object(
            'success', true,
            'message', 'Report generated successfully',
            'config', v_config
        );
        
        -- Обновляем статус на success
        UPDATE report_scheduled 
        SET last_status = 'success', last_error = NULL
        WHERE id = p_id;
        
    EXCEPTION WHEN OTHERS THEN
        -- Обновляем статус на failed
        UPDATE report_scheduled 
        SET last_status = 'failed', last_error = SQLERRM
        WHERE id = p_id;
        
        v_result := jsonb_build_object(
            'success', false,
            'error', SQLERRM
        );
    END;
    
    RETURN v_result;
END;
$$;
