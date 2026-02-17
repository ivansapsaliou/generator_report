-- Экспорт функций и процедур схемы public
-- База данных: rul_jkh
-- Дата экспорта: 2026-02-15 16:40:11

-- PROCEDURE: public.add_log(IN p_table_name text)
CREATE OR REPLACE PROCEDURE public.add_log(IN p_table_name text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $procedure$
DECLARE
    l_table_name varchar(256);
    l_cmd varchar(32000);
    l_cmd1 varchar(40000);
    l_cnt bigint;
    l_column_list varchar(32000);
    l_trigger_name varchar(256);
    l_log_table_name varchar(256);
    l_insert_column_list varchar(32000):='';
    l_insert_value_list varchar(32000):='';
    l_delete_column_list varchar(32000):='';
    l_delete_value_list varchar(32000):='';
    l_status boolean ;
    l_body char :='$';
    l_tab_column_cur CURSOR FOR
    SELECT column_name, data_type
    from information_schema.columns
    where upper(table_name)=upper(p_table_name)
    and (table_name!='PRV_SESSION' or column_name!='SESSION_ID');
BEGIN
    l_table_name:=lower(p_table_name);
    l_trigger_name:='log_'||substr(l_table_name,1,23)||'_tr';
    l_log_table_name:='log_'||substr(l_table_name,1,25);
    l_column_list:='  log_id bigserial not null,'||chr(10)||
                   '  log_time timestamp not null,'||chr(10)||
                   '  log_action varchar(4) not null,'||chr(10)||
                   '  log_user_id bigint,'||chr(10)||
                   '  log_user_ip varchar(128)';
    for rec in l_tab_column_cur loop
      l_column_list:=l_column_list||','||chr(10)||'  '||rec.column_name||' '||rec.data_type;
      if rec.data_type='VARCHAR' then
        l_column_list:=l_column_list||'('||rec.data_length||')';
      end if;
      l_insert_column_list:=l_insert_column_list||','||lower(rec.column_name);
      l_insert_value_list:=l_insert_value_list||',new.'||lower(rec.column_name);
      l_delete_column_list:=l_delete_column_list||','||lower(rec.column_name);
      l_delete_value_list:=l_delete_value_list||',old.'||lower(rec.column_name);
    end loop;
    select count(*)
    into STRICT l_cnt
    from pg_class
    where upper(relname)=upper(l_log_table_name);
    if l_cnt=0 then
      EXECUTE 'create table log.'||l_log_table_name||'('||chr(10)||l_column_list||chr(10)||')';
      EXECUTE 'create index '||l_log_table_name||'_idx on log.'||l_log_table_name||' (log_id)';
      RAISE NOTICE 'create log table %', l_log_table_name;
    end if;
    l_cmd := 'create trigger '||l_trigger_name||'
              after insert or update or delete on '||l_table_name||'
              FOR EACH ROW
              EXECUTE PROCEDURE log.trigger_fct_'||l_trigger_name||'();';
    l_cmd1 := 'CREATE OR REPLACE FUNCTION log.trigger_fct_'||l_trigger_name||'()
                RETURNS trigger AS
                '||l_body||'body'||l_body||'
                 DECLARE
                  l_min_log_id numeric;
                  l_user_id bigint :=  (select case when current_setting(''user_ctx.user_name'',true) = '''' then null else current_setting(''user_ctx.user_name'',true) end )::bigint;
                  l_user_ip character varying(128) := (select case when current_setting(''user_ctx.user_ip'',true) is null then inet_client_addr()::varchar else current_setting(''user_ctx.user_ip'',true) end);
                  l_updating character varying(1) := case when TG_OP = ''UPDATE'' then ''u'' else '''' end;
                 begin
                   if TG_OP = ''DELETE'' or TG_OP = ''UPDATE'' then
                    insert into log.'||l_log_table_name||'(log_time,log_action,log_user_id,log_user_ip'||l_delete_column_list||')
                    values (current_timestamp,l_updating||''d'',l_user_id,l_user_ip'||l_delete_value_list||');
                   end if;
                   if TG_OP = ''INSERT'' or TG_OP = ''UPDATE'' then
                    insert into log.'||l_log_table_name||'(log_time,log_action,log_user_id,log_user_ip'||l_insert_column_list||')
                    values (current_timestamp,l_updating||''i'',l_user_id,l_user_ip'||l_insert_value_list||');
                   end if;
                  RETURN NULL;
                 END;
                '||l_body||'body'||l_body||'
                LANGUAGE ''plpgsql''
                VOLATILE
                CALLED ON NULL INPUT
                SECURITY INVOKER
                PARALLEL UNSAFE
                COST 100;';
   begin
      EXECUTE l_cmd1;
    exception when others then
      RAISE NOTICE '%', l_cmd1;
      RAISE EXCEPTION '%', 'Error creating trigger function' USING ERRCODE = '45000';
    end;
    begin
      EXECUTE l_cmd;
    exception when others then
      RAISE NOTICE '%', l_cmd;
      RAISE EXCEPTION '%', 'Error creating trigger' USING ERRCODE = '45000';
    end;
    /*select has_schema_privilege('invoice', 'public', 'USAGE') into STRICT l_status;
    if l_status = True then
      RAISE NOTICE 'create trigger %', l_trigger_name;
    else
      RAISE EXCEPTION '%', 'Trigger '||l_trigger_name||' invalid' USING ERRCODE = '45000';
    end if;*/
  end;
  -----------------------------------------------------------------------------------------------
$procedure$

-- ======================================================================

-- PROCEDURE: public.bkp_process_charges(IN p_agreement_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.bkp_process_charges(IN p_agreement_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_ids BIGINT;
BEGIN
	FOREACH v_ids IN ARRAY p_agreement_id
    LOOP
      insert into rul_debug_log (debug_user_id,debug_module)
      values (current_setting('user_ctx.user_name',true),'Пересчет');
      -- Чистка старых начислений, кроме ручных.
      -- Удаляем начисления и детали по подключениям входящим договора в рамках расчетного периода.
      DELETE FROM rul_charge_detail WHERE start_date >= p_start_date AND end_date <= p_end_date
      AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = v_ids)
      AND (SELECT charge_checked FROM rul_charge WHERE charge_id = rul_charge_detail.charge_id) = 0;
      DELETE FROM rul_charge WHERE charge_type_id != 2 AND start_date >= p_start_date AND end_date <= p_end_date
      AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = v_ids)
      AND charge_checked = 0;
      DELETE FROM rul_consumption_losses WHERE start_date >= p_start_date AND end_date <= p_end_date
      AND ((connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = v_ids) OR connection_id is null)
      OR connection_id IS NULL);
      -- Формирование начислений по приборному учету.
      -- На данный момент просто формирует начисления исходя из показаний и формул.
      CALL public.process_consumption_data(p_start_date, p_end_date);
      CALL public.process_formuls_indication(p_start_date, p_end_date);
      -- Формирование теоретических нагрузочных расходов. Фактические будут считаться в ГПУ.
      CALL public.process_formuls_load(v_ids,p_start_date,p_end_date);
      -- Формирование теоретических расходов для способа учета по норме.
      -- Использует для своих расчетов метод, а не формулу. Также вместо id параметров ссылается на имена параметров
      CALL public.process_formuls_standard(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по подключению-источнику.
      --CALL public.process_formuls_source_connection(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по сечению.
      CALL public.process_formuls_pipe(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по среднему.
      -- CALL public.process_formuls_average(p_agreement_id,p_start_date,p_end_date); --Вызывается в ГПУ
      -- Расчет по ГПУ. Должен построить деревья в рамках договора. С нижних листов рассчитать Учетные способы учета.
      -- Затем произвести для каждого узла ГПУ.
      -- Так же должен обойти отдельные узлы, чтобы в них расчить начисления
      CALL public.process_group_accounting_new(v_ids, p_start_date, p_end_date);
      --25.11.2025 Новый расчет начислений, вернуть если что отдельные методы
      /*CALL public.process_charges_standard(p_agreement_id,p_start_date,p_end_date);
      CALL public.process_charges_load(p_agreement_id,p_start_date,p_end_date);
      CALL public.process_charges_source_connection(p_agreement_id,p_start_date,p_end_date);*/
      CALL public.process_charges_new(v_ids,p_start_date,p_end_date);
      --Начисление по потерям надо создавать после балансировки
      CALL public.process_charges_losses(v_ids,p_start_date,p_end_date);
      CALL public.process_charges_planned_consumption(v_ids,p_start_date,p_end_date);
      CALL public.process_zero_charges(v_ids,p_start_date,p_end_date);
      --CALL public.process_charges_pipe(p_agreement_id,p_start_date,p_end_date); -- Их не будет вообще походу
      --CALL public.process_charges_average(p_agreement_id,p_start_date,p_end_date); -- Их не будет вообще походу
      --CALL public.process_charges_indication(p_agreement_id,p_start_date,p_end_date); -- Их не будет вообще походу
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.calculate_attribute_section(IN p_section_id bigint)
CREATE OR REPLACE PROCEDURE public.calculate_attribute_section(IN p_section_id bigint)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	K numeric;
    V_p numeric;
    Q1 numeric;
    Q2 numeric;
    row_record record;
BEGIN
	DELETE FROM public.rul_attribute_section_value WHERE section_id = p_section_id AND attribute_section_id in (18,19,20,21);
    FOR row_record IN SELECT rasf.attribute_section_id
    FROM public.rul_attribute_section_formula rasf
    JOIN public.rul_line_parameter rlp
    	ON rlp.formula_id = rasf.formula_id
    JOIN public.rul_section rs
    	ON rlp.line_id = rs.line_id
    WHERE rasf.attribute_section_id in (18,19,20,21)
    AND rs.section_id = p_section_id
    ORDER BY rasf.attribute_section_id
    LOOP
    	IF row_record.attribute_section_id = 18 THEN
        -- Расчет Kстар
            select case when (1 + П)* m > 3 then 3 else (1 + П)* m end into K from
              (
              select (select value from public.rul_attribute_section_value where attribute_section_id = 11 and
              section_id = p_section_id) as m,
              (select value from public.rul_attribute_section_value where attribute_section_id = 12 and section_id = p_section_id) as П
              ) vals;
            insert into rul_attribute_section_value (section_id,attribute_section_id,value)
              values (p_section_id,18,K);
        ELSIF row_record.attribute_section_id = 19 THEN
        -- Расчет V_p
            select
                (
                  (PI()/4 * ((Dn1 - 2 * s1) ^ 2) * 0.000001) * L1
                  +
                  (PI()/4 * ((Dn2 - 2 * s2) ^ 2) * 0.000001) * L2
                ) * K
                into V_p
                from
            (
            select (select value from public.rul_attribute_section_value where attribute_section_id = 4 and section_id = p_section_id) as Dn1,
                (select value from public.rul_attribute_section_value where attribute_section_id = 6 and section_id = p_section_id) as s1,
                (select value from public.rul_attribute_section_value where attribute_section_id = 5 and section_id = p_section_id) as Dn2,
                (select value from public.rul_attribute_section_value where attribute_section_id = 7 and section_id = p_section_id) as s2,
                (select value from public.rul_attribute_section_value where attribute_section_id = 8 and section_id = p_section_id) as L1,
                (select value from public.rul_attribute_section_value where attribute_section_id = 9 and section_id = p_section_id) as L2
                ) vals;
            insert into rul_attribute_section_value (section_id,attribute_section_id,value)
              values (p_section_id,19,V_p);
        ELSIF row_record.attribute_section_id = 20 THEN
        -- Расчет Q1
            select vals.q1 * b * L1 into Q1 from
              (
              select (select value from public.rul_attribute_section_value where attribute_section_id = 16 and
              section_id = p_section_id) as q1,
              (select value from public.rul_attribute_section_value where attribute_section_id = 10 and section_id = p_section_id) as b,
              (select value from public.rul_attribute_section_value where attribute_section_id = 8 and section_id = p_section_id) as L1
              ) vals;
            insert into rul_attribute_section_value (section_id,attribute_section_id,value)
              values (p_section_id,20,Q1);
        ELSIF row_record.attribute_section_id = 21 THEN
        -- Расчет Q2
            select vals.q2 * b * L2 into Q2 from
              (
              select (select value from public.rul_attribute_section_value where attribute_section_id = 17 and section_id = p_section_id) as q2,
              (select value from public.rul_attribute_section_value where attribute_section_id = 10 and section_id = p_section_id) as b,
              (select value from public.rul_attribute_section_value where attribute_section_id = 9 and section_id = p_section_id) as L2
              ) vals;
            insert into rul_attribute_section_value (section_id,attribute_section_id,value)
              values (p_section_id,21,Q2);
        END IF;
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.change_confirmation_charge(IN p_charge_ids bigint[], IN p_charge_checked numeric)
CREATE OR REPLACE PROCEDURE public.change_confirmation_charge(IN p_charge_ids bigint[], IN p_charge_checked numeric)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_cnt bigint := 0;
BEGIN
    SELECT COUNT(*)
    INTO v_cnt
    FROM rul_charge rc
    WHERE rc.invoice_id IS NOT NULL
      AND (
            rc.charge_id = ANY (p_charge_ids)
            OR (
                rc.charge_type_id = 1
                AND rc.source_id != 4
            )
          )
      AND EXISTS (
          SELECT 1
          FROM (
              SELECT DISTINCT
                     connection_id,
                     date_trunc('month', billing_start_date) AS month_start
              FROM rul_charge
              WHERE charge_id = ANY (p_charge_ids)
          ) src
          WHERE rc.connection_id = src.connection_id
            AND date_trunc('month', rc.billing_start_date) = src.month_start
      );
    IF v_cnt != 0 AND p_charge_checked = 0 THEN
        RAISE EXCEPTION '%',
            get_message('ERR_CHANGE_CONFIRMATION_CHARGE');
    END IF;
    UPDATE rul_charge rc
    SET charge_checked = p_charge_checked
    WHERE EXISTS (
        SELECT 1
        FROM rul_charge c
        WHERE c.charge_id = ANY (p_charge_ids)
          AND c.connection_id = rc.connection_id
          AND date_trunc('month', c.billing_start_date)
              = date_trunc('month', rc.billing_start_date)
    )
    AND (
        rc.charge_id = ANY (p_charge_ids)
        OR (
            rc.charge_type_id = 1
            AND rc.source_id != 4
        )
    );
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.cleanup_scheme()
CREATE OR REPLACE PROCEDURE public.cleanup_scheme()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    ora2pg_rowcount int;
    l_affected_rows bigint := 0;
    c_log_store_days bigint :=  180;
	i RECORD;
BEGIN
    --Delete logs
    for i in (SELECT distinct table_name
              from information_schema.columns
              where table_schema = 'log') loop
      EXECUTE 'delete from log.'||i.table_name||' where log_time < now() - interval '' '
        ||c_log_store_days||' day''';
      GET DIAGNOSTICS ora2pg_rowcount = ROW_COUNT;
      l_affected_rows := l_affected_rows +  ora2pg_rowcount;
      commit;
    end loop;
  end;
  -----------------------------------------------------------------------------------------------
$procedure$

-- ======================================================================

-- PROCEDURE: public.clear_all_for_recalculate_invoice(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.clear_all_for_recalculate_invoice(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	-- Метод используется не только для пересчета и расчета финансов, но и для удаления
    -- проводок и счетов. Нужно это учитывать при внесении изменений.
	-- Возможно удаляет не все, что нужно т.к. перешли от счетов к Коду АУ
	-- Надо добавить логику, что при флаге deleted все зависимые сущности удаляются
    -- И добавить везде ограничение по коду АУ.
    -- Очищаем старую индексацию
	DELETE FROM public.rul_indexing
    	WHERE invoice_id = p_invoice_id
    	AND index_date >= p_start_date
        AND index_date <= p_end_date;
    DELETE FROM public.rul_indexing
    	 WHERE transaction_reversal_id IN
            (   SELECT rtr.transaction_reversal_id
                from rul_transaction_reversal rtr
                join rul_transaction rt_reversal
                    on rtr.source_correlation_transaction_id = rt_reversal.correlation_transaction_id
                join rul_transaction rt_storn
                    on rtr.storn_correlation_transaction_id = rt_storn.correlation_transaction_id
                where rt_reversal.operation_date >= p_start_date
                and rt_reversal.operation_date <= p_end_date
                order by rt_reversal.operation_date
            )
            OR
            transaction_reversal_id IN (SELECT rtr.transaction_reversal_id
                							FROM rul_transaction_reversal rtr
                                            WHERE deleted = 1::smallint)
            ;
    -- Очищаем старую пеню
	DELETE FROM public.rul_penalty
    	--WHERE source_invoice_id = p_invoice_id
    	WHERE invoice_id = p_invoice_id
    	AND start_date >= p_start_date
        AND end_date <= p_end_date;
    -- Удаляем пени относящиеся к сторнированию по месяцам
    DELETE FROM public.rul_penalty
    	 WHERE transaction_reversal_id IN
            (   SELECT rtr.transaction_reversal_id
                from rul_transaction_reversal rtr
                join rul_transaction rt_reversal
                    on rtr.source_correlation_transaction_id = rt_reversal.correlation_transaction_id
                join rul_transaction rt_storn
                    on rtr.storn_correlation_transaction_id = rt_storn.correlation_transaction_id
                where rt_reversal.operation_date >= p_start_date
                and rt_reversal.operation_date <= p_end_date
                order by rt_reversal.operation_date
            )
            OR
            transaction_reversal_id  IN (SELECT rtr.transaction_reversal_id
                							FROM rul_transaction_reversal rtr
                                            WHERE deleted = 1::smallint);
    -- Удаляем все погашения
    DELETE FROM rul_transaction_transaction
    	WHERE operation_date >= p_start_date
        AND   operation_date <= p_end_date
        AND   (
        		credit_transaction_id in
                	(
                    	SELECT transaction_version_id FROM rul_transaction_version
                        WHERE transaction_id IN
                          (
                              SELECT transaction_id FROM rul_transaction
                              WHERE (code_ay =
                                  (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = p_invoice_id)
                                  ) OR code_ay is null)
                              AND operation_date >= p_start_date
                              AND operation_date <= p_end_date
                          )
                    )
                OR
                debit_transaction_id in
                	(
                    	SELECT transaction_version_id FROM rul_transaction_version
                        WHERE transaction_id IN
                          (
                              SELECT transaction_id FROM rul_transaction
                              WHERE (code_ay =
                                  (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = p_invoice_id)
                                  ) OR code_ay is null)
                              AND operation_date >= p_start_date
                              AND operation_date <= p_end_date
                          )
                    )
        	  )
    ;
    --Удаляются все погашения проводок относящиеся к сторнированию
    DELETE FROM rul_transaction_transaction
    WHERE debit_transaction_id IN (SELECT transaction_version_id FROM rul_transaction_version WHERE transaction_reversal_id IN
    								(   SELECT rtr.transaction_reversal_id
                                        from rul_transaction_reversal rtr
                                        join rul_transaction rt_reversal
                                            on rtr.source_correlation_transaction_id = rt_reversal.correlation_transaction_id
                                        join rul_transaction rt_storn
                                            on rtr.storn_correlation_transaction_id = rt_storn.correlation_transaction_id
                                        where rt_reversal.operation_date >= p_start_date
                                        and rt_reversal.operation_date <= p_end_date
                                        order by rt_reversal.operation_date
                                    )
                                    OR
                                    transaction_reversal_id IN (SELECT rtr.transaction_reversal_id
                                                                    FROM rul_transaction_reversal rtr
                                                                    WHERE deleted = 1::smallint)
                                    );
    DELETE FROM rul_transaction_transaction
    WHERE credit_transaction_id IN (SELECT transaction_version_id FROM rul_transaction_version WHERE transaction_reversal_id IN
    								(   SELECT rtr.transaction_reversal_id
                                        from rul_transaction_reversal rtr
                                        join rul_transaction rt_reversal
                                            on rtr.source_correlation_transaction_id = rt_reversal.correlation_transaction_id
                                        join rul_transaction rt_storn
                                            on rtr.storn_correlation_transaction_id = rt_storn.correlation_transaction_id
                                        where rt_reversal.operation_date >= p_start_date
                                        and rt_reversal.operation_date <= p_end_date
                                        order by rt_reversal.operation_date
                                    )
                                    OR
                                    transaction_reversal_id IN (SELECT rtr.transaction_reversal_id
                                                                    FROM rul_transaction_reversal rtr
                                                                    WHERE deleted = 1::smallint)
                                    );
    -- Удаляем все версии транзакций по прямым погашениям
    -- Проблема, нужно не удалилить версии исходных проводок.
    -- Ну или нужно чтобы при рассчете создавались версии проводок для всех, у которых нет версии.
    -- Сделал чтобы создавались новые версии проводок внизу, так что должно работать ок
    DELETE FROM rul_transaction_version
    	WHERE month = date_trunc('month',p_start_date)
        AND transaction_id in (SELECT transaction_id FROM rul_transaction WHERE code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = p_invoice_id)));
    -- Удаляем все версии транзакций по сторнированным погашениям
    DELETE FROM rul_transaction_version WHERE transaction_reversal_id IN
      (SELECT rtr.transaction_reversal_id
      from rul_transaction_reversal rtr
      join rul_transaction rt_reversal
          on rtr.source_correlation_transaction_id = rt_reversal.correlation_transaction_id
      join rul_transaction rt_storn
          on rtr.storn_correlation_transaction_id = rt_storn.correlation_transaction_id
      where rt_reversal.operation_date >= p_start_date
      and rt_reversal.operation_date <= p_end_date
      order by rt_reversal.operation_date)
      OR transaction_reversal_id IN (SELECT rtr.transaction_reversal_id
                							FROM rul_transaction_reversal rtr
                                            WHERE deleted = 1::smallint);
    DELETE FROM rul_transaction_reversal WHERE deleted = 1::smallint;
    -- Затем нужно проставить флаги актуальности новые
    UPDATE rul_transaction_version SET is_actual = true
    WHERE transaction_version_id IN
    (
      SELECT transaction_version_id FROM (
        SELECT transaction_version_id, row_number() over (partition by transaction_id, month order by transaction_version_id desc, month desc) as rn
        FROM rul_transaction_version) actual
      WHERE actual.rn = 1
    );
    -- Удалить все транзакции системные (Почему не все? Чтобы не проводить заново импорт)
    DELETE FROM rul_transaction
    	WHERE 1=1
        AND   operation_date >= p_start_date
        AND   operation_date <= p_end_date
        --AND   operation_id IN (SELECT operation_id FROM rul_operation WHERE invoice_id = p_invoice_id
        --					   AND operation_date >= p_start_date
        --					   AND operation_date <= p_end_date)
        AND   is_system = true
        AND code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = p_invoice_id)
		);
    DELETE FROM rul_transaction WHERE deleted = 1::smallint;
    -- Удаление операций по счету за расчетный месяц
    DELETE FROM rul_operation
    	WHERE operation_date >= p_start_date
        AND   operation_date <= p_end_date
        AND   invoice_id = p_invoice_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_indexing(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint)
CREATE OR REPLACE PROCEDURE public.create_indexing(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint DEFAULT NULL::bigint)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_code_ay varchar;
BEGIN
    v_code_ay := (select code_ay from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id));
    -- Создаем индексацию по счету
    -- Выбираются все погашенные проводки в текущем периоде по этому счету и формируются индексации.
    INSERT INTO
      public.rul_indexing
    (
      charge_id,
      percent_index_consumption,
      index_date,
      currency_rate,
      index_value,
      index_coefficient,
      index_amount,
      index_nds,
      invoice_id,
      transaction_transaction_id,
      transaction_reversal_id,
      percent_charge_repayment,
      index_operation_code
    )
    select a.charge_id,
	   round(a.percent,2),
       a.index_date,
       a.current_currency_rate,
	   round(a.base_value * (1 + a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate),4) as rate_value,
       round(1 + a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate,6) as coef,
       round(
       (
       --(a.amount / a.base_value)
       --a.amount_nds
       a.charge_amount
       --/
       --((a.nds_percent + 100) / 100)
       )
       * a.percent
       --* (a.base_value * (1 + a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate) - a.base_value)
       * (a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate)
       / 100
       ,2)
       -- Отнимаю все суммы индексаций по этому начислению ??
       -- Это попытка создания именно разницы между индексациями при сторнировании
       -- Скорее всего неправильно. НУжно просто в конце сторнирования заводить для этого сторнировани отрицательные строки по всем предыдущим
       -- - case when p_storn_id is not null then (select sum(index_amount) from rul_indexing where charge_id = a.charge_id) else 0 end
       as amount
       ,
       round((a.nds_percent / 100) *
       round(
       (
       --(a.amount / a.base_value)
       --a.amount_nds
       a.charge_amount
       --/
       --((a.nds_percent + 100) / 100)
       )
       * a.percent
       --* (a.base_value * (1 + a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate) - a.base_value)
       * (a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate)
       / 100
       ,2)
       /*(((a.amount / a.base_value) / ((a.nds_percent + 100) / 100)) * --a.percent *
       (a.base_value * (1 + a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate) - a.base_value))
       / a.cnt*/
       ,2)
       -- - case when p_storn_id is not null then (select sum(index_amount) from rul_indexing where charge_id = a.charge_id) else 0 end)
       as amount_nds,
       --a.invoice_id,
       -- По идее прявязывается индексация к текущему счету, даже если оплата была в прошлом
       p_invoice_id,
       a.transaction_transaction_id,
       p_storn_id,
       round(sum(round(a.percent,2)) over (partition by a.charge_id order by index_date,a.transaction_transaction_id) + (select coalesce(sum(percent_index_consumption),0) from rul_indexing where charge_id = a.charge_id),2),
       (select code from rul_operation_template where operation_template_id =
           (select indexing_operation_template_id from rul_connection where connection_id =
              (select connection_id from rul_charge where charge_id = a.charge_id)))
    from (
        select rc.charge_id,
        rtt.amount * 100 / sum(coalesce(rc.amount_nds,0)) over (partition by rtt.transaction_transaction_id) as percent,
        -- Надо брать актуальную версию для нужного месяца (пока не так)
        (select currency_rate from rul_currency_rate
            where currency_rate_date = date_trunc('day',
            	(select rt.operation_date from rul_transaction rt
                  join rul_transaction_version rtv
                    on rtt.credit_transaction_id = rtv.transaction_version_id
                    and rt.transaction_id = rtv.transaction_id
                    --and rtv.is_actual is true
                    --and date_trunc('month',rt.operation_date) = rtv.month
                 ))
        and currency_code = 'RUB') as current_currency_rate,
        --date_trunc('day',rtt.operation_date) as index_date,
        (select rt.operation_date from rul_transaction rt
                  join rul_transaction_version rtv
                    on rtt.credit_transaction_id = rtv.transaction_version_id
                    and rt.transaction_id = rtv.transaction_id
                    --and rtv.is_actual is true
                    --and date_trunc('month',rt.operation_date) = rtv.month
                 ) as index_date,
        rc.currency_rate as charge_currency_rate,
        rc.cost_factor,
        rc.base_value,
        rtt.amount,
        rc.sum_consumption,
        rc.nds_percent,
        --rc.invoice_id,
        -- Индексации должны выставляться в счет на дату оплаты
        -- Вариант ниже закоммитил, т.к. при сторнирвоании индексация должна выставиться в текущий счет, а не в предыдущие.
        /*(select invoice_id from rul_invoice
        	where billing_start_date <= rtt.operation_date
            and billing_end_date >= rtt.operation_date
            and invoice_group_index = (select invoice_group_index from rul_invoice where invoice_id = p_invoice_id)
            and agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)
            limit 1
        )*/
        p_invoice_id as invoice_id,
        rtt.transaction_transaction_id,
        rc.cnt,
        rc.amount as charge_amount,
        rc.amount_nds
        from rul_transaction_transaction rtt
        join rul_transaction_version rtv
        	on rtt.debit_transaction_id = rtv.transaction_version_id
            --and rtv.is_actual is true
        join rul_transaction rt
            on rtv.transaction_id = rt.transaction_id
            and date_trunc('month',rtt.operation_date) = rtv.month
            and rt.code_ay = v_code_ay
        join rul_operation ro
            on ro.operation_id = rt.operation_id
        join
          (
          SELECT
            charge_id,connection_id,sum_consumption,base_value,amount,nds_percent,note,start_date,end_date,deleted,billing_start_date,
            billing_end_date,charge_type_id,amount_nds,nds_rub,charge_checked,need_recount,invoice_id,source_id,currency_rate,cost_factor,
            count(charge_id) over (partition by invoice_id) as cnt
          FROM
              public.rul_charge
          WHERE
          	 sum_consumption != 0
          ) rc
            on rc.invoice_id = ro.invoice_id
        join rul_invoice ri
        	on ri.invoice_id = rc.invoice_id
        WHERE 1=1
        	--AND rc.invoice_id = p_invoice_id
        	--AND rtt.operation_date >= p_start_date
        	--AND rtt.operation_date <= p_end_date
            AND ro.operation_template_id != (select indexing_operation_template_id from rul_connection where connection_id = rc.connection_id)
            AND
            (
              ((rtt.operation_date >= p_start_date
              AND rtt.operation_date <= p_end_date) OR p_storn_id is not null)
              -- Если передается айди сторнирования, то рассчитываем именно сторнирование.
              -- Без айди сторнирования считаем как обычно
              AND
              	(
                  rtt.debit_transaction_id in
                    (
                    	select transaction_version_id from rul_transaction_version where (transaction_reversal_id = p_storn_id or p_storn_id is null)
                    )
                  OR rtt.credit_transaction_id in
                  	(
                    	select transaction_version_id from rul_transaction_version where (transaction_reversal_id = p_storn_id or p_storn_id is null)
                    )
                )
            )
        ) a
        WHERE (a.base_value * (1 + a.cost_factor * (a.current_currency_rate - a.charge_currency_rate)/a.charge_currency_rate) - a.base_value) != 0
        order by a.charge_id,a.transaction_transaction_id;
    -- Заводим клонированные индексации для текущих счетов.
    -- Для этого выбираем все начисления, в которых поучаствовало сторнирование. И клонируем все записи с -
    IF p_storn_id IS NOT NULL
    THEN
    	INSERT INTO
          public.rul_indexing
        (
          charge_id,
          percent_index_consumption,
          index_date,
          currency_rate,
          index_value,
          index_coefficient,
          index_amount,
          index_nds,
          invoice_id,
          transaction_transaction_id,
          transaction_reversal_id,
          is_clone
        )
        SELECT charge_id,
          percent_index_consumption,
          index_date,
          currency_rate,
          index_value,
          index_coefficient,
          - index_amount,
          - index_nds,
          p_invoice_id,
          transaction_transaction_id,
          transaction_reversal_id,
          1::SMALLINT
        FROM rul_indexing
        WHERE 1=1
       	 	-- Выбираем все начисления, которые затронуло сторнирование
        	AND charge_id IN (SELECT DISTINCT charge_id FROM rul_indexing WHERE transaction_reversal_id = p_storn_id)
            AND transaction_reversal_id != p_storn_id -- Не берем текущие индексации
            ;
    END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_indexing_transaction(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint)
CREATE OR REPLACE PROCEDURE public.create_indexing_transaction(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint DEFAULT NULL::bigint)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Создание новых операций по счету за расчетный месяц по операциям начислений
    -- Когда сторнирование не создаем операцию, т.к. она типо уже есть. Спорно
    INSERT INTO
      public.rul_operation
    (
      operation_template_id,
      operation_date,
      invoice_id
    )
	select rcc.indexing_operation_template_id, p_end_date, p_invoice_id
	from rul_charge rc
    join rul_connection rcc
    	on rcc.connection_id = rc.connection_id
    join rul_indexing ri
    	on ri.charge_id = rc.charge_id
    where rcc.indexing_operation_template_id is not null
    	and rc.invoice_id = p_invoice_id
        and ri.index_date >= p_start_date
        and ri.index_date <= p_end_date
        and p_storn_id is null
    group by rcc.indexing_operation_template_id;
    -- Создание проводок по индексации за расчетный месяц
	INSERT INTO
      public.rul_transaction
    (
      client_id,
      operation_id,
      subconto_type_id,
      content,
      amount,
      transaction_type_id,
      create_date,
      operation_date,
      transaction_template_id,
      debit_subinvoice,
      credit_subinvoice,
      code_ay,
      document,
      operation_code,
      calculated_date
    )
    select  (select supplier_client_id from rul_agreement where rcc.agreement_id = agreement_id)
            , (select operation_id from rul_operation
            	where invoice_id = p_invoice_id
                AND operation_template_id = rcc.indexing_operation_template_id
                AND operation_date >= date_trunc('month',MIN(ri.index_date))
        		AND operation_date <= date_trunc('month',MIN(ri.index_date)) + interval '1 month')
            , 2
            , rtt.description
            , case when rtt.source_data_transaction_id = 1 then sum(ri.index_amount)
                   when rtt.source_data_transaction_id = 2 then sum(ri.index_nds)
                   when rtt.source_data_transaction_id = 3 then sum(ri.index_amount + ri.index_nds)
                end
            , 1
            , CURRENT_TIMESTAMP::timestamp(0)
            , case when p_storn_id is null then date_trunc('month',ri.index_date) + interval '1 month' - interval '1 second'
            else (select min(operation_date) from rul_transaction where correlation_transaction_id =
            		(select source_correlation_transaction_id from rul_transaction_reversal
                    	where transaction_reversal_id = p_storn_id)) end
            , rtt.transaction_template_id
            , rtt.debit_subinvoice
            , rtt.credit_subinvoice
            , (select code_ay from rul_agreement where agreement_id = rcc.agreement_id)
            , (select invoice_code from rul_invoice where invoice_id = p_invoice_id)
            , rot.code
            , case when p_storn_id is null then date_trunc('month',ri.index_date) + interval '1 month' - interval '1 second'
            else (select min(operation_date) from rul_transaction where correlation_transaction_id =
            		(select source_correlation_transaction_id from rul_transaction_reversal
                    	where transaction_reversal_id = p_storn_id)) end
    from rul_charge rc
    join rul_connection rcc
        on rcc.connection_id = rc.connection_id
    join rul_operation_template rot
        on rcc.indexing_operation_template_id = rot.operation_template_id
    join rul_transaction_template rtt
        on rtt.operation_template_id = rot.operation_template_id
    join rul_indexing ri
    	on ri.charge_id = rc.charge_id
        and (ri.transaction_reversal_id = p_storn_id or p_storn_id is null)
    where rc.invoice_id = p_invoice_id
    	and ((ri.index_date >= p_start_date and ri.index_date <= p_end_date) or p_storn_id is not null)
    group by rcc.indexing_operation_template_id, rtt.transaction_template_id, rcc.agreement_id, date_trunc('month',ri.index_date),rot.code;
    -- Проставляем сквозной айди для транзакций.
    update rul_transaction set correlation_transaction_id = transaction_id
    where correlation_transaction_id is null
    and is_system = true;
    -- Версию погашения нужно доделать. Но она должна как-то в следующий месяц создаваться вроде как.
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_invoices(IN p_charge_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.create_invoices(IN p_charge_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
    DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
	--DELETE FROM rul_invoice WHERE billing_start_date >= p_start_date
    --						  AND billing_end_date <= p_end_date
    --                          AND agreement_id = p_agreement_id;
      SELECT
            COUNT(*) INTO v_cnt
            FROM rul_charge rc
            JOIN rul_connection rco ON rc.connection_id = rco.connection_id
            WHERE rc.billing_start_date >= p_start_date
            AND rc.charge_checked =0
            AND rc.billing_end_date <= p_end_date
            AND rco.connection_id IN (SELECT DISTINCT connection_id FROM rul_charge WHERE charge_id = ANY(p_charge_id))
            AND rc.invoice_id IS NULL
            AND (
            rc.charge_id = ANY (p_charge_id)
            OR (
                rc.charge_type_id = 1
                AND rc.source_id != 4
            )
      );
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_CREATE_INVOICE_CHARGE');
	END IF;
SELECT string_agg(i.invoice_code, ', ')
INTO v_err_name
FROM rul_charge rc
JOIN rul_connection r
    ON rc.connection_id = r.connection_id
JOIN rul_invoice i
    ON i.agreement_id = r.agreement_id
   AND i.invoice_group_index = r.invoice_group_index
WHERE rc.charge_id = ANY (p_charge_id)
  AND i.billing_start_date <= p_start_date
  AND i.billing_end_date   >= p_end_date;
IF v_err_name IS NOT NULL THEN
    RAISE EXCEPTION '%',
        get_message('ERR_CREATE_INVOICE_CHARGE_INVOICE', v_err_name);
END IF;
        WITH time_calcs AS (
            SELECT
                rc.charge_id,
                rc.connection_id,
                rco.agreement_id,
                rc.billing_start_date,
                rc.billing_end_date,
                rc.invoice_group_index,
                -- Для плановых потреблений, каждый счет должен создаваться уникальным относительно каждого начисления
                -- Все остальные типы начислений агрегируются согласно заложенной логике
                case when rc.source_id = 4 then rc.charge_id else 1 end as weight
            FROM rul_charge rc
            JOIN rul_connection rco ON rc.connection_id = rco.connection_id
            WHERE rc.billing_start_date >= p_start_date
            AND rc.billing_end_date <= p_end_date
            AND rco.connection_id IN (SELECT DISTINCT connection_id FROM rul_charge WHERE charge_id = ANY(p_charge_id))
            AND rc.invoice_id IS NULL
            AND (
            rc.charge_id = ANY (p_charge_id)
            OR (
                rc.charge_type_id = 1
                AND rc.source_id != 4
            )
      )
        ),
        unique_groups AS (
            SELECT DISTINCT
                agreement_id,
                billing_start_date,
                billing_end_date,
                invoice_group_index,
                weight,
                nextval('rul_invoice_invoice_id_seq') AS invoice_id
            FROM time_calcs
        )
        UPDATE rul_charge rc
        SET invoice_id = ug.invoice_id
        FROM time_calcs tc
        JOIN unique_groups ug
            ON tc.agreement_id = ug.agreement_id
            AND tc.weight = ug.weight
            AND tc.billing_start_date = ug.billing_start_date
            AND tc.billing_end_date = ug.billing_end_date
            AND tc.invoice_group_index = ug.invoice_group_index
        WHERE rc.charge_id = tc.charge_id;
        INSERT INTO
          public.rul_invoice
        (
          invoice_id,
          invoice_code,
          agreement_id,
          billing_start_date,
          billing_end_date,
          create_date,
          sum_amount,
          sum_amount_unnds,
          sum_nds,
          penalty,
          total_amount,
          invoice_group_index,
          invoice_type_id
        )
        select
        	rc.invoice_id,
            -- СФ[Отчетный месяц в формате ГГММ]-[Сквозной порядковый номер внутри отдельного поставщика]
            'СФ'||to_char(p_start_date,'YYMM')||'-'||
            (COALESCE((select max( case when SPLIT_PART(invoice_code, '-', 2) = '' then 0::BIGINT
		    							when SPLIT_PART(invoice_code, '-', 2) is null then 0::BIGINT
            							else SPLIT_PART(invoice_code, '-', 2)::BIGINT end )
            	from rul_invoice
            	where agreement_id in
                	(
                    	select agreement_id from rul_agreement where supplier_client_id = ra.supplier_client_id
                    )
            ),0) + row_number() over (partition by ra.supplier_client_id))::varchar,
            rco.agreement_id,
            rc.billing_start_date,
            rc.billing_end_date,
        	p_end_date,
            sum(rc.amount_nds_new),
            sum(rc.amount_unnds_new),
            sum(rc.nds_rub),
            (select penalty from rul_agreement where agreement_id = rco.agreement_id),
            sum(rc.amount_nds_new) + sum(rc.nds_rub) + sum(rc.amount_unnds_new),
            rc.invoice_group_index,
            case when max(rc.source_id) = 4 then 2 else 1 end -- так разделил плановые и не плановые счета, криво но быстро
        from (select *, case when nds_percent is null then 0 else amount end as amount_nds_new,
     					case when nds_percent is null then amount else 0 end as amount_unnds_new
              from rul_charge) rc
        join rul_connection rco
        	on rc.connection_id = rco.connection_id
        join rul_agreement ra
        	on ra.agreement_id = rco.agreement_id
        where rc.billing_start_date >= p_start_date
        and rc.billing_end_date <= p_end_date
        AND rco.connection_id IN (SELECT DISTINCT connection_id FROM rul_charge WHERE charge_id = ANY(p_charge_id))
        AND rc.invoice_id NOT IN (SELECT invoice_id FROM rul_invoice)
        group by rc.invoice_id,rco.agreement_id,rc.billing_start_date,rc.billing_end_date,rc.invoice_group_index,ra.supplier_client_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_line_parameter(IN p_json json)
CREATE OR REPLACE PROCEDURE public.create_line_parameter(IN p_json json)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_line_id BIGINT := (p_json->>'line_id')::BIGINT;
    v_del BIGINT[];
    v_cnt bigint:=0;
    v_user_id bigint :=  (select case when current_setting('user_ctx.user_name',true) = '' then null else current_setting('user_ctx.user_name',true) end )::bigint;
    row record;
BEGIN
	CREATE TEMP TABLE new_data AS
    WITH input AS (
        SELECT p_json AS data
    ),
    line_params AS (
        SELECT
            (data ->> 'line_id')::BIGINT AS line_id,
            lp.value AS line_param
        FROM input
        CROSS JOIN LATERAL json_array_elements(data -> 'line_parameters') AS lp
    ),
    children AS (
        SELECT
            line_id,
            (line_param ->> 'line_parameter_id')::BIGINT AS line_parameter_id,
            (line_param ->> 'node_calculate_parameter_id')::BIGINT AS parent_node_calculate_parameter_id,
            (line_param ->> 'formula_id')::BIGINT AS formula_id,
            child.value AS child
        FROM line_params
        CROSS JOIN LATERAL json_array_elements(line_param -> 'line_parameter_children') AS child
    ),
    parent_groups AS (
        SELECT DISTINCT
            parent_node_calculate_parameter_id,
            line_parameter_id
        FROM children
    ),
    generated_ids AS (
        SELECT
            parent_node_calculate_parameter_id,
            CASE
                WHEN line_parameter_id IS NULL THEN
                    nextval('rul_line_parameter_line_parameter_id_seq')
                ELSE
                    line_parameter_id
            END AS effective_line_parameter_id
        FROM parent_groups
    ),
    final_result AS (
        SELECT
            c.line_id,
            g.effective_line_parameter_id AS line_parameter_id,
            c.parent_node_calculate_parameter_id,
            c.formula_id,
            CASE
                WHEN (c.child ->> 'line_parameter_child_id')::BIGINT IS NULL THEN
                    nextval('rul_line_parameter_child_line_parameter_child_id_seq')
                ELSE
                    (c.child ->> 'line_parameter_child_id')::BIGINT
            END AS line_parameter_child_id,
            (c.child ->> 'node_calculate_parameter_id')::BIGINT AS child_node_calculate_parameter_id
        FROM children c
        JOIN generated_ids g
            ON c.parent_node_calculate_parameter_id = g.parent_node_calculate_parameter_id
    )
    SELECT final_result.*, a.line_parameter_id as lp, b.line_parameter_child_id as lpc,child_node_calculate_parameter_id as cncpi,parent_node_calculate_parameter_id as pncpi,
    CASE
        WHEN a.line_parameter_id IS NOT NULL AND final_result.line_parameter_id IS NULL THEN 'd'
        WHEN a.line_parameter_id IS NULL AND final_result.line_parameter_id IS NOT NULL THEN 'i'
        WHEN a.line_parameter_id IS NOT NULL AND final_result.line_parameter_id IS NOT NULL THEN 'u'
    END as operation_lp,
    CASE
        WHEN b.line_parameter_child_id IS NOT NULL AND final_result.line_parameter_child_id IS NULL THEN 'd'
        WHEN b.line_parameter_child_id IS NULL AND final_result.line_parameter_child_id IS NOT NULL THEN 'i'
        WHEN b.line_parameter_child_id IS NOT NULL AND final_result.line_parameter_child_id IS NOT NULL THEN 'u'
    END as operation_lpc,
    CASE
        WHEN a.formula_id IS NOT NULL AND final_result.formula_id IS NULL THEN 'd'
        WHEN a.formula_id IS NULL AND final_result.formula_id IS NOT NULL THEN 'i'
        WHEN a.formula_id <> final_result.formula_id  THEN 'u'
    END as formula_lp
    FROM final_result
    FULL JOIN
    (SELECT rlp.line_parameter_id,rlp.formula_id
    FROM rul_line rl
    JOIN rul_line_parameter rlp
        ON rl.line_id = rlp.line_id
    WHERE rl.line_id = v_line_id ) a
    ON a.line_parameter_id = final_result.line_parameter_id
    FULL JOIN
    (SELECT rlpc.line_parameter_child_id
    FROM rul_line rl
    JOIN rul_line_parameter rlp
        ON rl.line_id = rlp.line_id
    JOIN rul_line_parameter_child rlpc
        ON rlp.line_parameter_id = rlpc.line_parameter_id
    WHERE rl.line_id = v_line_id ) b
    ON b.line_parameter_child_id = final_result.line_parameter_child_id;
	SELECT COUNT(*)
    INTO v_cnt
    FROM new_data nd
     JOIN rul_node_calculate_parameter rncp1
        ON nd.cncpi = rncp1.node_calculate_parameter_id
    JOIN rul_node rn
        ON rncp1.node_id = rn.node_id
     JOIN rul_node_calculate_parameter rncp2
        ON nd.pncpi = rncp2.node_calculate_parameter_id
    JOIN rul_node rn2
        ON rncp2.node_id = rn2.node_id
    WHERE rn2.node_type_id = 2 and rn.node_type_id != rn2.node_type_id;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_CREATE_LINE_PARAMETER_COMMERCIAL');
	END IF;
	SELECT array_agg(DISTINCT line_parameter_id)
    INTO v_del
    FROM new_data
    WHERE formula_lp IS NOT NULL;
    IF v_del IS NOT NULL
    THEN
        CALL delete_attribute_section_value(v_del);
    END IF;
    SELECT array_agg(lp) INTO v_del
    FROM new_data
    WHERE operation_lp = 'd';
    CALL delete_entity('line_parameter', v_del);
    SELECT array_agg(lpc) INTO v_del
    FROM new_data
    WHERE operation_lpc = 'd';
    CALL delete_entity('line_parameter_child', v_del);
    INSERT INTO
      public.rul_line_parameter
    (
      line_parameter_id,
      line_id,
      node_calculate_parameter_id,
      op_user_id,
      op_date,
      formula_id
    )
    SELECT line_parameter_id, line_id, parent_node_calculate_parameter_id,v_user_id,now(),formula_id
    FROM new_data
    WHERE operation_lp IN ('i','u')
    GROUP BY line_parameter_id, line_id, parent_node_calculate_parameter_id,formula_id
    ON CONFLICT (line_parameter_id)
    DO UPDATE SET
    	line_id = EXCLUDED.line_id,
    	node_calculate_parameter_id = EXCLUDED.node_calculate_parameter_id,
      	op_user_id = EXCLUDED.op_user_id,
      	op_date = EXCLUDED.op_date,
      	formula_id = EXCLUDED.formula_id
    ;
    INSERT INTO
      public.rul_line_parameter_child
    (
      line_parameter_child_id,
      line_parameter_id,
      node_calculate_parameter_id,
      op_user_id,
      op_date
    )
    SELECT line_parameter_child_id, line_parameter_id, child_node_calculate_parameter_id,v_user_id,now()
    FROM new_data
    WHERE operation_lpc IN ('i','u')
    ON CONFLICT (line_parameter_child_id)
    DO UPDATE SET
    	line_parameter_id = EXCLUDED.line_parameter_id,
    	node_calculate_parameter_id = EXCLUDED.node_calculate_parameter_id,
      	op_user_id = EXCLUDED.op_user_id,
      	op_date = EXCLUDED.op_date
    ;
    SELECT array_agg(child_node_calculate_parameter_id) INTO v_del
    FROM new_data
    WHERE child_node_calculate_parameter_id IS NOT NULL;
		IF v_del IS NOT NULL
		THEN
			CALL define_balancing_node(v_del);
		END IF;
    SELECT array_agg(parent_node_calculate_parameter_id) INTO v_del
    FROM new_data
    WHERE parent_node_calculate_parameter_id IS NOT NULL;
		IF v_del IS NOT NULL
		THEN
			CALL define_balancing_node(v_del);
		END IF;
    DROP TABLE new_data;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_penalty(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint, IN p_storn_operation_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.create_penalty(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint DEFAULT NULL::bigint, IN p_storn_operation_date timestamp without time zone DEFAULT NULL::timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	month record;
    v_code_ay varchar;
BEGIN
	v_code_ay := (select code_ay from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id));
	-- В пене не должны участвовать импортированные проводки
    -- (Вроде как учтено, т.к. есть джоин по операциям, а операции есть только у системных проводок)
    -- По факту оплаты
    -- Берем все погашенные проводки за месяц, где дебетовая проводка это проводка оплаты начисления в рамках определенного счета.
    -- И для них рассчитываем пеню на основании дат оплаты (дата операции кредитовой проводки) и даты выставления проводки по начислению с учетом
    -- количества дней на оплату
	INSERT INTO
      public.rul_penalty
    (
      start_date,
      end_date,
      amount,
      source_invoice_id,
      penalty_type_id,
      penalty_value,
      penalty_nds_value,
      invoice_id,
      transaction_transaction_id,
      transaction_reversal_id,
      penalty
    )
    select GREATEST(date_trunc('month',rtt.operation_date),rt_debit.operation_date + interval '1 day' *
        (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))) as start_date,
        LEAST(rt_credit.operation_date,p_end_date) as end_date,
        rtt.amount as calculate_amount,
        -- Ищется исходя из даты дебетовой операции
        ro.invoice_id as source_invoice_id,
        1 as penalty_type,
        rtt.amount * (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)) *
        (extract (day from (LEAST(rt_credit.operation_date,p_end_date) -
        GREATEST(date_trunc('month',rtt.operation_date),rt_debit.operation_date + interval '1 day' *
        (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)))
        ))+1) * (select (sum_amount-sum_nds)/(sum_amount + sum_amount_unnds) from rul_invoice where invoice_id = p_invoice_id) as amount,
        rtt.amount * (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)) *
        (extract (day from (LEAST(rt_credit.operation_date,p_end_date) -
        GREATEST(date_trunc('month',rtt.operation_date),rt_debit.operation_date + interval '1 day' *
        (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)))
        ))+1) * (select sum_nds/(sum_amount + sum_amount_unnds) from rul_invoice where invoice_id = p_invoice_id) as amount_nds,
        -- Индексации должны выставляться в счет на дату оплаты
        (select invoice_id from rul_invoice
        	where billing_start_date <= rtt.operation_date
            and billing_end_date >= rtt.operation_date
            and invoice_group_index = (select invoice_group_index from rul_invoice where invoice_id = p_invoice_id)
            and agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)
            limit 1
        ) as include_invoice_id,
        rtt.transaction_transaction_id,
      	p_storn_id,
        (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))
    from rul_transaction_transaction rtt
    join rul_transaction_version rtv_credit
    	on rtt.credit_transaction_id = rtv_credit.transaction_version_id
    join rul_transaction rt_credit
        on rtv_credit.transaction_id = rt_credit.transaction_id
        and rt_credit.code_ay = v_code_ay
        -- Проверка актуальности не нужна, т.к. привязано все напрямую к версиям
        --and rtv_credit.is_actual is TRUE
        --and date_trunc('month',rt_credit.operation_date) = rtv_credit.month
    join rul_transaction_version rtv_debit
    	on rtt.debit_transaction_id = rtv_debit.transaction_version_id
    join rul_transaction rt_debit
        on rtv_debit.transaction_id = rt_debit.transaction_id
        and rt_debit.code_ay = v_code_ay
        -- Проверка актуальности не нужна, т.к. привязано все напрямую к версиям
        --and rtv_debit.is_actual is TRUE
        --and date_trunc('month',rt_debit.operation_date) = rtv_debit.month
    join rul_operation ro
        on ro.operation_id = rt_debit.operation_id
    join rul_operation_template rot
        on	rot.operation_template_id = ro.operation_template_id
    where rt_debit.operation_date + interval '1 day' *
        (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))
    < rt_credit.operation_date
    and rot.operation_type_id in (1,3)
    --and ro.invoice_id = p_invoice_id
    --and rtt.operation_date >= p_start_date
    --and rtt.operation_date <= p_end_date
    AND
            (
              ((rtt.operation_date >= p_start_date
              AND rtt.operation_date <= p_end_date) OR p_storn_id is not null)
              -- Если передается айди сторнирования, то рассчитываем именно сторнирование.
              -- Без айди сторнирования считаем как обычно
              AND
              	(
                  rtt.debit_transaction_id in
                    (
                    	select transaction_version_id from rul_transaction_version where (transaction_reversal_id = p_storn_id or p_storn_id is null)
                    )
                  OR rtt.credit_transaction_id in
                  	(
                    	select transaction_version_id from rul_transaction_version where (transaction_reversal_id = p_storn_id or p_storn_id is null)
                    )
                )
            )
    ;
    -- По закрытию периода
    -- Выбираем все непогашенные "дебетовые" проводки по оплате начислений (те у которых не 100 процентов погашения)
    -- Выставляем пеню на остаток.
    IF p_storn_id is not null
    THEN
    	-- Цикл по месяцам, чтобы на все непогашенные остатки в каждом месяце рассчитать пеню (Не факт что это правильно)
    	FOR month IN SELECT
            date_trunc('month', d)::date AS start_date,
            (date_trunc('month', d) + interval '1 month' - interval '1 day')::date AS end_date
        FROM
            generate_series(date_trunc('month', p_storn_operation_date),date_trunc('month', p_end_date) - interval '1 month','1 month') AS d
        LOOP
          INSERT INTO
            public.rul_penalty
          (
            start_date,
            end_date,
            amount,
            source_invoice_id,
            penalty_type_id,
            penalty_value,
            penalty_nds_value,
            invoice_id,
            transaction_transaction_id,
            transaction_reversal_id,
            penalty
          )
          select GREATEST(month.start_date,rt_debit.operation_date + interval '1 day' *
              (select coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))) as start_date,
              month.end_date as end_date,
              rt_debit.amount * (1 - rtv_debit.payment_percent / 100),
              ro.invoice_id,
              2, -- По закрытию периода
              rt_debit.amount * (1 - rtv_debit.payment_percent / 100) * (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)) *
              (extract (day from (month.end_date -  GREATEST(month.start_date,rt_debit.operation_date + interval '1 day' *
              (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)))))
               +1)* (select (sum_amount-sum_nds)/(sum_amount + sum_amount_unnds) from rul_invoice where invoice_id = p_invoice_id),
              rt_debit.amount * (1 - rtv_debit.payment_percent / 100) * (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)) *
              (extract (day from (month.end_date -  GREATEST(month.start_date,rt_debit.operation_date + interval '1 day' *
              (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)))))
              +1)* (select sum_nds/(sum_amount + sum_amount_unnds) from rul_invoice where invoice_id = p_invoice_id),
              -- Индексации должны выставляться в счет на дату оплаты
              p_invoice_id,
              null,
              p_storn_id,
              (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))
          from rul_transaction rt_debit
          join (select *, row_number() over (partition by transaction_id order by rul_transaction_version.month desc) as rn from rul_transaction_version
                      where date_trunc('month',month.end_date) >= rul_transaction_version.month
                      and (transaction_reversal_id = p_storn_id or p_storn_id is null)) rtv_debit
              on rtv_debit.transaction_id = rt_debit.transaction_id
              and rtv_debit.is_actual is TRUE
              and rtv_debit.rn = 1
              and rt_debit.code_ay = v_code_ay
              --and date_trunc('month',rt_debit.operation_date) = rtv_debit.month
          join rul_operation ro --Операция есть только у дебетовой проводки, так мы отсеяли все кредитовые
              on ro.operation_id = rt_debit.operation_id
          join rul_operation_template rot
              on	rot.operation_template_id = ro.operation_template_id
          where rt_debit.operation_date + interval '1 day' *
              (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))
          < month.end_date
          and rot.operation_type_id in (1,3)
          --and ro.invoice_id = p_invoice_id
          and rtv_debit.payment_percent != 100;
        END LOOP;
    ELSE
       INSERT INTO
        public.rul_penalty
      (
        start_date,
        end_date,
        amount,
        source_invoice_id,
        penalty_type_id,
        penalty_value,
        penalty_nds_value,
        invoice_id,
        transaction_transaction_id,
        transaction_reversal_id,
        penalty
      )
      select GREATEST(p_start_date,rt_debit.operation_date + interval '1 day' *
          (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))) as start_date,
          p_end_date as end_date,
          rt_debit.amount * (1 - rtv_debit.payment_percent / 100),
          ro.invoice_id,
          2, -- По закрытию периода
          rt_debit.amount * (1 - rtv_debit.payment_percent / 100) * (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)) *
          (extract (day from (p_end_date -  GREATEST(p_start_date,rt_debit.operation_date + interval '1 day' *
          (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)))))
          +1) * (select (sum_amount-sum_nds)/(sum_amount + sum_amount_unnds) from rul_invoice where invoice_id = p_invoice_id),
          rt_debit.amount * (1 - rtv_debit.payment_percent / 100) * (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)) *
          (extract (day from (p_end_date -  GREATEST(p_start_date,rt_debit.operation_date + interval '1 day' *
          (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id)))))
          +1)* (select sum_nds/(sum_amount + sum_amount_unnds) from rul_invoice where invoice_id = p_invoice_id),
          p_invoice_id,
          null,
          p_storn_id,
          (select penalty/100 from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))
      from rul_transaction rt_debit
      join (select *, row_number() over (partition by transaction_id order by rul_transaction_version.month desc) as rn from rul_transaction_version
                  where date_trunc('month',p_end_date) >= rul_transaction_version.month) rtv_debit
          on rtv_debit.transaction_id = rt_debit.transaction_id
          and rtv_debit.is_actual is TRUE
          and rtv_debit.rn = 1
          and rt_debit.code_ay = v_code_ay
          --and date_trunc('month',rt_debit.operation_date) = rtv_debit.month
      join rul_operation ro --Операция есть только у дебетовой проводки, так мы отсеяли все кредитовые
          on ro.operation_id = rt_debit.operation_id
      join rul_operation_template rot
          on	rot.operation_template_id = ro.operation_template_id
      where rt_debit.operation_date + interval '1 day' *
          (select  coalesce(pay_day_count,0) from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id))
      < p_end_date
      and rot.operation_type_id in (1,3)
      --and ro.invoice_id = p_invoice_id
      and rtv_debit.payment_percent != 100;
    END IF;
    -- Заводим клонированные пени для текущих счетов.
    -- Для этого выбираем все источники счетов, в которых поучаствовало сторнирование. И клонируем все записи с -
    -- Только для сторнирования
    IF p_storn_id IS NOT NULL
    THEN
    	INSERT INTO
          public.rul_indexing
        (
          start_date,
          end_date,
          amount,
          source_invoice_id,
          penalty_type_id,
          penalty_value,
          penalty_nds_value,
          invoice_id,
          transaction_transaction_id,
          transaction_reversal_id,
          penalty,
          is_clone
        )
        SELECT start_date,
          end_date,
          amount,
          source_invoice_id,
          penalty_type_id,
          - penalty_value,
          - penalty_nds_value,
          p_invoice_id,
          transaction_transaction_id,
          transaction_reversal_id,
          penalty,
          1::SMALLINT
        FROM rul_penalty
        WHERE 1=1
       	 	-- Выбираем все начисления, которые затронуло сторнирование
        	AND source_invoice_id IN (SELECT DISTINCT source_invoice_id FROM rul_penalty WHERE transaction_reversal_id = p_storn_id)
            AND transaction_reversal_id != p_storn_id -- Не берем текущие индексации
            ;
    END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_penalty_transaction(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint)
CREATE OR REPLACE PROCEDURE public.create_penalty_transaction(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_storn_id bigint DEFAULT NULL::bigint)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Создание новых операций по счету за расчетный месяц по пене
    INSERT INTO
      public.rul_operation
    (
      operation_template_id,
      operation_date,
      invoice_id
    )
	select ra.penalty_operation_template_id, p_end_date, p_invoice_id
    from rul_invoice ri
    join rul_agreement ra
    	on ri.agreement_id = ra.agreement_id
    join rul_penalty rp
    	on ri.invoice_id = rp.source_invoice_id
    where ra.penalty_operation_template_id is not null
    	and ri.invoice_id = p_invoice_id
        and rp.start_date >= p_start_date
        and rp.end_date <= p_end_date
        and p_storn_id is null
    group by ra.penalty_operation_template_id;
    -- Создание проводок по пене за расчетный месяц
	INSERT INTO
      public.rul_transaction
    (
      client_id,
      operation_id,
      subconto_type_id,
      content,
      amount,
      transaction_type_id,
      create_date,
      operation_date,
      transaction_template_id,
      credit_subinvoice,
      debit_subinvoice,
      code_ay,
      document,
      operation_code,
      calculated_date
    )
    select  (select supplier_client_id from rul_agreement where ra.agreement_id = agreement_id)
            , (select operation_id from rul_operation
            	where invoice_id = p_invoice_id
                AND operation_template_id = ra.penalty_operation_template_id
                AND operation_date >=  date_trunc('month',MIN(rp.start_date))
        		AND operation_date <= date_trunc('month',MIN(rp.start_date)) + interval '1 month')
            , 2
            , rtt.description
            , case when rtt.source_data_transaction_id = 1 then sum(rp.penalty_value)
                   when rtt.source_data_transaction_id = 2 then sum(rp.penalty_nds_value)
                   when rtt.source_data_transaction_id = 3 then sum(rp.penalty_value + rp.penalty_nds_value)
                end
            , 1
            , CURRENT_TIMESTAMP::timestamp(0)
            , case when p_storn_id is null then date_trunc('month',rp.start_date) + interval '1 month' - interval '1 second'
            else (select min(operation_date) from rul_transaction where correlation_transaction_id =
            		(select source_correlation_transaction_id from rul_transaction_reversal
            			where transaction_reversal_id = p_storn_id)) end
            , rtt.transaction_template_id
            , rtt.credit_subinvoice
            , rtt.debit_subinvoice
            , (select code_ay from rul_agreement where agreement_id = ra.agreement_id)
            , (select invoice_code from rul_invoice where invoice_id = p_invoice_id)
            , rot.code
            , case when p_storn_id is null then date_trunc('month',rp.start_date) + interval '1 month' - interval '1 second'
            else (select min(operation_date) from rul_transaction where correlation_transaction_id =
            		(select source_correlation_transaction_id from rul_transaction_reversal
            			where transaction_reversal_id = p_storn_id)) end
    from rul_invoice ri
    join rul_agreement ra
        on ra.agreement_id = ri.agreement_id
    join rul_operation_template rot
        on ra.penalty_operation_template_id = rot.operation_template_id
    join rul_transaction_template rtt
        on rtt.operation_template_id = rot.operation_template_id
    join rul_penalty rp
    	on rp.source_invoice_id = ri.invoice_id
        and (rp.transaction_reversal_id = p_storn_id or p_storn_id is null)
    where ri.invoice_id = p_invoice_id
    	and ((rp.start_date >= p_start_date and rp.end_date <= p_end_date) or p_storn_id is not NULL)
    group by ra.penalty_operation_template_id, rtt.transaction_template_id, ra.agreement_id, date_trunc('month',rp.start_date);
    -- Проставляем сквозной айди для транзакций.
    update rul_transaction set correlation_transaction_id = transaction_id
    where correlation_transaction_id is null
    and is_system = true;
	-- Версию погашения нужно доделать.
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_storn_pay(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.create_storn_pay(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	debit_row record;
    credit_row record;
    new_debit_percent numeric;
    credit_version_id bigint;
    debit_version_id bigint;
    storn record;
    v_code_ay varchar;
BEGIN
    -- Через счет получаем код АУ, по которому будем отсеивать проводки, которые будут гаситься
    -- Возможно нужно будет еще что-то для отсеивания
    v_code_ay := (select code_ay from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id));
	-- Цикл по всем проводкам сторнирующим
	FOR storn IN
    select rt_storn.operation_date,rt_storn.transaction_id,rtr.transaction_reversal_id,rt_storn.credit_subinvoice,rt_reversal.operation_date as reversal_date
    from rul_transaction_reversal rtr
    join rul_transaction rt_reversal
        on rtr.source_correlation_transaction_id = rt_reversal.correlation_transaction_id
    join rul_transaction rt_storn
        on rtr.storn_correlation_transaction_id = rt_storn.correlation_transaction_id
    where rt_reversal.operation_date >= p_start_date
    and rt_reversal.operation_date <= p_end_date
    and rt_reversal.code_ay = v_code_ay
    and rt_storn.code_ay = v_code_ay
    order by rt_reversal.operation_date
	LOOP
		RAISE NOTICE 'Начало сторнирвоания - %', storn;
        -- Выбираем проводки предудущие месяцу сторнирования (т.е. получаем актуальные данные по проводкам на начало месяца)
        -- Плюс выбираем проводки которые были созданы позднее месяца сторнирования
        -- Может мы должны создать версии для проводок, котрые собираемся сторнировать
        FOR debit_row IN
        /*select amount, transaction_id, payment_percent, debit, debit_subinvoice, transaction_version_id, operation_date
        from (
        select rt.amount, rt.transaction_id, rtv.payment_percent,
            rt.amount * (100 - rtv.payment_percent) / 100 as debit, rt.debit_subinvoice, rtv.transaction_version_id,rt.operation_date,
            rtv.month, row_number() over (partition by rt.transaction_id order by rtv.month desc) as rn
            from rul_transaction rt
            join rul_transaction_version rtv
                on rt.transaction_id = rtv.transaction_id
                and rtv.is_actual = true
                and date_trunc('month',storn.operation_date) > rtv.month
            LEFT JOIN rul_transaction_reversal rtr
                on rtr.source_transaction_id = rt.transaction_id
            where rt.is_system = true
            and rtv.is_actual
            and rt.debit_subinvoice = storn.credit_subinvoice
            and rtr.transaction_reversal_id is null
            and rt.operation_date < p_start_date
            order by rt.debit_subinvoice,rt.create_date, rt.transaction_id
        ) a
        where a.rn = 1
            and a.payment_percent != 100
        union all
        select rt.amount - sum(coalesce(rtr.amount,0)) over(partition by rtr.storn_transaction_id), rt.transaction_id, COALESCE(rtv.payment_percent,0),
        (rt.amount - sum(coalesce(rtr.amount,0)) over(partition by rtr.storn_transaction_id)) * (100 - COALESCE(rtv.payment_percent,0)) / 100 as debit, rt.debit_subinvoice,
        rtv.transaction_version_id,rt.operation_date
        from rul_transaction rt
        left join rul_transaction_reversal rtr
                on rtr.storn_transaction_id = rt.transaction_id
        left join rul_transaction_version rtv
                on rt.transaction_id = rtv.transaction_id
                and rtv.is_actual = true
                and date_trunc('month',rt.operation_date) = rtv.month
                and rtv.transaction_reversal_id = storn.transaction_reversal_id
        LEFT JOIN rul_transaction_reversal rtr2
            on rtr2.source_transaction_id = rt.transaction_id
        where rt.operation_date >= storn.operation_date
        and rt.is_system = true
        and rt.debit_subinvoice = storn.credit_subinvoice
        and rt.operation_date < p_start_date
        and rtr2.transaction_reversal_id is null
        order by operation_date, transaction_id*/
        WITH first_part AS (
                -- Проводки старше месяца сторнирования
                SELECT
                    rt.amount,
                    rt.transaction_id,
                    rtv.payment_percent,
                    rt.amount * (100 - rtv.payment_percent) / 100 AS debit,
                    rt.debit_subinvoice,
                    rtv.transaction_version_id,
                    rt.operation_date,
                    rt.correlation_transaction_id,
                    ROW_NUMBER() OVER (PARTITION BY rt.transaction_id ORDER BY rtv.month DESC,rtv.transaction_version_id) AS rn
                FROM rul_transaction rt
                JOIN rul_transaction_version rtv
                    ON rt.transaction_id = rtv.transaction_id
                   AND rtv.is_actual = true
                   AND date_trunc('month', storn.operation_date) > rtv.month
                   AND rt.code_ay = v_code_ay
                LEFT JOIN rul_transaction_reversal rtr
                    ON rtr.source_correlation_transaction_id = rt.correlation_transaction_id
                WHERE
                    rt.is_debit = 1
                    AND rt.debit_subinvoice = storn.credit_subinvoice
                    AND rtr.transaction_reversal_id IS NULL
                    AND rt.operation_date < p_start_date
            ),
            filtered_first AS (
                -- Оставляем только самую "свежую" версию
                SELECT *
                FROM first_part
                WHERE rn = 1
                  AND payment_percent != 100
                  -- Убираемые сторнирующие проводки
                  AND NOT EXISTS (
                      SELECT 1
                      FROM rul_transaction_reversal rtr2
                      WHERE rtr2.storn_correlation_transaction_id = first_part.correlation_transaction_id
                  )
            ),
            second_part AS (
                -- Выбираем проводки, которые созданы в месяце сторнирования и позже
                SELECT
                    rt.amount - SUM(COALESCE(rtr.amount, 0)) OVER (PARTITION BY rtr.storn_correlation_transaction_id) AS amount,
                    rt.transaction_id,
                    COALESCE(rtv.payment_percent, 0) AS payment_percent,
                    (rt.amount - SUM(COALESCE(rtr.amount, 0)) OVER (PARTITION BY rtr.storn_correlation_transaction_id))
                        * (100 - COALESCE(rtv.payment_percent, 0)) / 100 AS debit,
                    rt.debit_subinvoice,
                    rtv.transaction_version_id,
                    rt.operation_date
                FROM rul_transaction rt
                -- Мы должны соединить только те проводки сторнированные, которые были в этом месяце и раньше
                -- И группируем, чтобы учесть все сторнирования
                LEFT JOIN (select sum(rtr.amount) as amount,rtr.storn_correlation_transaction_id from rul_transaction_reversal rtr join rul_transaction rt
                           on rtr.source_correlation_transaction_id = rt.correlation_transaction_id
                           and rt.operation_date <= storn.reversal_date
                           group by rtr.storn_correlation_transaction_id
                           ) rtr
                    ON rtr.storn_correlation_transaction_id = rt.correlation_transaction_id
                    AND rt.code_ay = v_code_ay
                LEFT JOIN rul_transaction_version rtv
                    ON rt.transaction_id = rtv.transaction_id
                   AND rtv.is_actual = true
                   AND DATE_TRUNC('month', rt.operation_date) = rtv.month
                   AND rtv.transaction_reversal_id = storn.transaction_reversal_id
                LEFT JOIN rul_transaction_reversal rtr2
                    ON rtr2.source_correlation_transaction_id = rt.correlation_transaction_id
                WHERE
                    rt.operation_date >= storn.operation_date
                    AND rt.is_debit = 1
                    AND rt.debit_subinvoice = storn.credit_subinvoice
                    AND rtr2.transaction_reversal_id IS NULL
                    AND rt.operation_date < p_start_date
                    AND rtr.storn_correlation_transaction_id IS NOT NULL
            )
            SELECT
                amount,
                transaction_id,
                payment_percent,
                debit,
                debit_subinvoice,
                transaction_version_id,
                operation_date
            FROM filtered_first
            UNION ALL
            SELECT
                amount,
                transaction_id,
                payment_percent,
                debit,
                debit_subinvoice,
                transaction_version_id,
                operation_date
            FROM second_part
            WHERE payment_percent != 100
            ORDER BY debit_subinvoice, operation_date, transaction_id
        LOOP
            new_debit_percent := debit_row.payment_percent;
            raise notice '%: - Debit %', new_debit_percent,debit_row;
            FOR credit_row IN
            /*select amount, transaction_id, payment_percent, credit, credit_subinvoice, transaction_version_id, operation_date
            from (
            select rt.amount, rt.transaction_id, rtv.payment_percent,
                rt.amount * (100 - rtv.payment_percent) / 100 as credit, rt.credit_subinvoice, rtv.transaction_version_id,rt.operation_date,
                rtv.month, row_number() over (partition by rt.transaction_id order by rtv.month desc) as rn
                from rul_transaction rt
                join rul_transaction_version rtv
                    on rt.transaction_id = rtv.transaction_id
                    and rtv.is_actual = true
                    and date_trunc('month',storn.operation_date) >= rtv.month
                LEFT JOIN rul_transaction_reversal rtr
                    on rtr.source_transaction_id = rt.transaction_id
                where rt.is_system = false
                and rtv.is_actual
                and rt.credit_subinvoice = storn.credit_subinvoice
                and rtr.transaction_reversal_id is null
                and rt.operation_date < p_start_date
                --and rt.operation_date >  date_trunc('month','2025-08-24 00:00:00+03'::timestamp without time zone)
                order by rt.credit_subinvoice,rt.create_date, rt.transaction_id
            ) a
            where a.rn = 1
                and a.payment_percent != 100
            union all
            select rt.amount - sum(coalesce(rtr.amount,0)) over(partition by rtr.storn_transaction_id), rt.transaction_id, COALESCE(rtv.payment_percent,0),
            (rt.amount - sum(coalesce(rtr.amount,0)) over(partition by rtr.storn_transaction_id)) * (100 - COALESCE(rtv.payment_percent,0)) / 100 as debit, rt.credit_subinvoice,
            rtv.transaction_version_id,rt.operation_date
            from rul_transaction rt
            left join rul_transaction_reversal rtr
                    on rtr.storn_transaction_id = rt.transaction_id
            left join rul_transaction_version rtv
                    on rt.transaction_id = rtv.transaction_id
                    and rtv.is_actual = true
                    and date_trunc('month',rt.operation_date) = rtv.month
                    and rtv.transaction_reversal_id = storn.transaction_reversal_id
            LEFT JOIN rul_transaction_reversal rtr2
                on rtr2.source_transaction_id = rt.transaction_id
            where rt.operation_date >= storn.operation_date
            and rt.is_system = false
            and rt.credit_subinvoice = storn.credit_subinvoice
            and rtr2.transaction_reversal_id is null
            and rt.operation_date < p_start_date
            order by operation_date, transaction_id*/
            WITH first_part AS (
                -- Проводки старше месяца сторнирования
                SELECT
                    rt.amount,
                    rt.transaction_id,
                    rtv.payment_percent,
                    rt.amount * (100 - rtv.payment_percent) / 100 AS credit,
                    rt.credit_subinvoice,
                    rtv.transaction_version_id,
                    rt.operation_date,
                    rt.correlation_transaction_id,
                    ROW_NUMBER() OVER (PARTITION BY rt.transaction_id ORDER BY rtv.month DESC,rtv.transaction_version_id) AS rn
                FROM rul_transaction rt
                JOIN rul_transaction_version rtv
                    ON rt.transaction_id = rtv.transaction_id
                   AND rtv.is_actual = true
                   AND date_trunc('month', storn.operation_date) > rtv.month
                   AND rt.code_ay = v_code_ay
                LEFT JOIN rul_transaction_reversal rtr
                    ON rtr.source_correlation_transaction_id = rt.correlation_transaction_id
                WHERE
                    rt.is_debit = 0
                    AND rt.credit_subinvoice = storn.credit_subinvoice
                    AND rtr.transaction_reversal_id IS NULL
                    AND rt.operation_date < p_start_date
            ),
            filtered_first AS (
                -- Оставляем только самую "свежую" версию
                SELECT *
                FROM first_part
                WHERE rn = 1
                  AND payment_percent != 100
                  -- Убираемые сторнирующие проводки
                  AND NOT EXISTS (
                      SELECT 1
                      FROM rul_transaction_reversal rtr2
                      WHERE rtr2.storn_correlation_transaction_id = first_part.correlation_transaction_id
                  )
            ),
            second_part AS (
                -- Выбираем проводки, которые созданы в месяце сторнирования и старше
                SELECT
                    rt.amount - SUM(COALESCE(rtr.amount, 0)) OVER (PARTITION BY rtr.storn_correlation_transaction_id) AS amount,
                    rt.transaction_id,
                    COALESCE(rtv.payment_percent, 0) AS payment_percent,
                    (rt.amount - SUM(COALESCE(rtr.amount, 0)) OVER (PARTITION BY rtr.storn_correlation_transaction_id))
                        * (100 - COALESCE(rtv.payment_percent, 0)) / 100 AS credit,
                    rt.credit_subinvoice,
                    rtv.transaction_version_id,
                    rt.operation_date
                FROM rul_transaction rt
                -- Мы должны соединить только те проводки сторнированные, которые были в этом месяце и раньше
                -- И группируем, чтобы учесть все сторнирования
                LEFT JOIN (select sum(rtr.amount) as amount,rtr.storn_correlation_transaction_id from rul_transaction_reversal rtr join rul_transaction rt
                           on rtr.source_correlation_transaction_id = rt.correlation_transaction_id
                           and rt.operation_date <= storn.reversal_date
                           and rt.is_debit = 0
                           group by rtr.storn_correlation_transaction_id
                           ) rtr
                    ON rtr.storn_correlation_transaction_id = rt.correlation_transaction_id
                    AND rt.code_ay = v_code_ay
                LEFT JOIN rul_transaction_version rtv
                    ON rt.transaction_id = rtv.transaction_id
                   AND rtv.is_actual = true
                   AND DATE_TRUNC('month', rt.operation_date) = rtv.month
                   AND rtv.transaction_reversal_id = storn.transaction_reversal_id
                LEFT JOIN rul_transaction_reversal rtr2
                    ON rtr2.source_correlation_transaction_id = rt.correlation_transaction_id
                WHERE
                    rt.operation_date >= storn.operation_date
                    AND rt.is_debit = 0
                    AND rt.credit_subinvoice = storn.credit_subinvoice
                    AND rtr2.transaction_reversal_id IS NULL
                    AND rt.operation_date < p_start_date
                    AND rtr.storn_correlation_transaction_id IS NOT NULL
            )
            SELECT
                amount,
                transaction_id,
                payment_percent,
                credit,
                credit_subinvoice,
                transaction_version_id,
                operation_date
            FROM filtered_first
            UNION ALL
            SELECT
                amount,
                transaction_id,
                payment_percent,
                credit,
                credit_subinvoice,
                transaction_version_id,
                operation_date
            FROM second_part
            WHERE payment_percent != 100
            ORDER BY credit_subinvoice, operation_date, transaction_id
            LOOP
            raise notice '%: - Credit %', new_debit_percent,credit_row;
                IF (100 - new_debit_percent) * debit_row.amount / 100 > credit_row.credit
                  THEN
                      --UPDATE rul_transaction SET payment_percent = 100 WHERE transaction_id = credit_row.transaction_id;
                      --new_debit_percent := new_debit_percent + (credit_row.credit / ((100 - new_debit_percent) * debit_row.amount / 100)) * 100;
                      INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual, transaction_reversal_id)
                      VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),credit_row.transaction_id,true,storn.transaction_reversal_id)
                      ON CONFLICT (month,is_actual,transaction_id, transaction_reversal_id)
                      DO UPDATE SET payment_percent = 100
                      WHERE rul_transaction_version.transaction_version_id = credit_row.transaction_version_id
                      RETURNING rul_transaction_version.transaction_version_id INTO credit_version_id;
                      UPDATE rul_transaction_version SET is_actual = false
                      WHERE
                        month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                        and transaction_id = credit_row.transaction_id
                        and transaction_version_id != credit_version_id;
                      new_debit_percent := new_debit_percent + (credit_row.credit / debit_row.amount) * 100;
                      raise notice 'Percent1 - %', new_debit_percent;
                      --
                      --UPDATE rul_transaction SET payment_percent = new_debit_percent WHERE transaction_id = debit_row.transaction_id;
                      raise notice 'Insert 1 - %,%,%,%', GREATEST(credit_row.operation_date,debit_row.operation_date),debit_row.transaction_id,new_debit_percent,debit_version_id;
                      INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual, transaction_reversal_id)
                      VALUES (new_debit_percent, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),debit_row.transaction_id,true,storn.transaction_reversal_id)
                      ON CONFLICT (month,is_actual,transaction_id, transaction_reversal_id)
                      DO UPDATE SET payment_percent = new_debit_percent
                      WHERE rul_transaction_version.transaction_version_id = debit_version_id
                      RETURNING rul_transaction_version.transaction_version_id INTO debit_version_id;
                      INSERT INTO public.rul_transaction_transaction (credit_transaction_id,debit_transaction_id,amount,operation_date)
                      VALUES (credit_version_id,debit_version_id,credit_row.credit,GREATEST(credit_row.operation_date,debit_row.operation_date));
                      UPDATE rul_transaction_version SET is_actual = false
                      WHERE
                        month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                        and transaction_id = debit_row.transaction_id
                        and transaction_version_id != debit_version_id;
                ELSEIF (100 - new_debit_percent) * debit_row.amount / 100 < credit_row.credit
                  THEN
                      --UPDATE rul_transaction SET payment_percent = 100 WHERE transaction_id = debit_row.transaction_id;
                      INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual, transaction_reversal_id)
                      VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),debit_row.transaction_id,true,storn.transaction_reversal_id)
                      ON CONFLICT (month,is_actual,transaction_id,transaction_reversal_id)
                      DO UPDATE SET payment_percent = 100
                      WHERE rul_transaction_version.transaction_version_id = debit_row.transaction_version_id
                      RETURNING rul_transaction_version.transaction_version_id INTO debit_version_id;
                      UPDATE rul_transaction_version SET is_actual = false
                      WHERE
                        month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                        and transaction_id = debit_row.transaction_id
                        and transaction_version_id != debit_version_id;
                      raise notice 'id debit - % - %', debit_row.transaction_id,debit_row.transaction_version_id;
                      --
                      --UPDATE rul_transaction SET payment_percent = payment_percent + (((100 - new_debit_percent) * debit_row.amount / 100) / credit_row.amount) * 100
                      --WHERE transaction_id = credit_row.transaction_id;
                      INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual, transaction_reversal_id)
                      VALUES (credit_row.payment_percent + (((100 - new_debit_percent) * debit_row.amount / 100) / credit_row.amount) * 100
                      , date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),credit_row.transaction_id,true,storn.transaction_reversal_id)
                      ON CONFLICT (month,is_actual,transaction_id, transaction_reversal_id)
                      DO UPDATE SET payment_percent = credit_row.payment_percent + (((100 - new_debit_percent) * debit_row.amount / 100) / credit_row.amount) * 100
                      WHERE rul_transaction_version.transaction_version_id = credit_row.transaction_version_id
                      RETURNING rul_transaction_version.transaction_version_id INTO credit_version_id;
                      UPDATE rul_transaction_version SET is_actual = false
                      WHERE
                        month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                        and transaction_id = credit_row.transaction_id
                        and transaction_version_id != credit_version_id;
                      --
                      INSERT INTO public.rul_transaction_transaction (credit_transaction_id,debit_transaction_id,amount,operation_date)
                      VALUES (credit_version_id,debit_version_id,((100 - new_debit_percent) * debit_row.amount / 100),GREATEST(credit_row.operation_date,debit_row.operation_date));
                      raise notice 'Percent2 - %', credit_row.payment_percent + (((100 - new_debit_percent) * debit_row.amount / 100) / credit_row.amount) * 100;
                      EXIT;
                ELSEIF (100 - new_debit_percent) * debit_row.amount / 100 = credit_row.credit
                  THEN
                      --UPDATE rul_transaction SET payment_percent = 100 WHERE transaction_id = debit_row.transaction_id;
                      --UPDATE rul_transaction SET payment_percent = 100 WHERE transaction_id = credit_row.transaction_id;
                      --
                      INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual, transaction_reversal_id)
                      VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),debit_row.transaction_id,true,storn.transaction_reversal_id)
                      ON CONFLICT (month,is_actual,transaction_id, transaction_reversal_id)
                      DO UPDATE SET payment_percent = 100
                      WHERE rul_transaction_version.transaction_version_id = debit_row.transaction_version_id
                      RETURNING rul_transaction_version.transaction_version_id INTO debit_version_id;
                      UPDATE rul_transaction_version SET is_actual = false
                      WHERE
                        month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                        and transaction_id = debit_row.transaction_id
                        and transaction_version_id != debit_version_id;
                      INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual, transaction_reversal_id)
                      VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),credit_row.transaction_id,true,storn.transaction_reversal_id)
                      ON CONFLICT (month,is_actual,transaction_id, transaction_reversal_id)
                      DO UPDATE SET payment_percent = 100
                      WHERE rul_transaction_version.transaction_version_id = credit_row.transaction_version_id
                      RETURNING rul_transaction_version.transaction_version_id INTO credit_version_id;
                      UPDATE rul_transaction_version SET is_actual = false
                      WHERE
                        month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                        and transaction_id = credit_row.transaction_id
                        and transaction_version_id != credit_version_id;
                      INSERT INTO public.rul_transaction_transaction (credit_transaction_id,debit_transaction_id,amount,operation_date)
                      VALUES (credit_version_id,debit_version_id,credit_row.credit,GREATEST(credit_row.operation_date,debit_row.operation_date));
                      EXIT;
                END IF;
            END LOOP;
        END LOOP;
        -- Когда прошло сторнирование одной проводки, можно посчитать пеню и индексацию
        -- Т.к. индексация считается по погашения, то в метод обычной идексации надо передать все погашения, которые относятся к сторнированию
        -- Можно дописать в метод формирования пени параметр сторнирования
        call public.create_indexing(p_invoice_id,p_start_date,p_end_date,storn.transaction_reversal_id);
        call public.create_indexing_transaction(p_invoice_id,p_start_date,p_end_date,storn.transaction_reversal_id);
        -- Для пени же есть два расчета, по оплате, где такой же метод как индексация
        -- И второй, в котором нужно создать пеню на неоплаченные остатки.
        -- Чтобы это сделать нужно будет взять все актуальные в каждом месяце неоплаченные остатки и рассчитать для них пеню
        -- Похоже пеню придется запускать на каждый месяц для правильного расчета
        call public.create_penalty(p_invoice_id,p_start_date,p_end_date,storn.transaction_reversal_id,storn.operation_date);
      	call public.create_penalty_transaction(p_invoice_id,p_start_date,p_end_date,storn.transaction_reversal_id);
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_transaction(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.create_transaction(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Создание новых операций по счету за расчетный месяц по операциям начислений
    INSERT INTO
      public.rul_operation
    (
      operation_template_id,
      operation_date,
      invoice_id
    )
	select rcc.service_operation_template_id, p_end_date, p_invoice_id
	from rul_charge rc
    join rul_connection rcc
    	on rcc.connection_id = rc.connection_id
    where rcc.service_operation_template_id is not null
    	and rc.invoice_id = p_invoice_id
        and rc.billing_start_date >= p_start_date
        and rc.billing_end_date <= p_end_date
    group by rcc.service_operation_template_id;
    -- Создание проводок по начислениям за расчетный месяц
	INSERT INTO
      public.rul_transaction
    (
      client_id,
      operation_id,
      subconto_type_id,
      content,
      amount,
      transaction_type_id,
      create_date,
      operation_date,
      transaction_template_id,
      debit_subinvoice,
      credit_subinvoice,
      code_ay,
      document,
      operation_code,
      calculated_date
    )
    SELECT (select supplier_client_id from rul_agreement where rcc.agreement_id = agreement_id)
            , (select operation_id from rul_operation
            	where invoice_id = p_invoice_id
                AND operation_template_id = rcc.service_operation_template_id
                AND operation_date >= p_start_date
        		AND operation_date <= p_end_date)
            , 2
            , rtt.description
            , case when rtt.source_data_transaction_id = 1 then sum(rc.amount)
                   when rtt.source_data_transaction_id = 2 then sum(rc.nds_rub)
                   when rtt.source_data_transaction_id = 3 then sum(rc.amount_nds)
                end
            , 1
            , CURRENT_TIMESTAMP::timestamp(0)
            , date_trunc('month',rc.start_date) + interval '1 month' - interval '1 second'
            , rtt.transaction_template_id
            , rtt.debit_subinvoice
            , rtt.credit_subinvoice
            , (select code_ay from rul_agreement where agreement_id = rcc.agreement_id)
            , (select invoice_code from rul_invoice where invoice_id = p_invoice_id)
            , rot.code
            , date_trunc('month',rc.start_date) + interval '1 month' - interval '1 second'
    from rul_charge rc
    join rul_connection rcc
        on rcc.connection_id = rc.connection_id
    join rul_operation_template rot
        on rcc.service_operation_template_id = rot.operation_template_id
    join rul_transaction_template rtt
        on rtt.operation_template_id = rot.operation_template_id
    where invoice_id = p_invoice_id
    	and rtt.source_data_transaction_id is not null
    	and rc.billing_start_date >= p_start_date
        and rc.billing_end_date <= p_end_date
    group by rcc.service_operation_template_id, rtt.transaction_template_id, rcc.agreement_id, rtt.debit_subinvoice, rtt.credit_subinvoice, date_trunc('month',rc.start_date),rot.code;
    -- Проставляем сквозной айди для транзакций.
    update rul_transaction set correlation_transaction_id = transaction_id
    where correlation_transaction_id is null
    and is_system = true;
    -- Создание версий процента погашения проводок
    INSERT INTO
      public.rul_transaction_version
    (
      payment_percent,
      transaction_reversal_id,
      create_date,
      month,
      transaction_id
    )
    SELECT
    	0
    	, NULL
        , CURRENT_TIMESTAMP::timestamp(0)
        , date_trunc('month',operation_date)
        , transaction_id
    FROM rul_transaction
    WHERE is_system IS TRUE
    	AND operation_date >= p_start_date
    	AND operation_date <= p_end_date;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.create_transaction_pay(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.create_transaction_pay(IN p_invoice_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	debit_row record;
    credit_row record;
    new_debit_percent numeric;
    credit_version_id bigint;
    debit_version_id bigint;
    v_code_ay varchar;
BEGIN
	-- Через счет получаем код АУ, по которому будем отсеивать проводки, которые будут гаситься
    -- Возможно нужно будет еще что-то для отсеивания
    v_code_ay := (select code_ay from rul_agreement where agreement_id = (select agreement_id from rul_invoice where invoice_id = p_invoice_id));
    -- Выбираем все версии проводок, которые старше нашей даты операции, и отсекаем только нужные
    -- Т.е. самые близкие по дате актуальные версии. Это нужно из-за того, что в каждом месяце своя актуальная версия.
    -- Возможно стоит добавить еще одну колонку актуальности, которая будет показывать актуальность общую, независимо от месяца.
    -- Чтобы по этому полю было понятно, что выбрать для расчета текущего месяца
    -- Либо же как вариант создавать версии всех проводок каждый месяц, тогда будет больше данных, но исчезнет нужда в поиске всех месяцев, можно будет брать предыдущий.
    FOR debit_row IN
    SELECT
    	amount, transaction_id, payment_percent, debit, debit_subinvoice, transaction_version_id, operation_date
    FROM
    (
      SELECT rt.amount, rt.transaction_id, rtv.payment_percent, rt.amount * (100 - rtv.payment_percent) / 100 as debit,
          rt.debit_subinvoice, rtv.transaction_version_id, rt.operation_date, row_number() over (partition by rt.transaction_id order by rtv.month desc,rtv.transaction_version_id) as rn
      FROM rul_transaction rt
      JOIN rul_transaction_version rtv
          ON rt.transaction_id = rtv.transaction_id
          AND rtv.is_actual = TRUE
          --AND date_trunc('month',rt.operation_date) <= rtv.month
      LEFT JOIN rul_transaction_reversal rtr
          ON rtr.source_correlation_transaction_id = rt.correlation_transaction_id
      WHERE rt.is_debit = 1
        AND rtr.transaction_reversal_id IS NULL
        AND rt.code_ay = v_code_ay
        --AND rt.operation_date >= p_start_date
        --AND rt.operation_date <= p_end_date
    ) all_actual
    WHERE all_actual.rn = 1
    	AND all_actual.payment_percent != 100
    ORDER BY debit_subinvoice,operation_date,transaction_id
    LOOP
        new_debit_percent := debit_row.payment_percent;
        raise notice '%: - Debit %', new_debit_percent,debit_row;
        -- Тоже самое что по дебетовым(вверху описание), только теперь по кредитным проводкам
        FOR credit_row IN
        SELECT
            amount, transaction_id, payment_percent, credit, credit_subinvoice, transaction_version_id, operation_date
        FROM
        (
            SELECT rt.amount, rt.transaction_id, rtv.payment_percent, rt.amount * (100 - rtv.payment_percent) / 100 as credit, rt.credit_subinvoice,
             rtv.transaction_version_id, rt.operation_date, row_number() over (partition by rt.transaction_id order by rtv.month desc,rtv.transaction_version_id) as rn
            FROM rul_transaction rt
            JOIN rul_transaction_version rtv
                ON rt.transaction_id = rtv.transaction_id
                AND rtv.is_actual = true
                --AND date_trunc('month',rt.operation_date) >= rtv.month
            LEFT JOIN rul_transaction_reversal rtr
                ON rtr.source_correlation_transaction_id = rt.correlation_transaction_id
            WHERE rt.is_debit = 0
            	AND rt.credit_subinvoice = debit_row.debit_subinvoice
            	AND rt.operation_id is null -- Выбираем импортированнные проводки, т.к. у них нету операции. Возможно это не то, что нужно.
            	AND rtr.transaction_reversal_id is null
                AND rt.code_ay = v_code_ay
            	--AND rt.operation_date >= p_start_date
        		--AND rt.operation_date <= p_end_date
        ) all_actual
        WHERE all_actual.rn = 1
        AND all_actual.payment_percent != 100
        ORDER BY operation_date, transaction_id
        LOOP
        raise notice '%: - Credit %', new_debit_percent,credit_row;
            -- При кредитовой проводке меньшей чем дебетовой, кредитовая гасится полность, а дебетовая получает новый процент погашения
            IF (100 - new_debit_percent) * debit_row.amount / 100 > credit_row.credit
              THEN
                  -- Либо добавляем версию погашения проводки, либо обновляем.
                  INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual)
                  VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),credit_row.transaction_id,true)
                  ON CONFLICT (month,is_actual,transaction_id,transaction_reversal_id)
                  DO UPDATE SET payment_percent = 100
                  WHERE rul_transaction_version.transaction_version_id = credit_row.transaction_version_id
                  RETURNING rul_transaction_version.transaction_version_id INTO credit_version_id;
                  -- Обновляем актуальность версии проводки
                  UPDATE rul_transaction_version SET is_actual = false
                  WHERE
                  	month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                    and transaction_id = credit_row.transaction_id
                    and transaction_version_id != credit_version_id;
                  -- Высчитываем новый процент погашения дебетовой проводки, чтобы знать для следующего шага цикла
                  new_debit_percent := new_debit_percent + (credit_row.credit / debit_row.amount) * 100;
                  raise notice 'Percent1 - %', new_debit_percent;
                  INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual)
                  VALUES (new_debit_percent, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),debit_row.transaction_id,true)
                  ON CONFLICT (month,is_actual,transaction_id,transaction_reversal_id)
                  DO UPDATE SET payment_percent = new_debit_percent
                  WHERE rul_transaction_version.transaction_version_id = debit_row.transaction_version_id
                  RETURNING rul_transaction_version.transaction_version_id INTO debit_version_id;
                  UPDATE rul_transaction_version SET is_actual = false
                  WHERE
                  	month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                    and transaction_id = debit_row.transaction_id
                    and transaction_version_id != debit_version_id;
                  -- Записываем погашение проводок в таблицу погашения.
                  INSERT INTO public.rul_transaction_transaction (credit_transaction_id,debit_transaction_id,amount,operation_date)
                  VALUES (credit_version_id,debit_version_id,credit_row.credit,GREATEST(credit_row.operation_date,debit_row.operation_date));
            -- При дебетовой проводке меньшей чем кредитовая, дебетовая гасится полность, а кредитовая получает новый процент погашения
            -- Процесс такой же как и в предыдущем IF
            ELSEIF (100 - new_debit_percent) * debit_row.amount / 100 < credit_row.credit
              THEN
                  INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual)
                  VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),debit_row.transaction_id,true)
                  ON CONFLICT (month,is_actual,transaction_id,transaction_reversal_id)
                  DO UPDATE SET payment_percent = 100
                  WHERE rul_transaction_version.transaction_version_id = debit_row.transaction_version_id
                  RETURNING rul_transaction_version.transaction_version_id INTO debit_version_id;
                  UPDATE rul_transaction_version SET is_actual = false
                  WHERE
                  	month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                    and transaction_id = debit_row.transaction_id
                    and transaction_version_id != debit_version_id;
                  raise notice 'id debit - % - %', debit_row.transaction_id,debit_row.transaction_version_id;
                  --
                  INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual)
                  VALUES (credit_row.payment_percent + (((100 - new_debit_percent) * debit_row.amount / 100) / credit_row.amount) * 100 , date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),credit_row.transaction_id,true)
                  ON CONFLICT (month,is_actual,transaction_id,transaction_reversal_id)
                  DO UPDATE SET payment_percent = rul_transaction_version.payment_percent + (((100 - new_debit_percent) * debit_row.amount / 100) / credit_row.amount) * 100
                  WHERE rul_transaction_version.transaction_version_id = credit_row.transaction_version_id
                  RETURNING rul_transaction_version.transaction_version_id INTO credit_version_id;
                  UPDATE rul_transaction_version SET is_actual = false
                  WHERE
                  	month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                    and transaction_id = credit_row.transaction_id
                    and transaction_version_id != credit_version_id;
                  --
                  INSERT INTO public.rul_transaction_transaction (credit_transaction_id,debit_transaction_id,amount,operation_date)
                  VALUES (credit_version_id,debit_version_id,((100 - new_debit_percent) * debit_row.amount / 100),GREATEST(credit_row.operation_date,debit_row.operation_date));
                  raise notice 'Percent2 - %', credit_row.payment_percent + (((100 - new_debit_percent) * debit_row.amount / 100) / credit_row.amount) * 100;
                  EXIT;
            -- Если кредитовая и дебетовая проводки равны, то гасим обе полностью
            ELSEIF (100 - new_debit_percent) * debit_row.amount / 100 = credit_row.credit
              THEN
                  INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual)
                  VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),debit_row.transaction_id,true)
                  ON CONFLICT (month,is_actual,transaction_id,transaction_reversal_id)
                  DO UPDATE SET payment_percent = 100
                  WHERE rul_transaction_version.transaction_version_id = debit_row.transaction_version_id
                  RETURNING rul_transaction_version.transaction_version_id INTO debit_version_id;
                  UPDATE rul_transaction_version SET is_actual = false
                  WHERE
                  	month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                    and transaction_id = debit_row.transaction_id
                    and transaction_version_id != debit_version_id;
                  INSERT INTO rul_transaction_version (payment_percent, month, transaction_id, is_actual)
                  VALUES (100, date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date)),credit_row.transaction_id,true)
                  ON CONFLICT (month,is_actual,transaction_id,transaction_reversal_id)
                  DO UPDATE SET payment_percent = 100
                  WHERE rul_transaction_version.transaction_version_id = credit_row.transaction_version_id
                  RETURNING rul_transaction_version.transaction_version_id INTO credit_version_id;
                  UPDATE rul_transaction_version SET is_actual = false
                  WHERE
                  	month = date_trunc('month',GREATEST(credit_row.operation_date,debit_row.operation_date))
                    and transaction_id = credit_row.transaction_id
                    and transaction_version_id != credit_version_id;
                  INSERT INTO public.rul_transaction_transaction (credit_transaction_id,debit_transaction_id,amount,operation_date)
                  VALUES (credit_version_id,debit_version_id,credit_row.credit,GREATEST(credit_row.operation_date,debit_row.operation_date));
                  EXIT;
            END IF;
        END LOOP;
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.define_balancing_node(IN p_node_calculate_parameter_id bigint[])
CREATE OR REPLACE PROCEDURE public.define_balancing_node(IN p_node_calculate_parameter_id bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_node_calculate_parameter_id BIGINT;
    v_balancing_node_calculate_parameter_id BIGINT;
    v_commercial_node_calculate_parameter_id BIGINT;
    v_cnt BIGINT;
BEGIN
	-- На вход приходят РП нижележащих узлов
    FOREACH v_node_calculate_parameter_id IN ARRAY p_node_calculate_parameter_id
	LOOP
    -- Получаем РП балансного узла через верхний параметр, если нижележащий РП не связан ни с каким параметром,
    -- то мы проставляем null. Если же РП лежит в балансном узле, то мы должны взять его РП.
    -- В другом случае берем сохранненный РП балансного узла.
    SELECT COUNT(*)
    INTO v_cnt
    FROM rul_line_parameter_child
    WHERE node_calculate_parameter_id = v_node_calculate_parameter_id;
    IF v_cnt > 1 THEN
        RAISE EXCEPTION '%', get_message('ERR_CREATE_NEW_LINE');
    END IF;
    SELECT CASE WHEN rn.node_type_id = 3 THEN node_calculate_parameter_id ELSE balancing_node_calculate_parameter_id END
    INTO v_balancing_node_calculate_parameter_id
    FROM rul_node_calculate_parameter rncp
    JOIN rul_node rn
    	ON rn.node_id = rncp.node_id
    WHERE node_calculate_parameter_id = (SELECT node_calculate_parameter_id FROM rul_line_parameter
    									 WHERE line_parameter_id = (SELECT line_parameter_id FROM rul_line_parameter_child
                                         							WHERE node_calculate_parameter_id = v_node_calculate_parameter_id));
    SELECT CASE WHEN rn.node_type_id in (1,3) THEN v_node_calculate_parameter_id ELSE commercial_node_calculate_parameter_id END
    INTO v_commercial_node_calculate_parameter_id
    FROM rul_node_calculate_parameter rncp
    JOIN rul_node rn
    	ON rn.node_id = rncp.node_id
    WHERE node_calculate_parameter_id = (SELECT node_calculate_parameter_id FROM rul_line_parameter
    									 WHERE line_parameter_id = (SELECT line_parameter_id FROM rul_line_parameter_child
                                         							WHERE node_calculate_parameter_id = v_node_calculate_parameter_id));
    IF v_commercial_node_calculate_parameter_id IS NULL
    THEN
    	v_commercial_node_calculate_parameter_id = v_node_calculate_parameter_id;
    END IF;
    -- Теперь, зная что нужно проставлять узлам ниже по сети, можно построить дерево.
	CREATE TEMP TABLE temp_result AS
        WITH RECURSIVE tree_cte AS (
            -- Базовый случай: выбираем корневые элементы
            SELECT
                zero_level.node_id AS node_id,
                zero_level.node_id AS child_id,
                0 AS level,
                ARRAY[zero_level.node_id] AS path,
                zero_level.node_id::TEXT AS path_str,
                (SELECT node_type_id FROM rul_node WHERE node_id =
      			(SELECT node_id FROM rul_node_calculate_parameter WHERE node_calculate_parameter_id = zero_level.node_id)) AS node_type_id,
                0 as cycle_detected,
                1 as need_update_balancing_node,
                1 as need_update_commercial_node
    		FROM (select 'zero_level' as name, v_node_calculate_parameter_id::bigint as node_id) zero_level
            UNION ALL
            -- Рекурсивный случай: присоединяем детей с учетом дат родителя
            SELECT
                rlp.node_calculate_parameter_id AS node_id,
                rlpc.node_calculate_parameter_id AS child_id,
                t.level + 1,
                t.path || rlpc.node_calculate_parameter_id,
                t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                rn.node_type_id,
                CASE WHEN rlpc.node_calculate_parameter_id = ANY(t.path) THEN t.cycle_detected + 1 ELSE t.cycle_detected END,
                CASE WHEN t.need_update_balancing_node = 0
                		THEN 0
                	 WHEN t.need_update_balancing_node = 1 AND t.node_type_id = 3
                     	THEN 0
                     ELSE 1
                END,
                CASE WHEN t.need_update_commercial_node = 0
                		THEN 0
                	 WHEN t.need_update_commercial_node = 1 AND t.node_type_id IN (1,3)
                     	THEN 0
                     ELSE 1
                END
                     FROM tree_cte t
            JOIN public.rul_line_parameter rlp
                ON t.child_id = rlp.node_calculate_parameter_id
            JOIN public.rul_line_parameter_child rlpc
                ON rlpc.line_parameter_id = rlp.line_parameter_id
            JOIN public.rul_node_calculate_parameter rncp
            	ON rlpc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
            JOIN public.rul_node rn
            	ON rn.node_id = rncp.node_id
        	-- Если встречается балансный узел, то дальше дерево не строится
            WHERE 1=1--t.node_type_id != 3
            	AND t.cycle_detected + 1 < 2
        )
        SELECT
            tree_cte.node_id,
            tree_cte.child_id,
            tree_cte.cycle_detected,
            tree_cte.need_update_balancing_node,
            tree_cte.need_update_commercial_node
        FROM tree_cte
        ORDER BY path, node_id, child_id;
        SELECT COUNT(*)
        INTO v_cnt
        FROM temp_result
        WHERE cycle_detected > 0;
        IF v_cnt != 0 THEN
            RAISE EXCEPTION '%', get_message('ERR_CYCLE_DETECTED');
        END IF;
        UPDATE rul_node_calculate_parameter SET balancing_node_calculate_parameter_id = v_balancing_node_calculate_parameter_id
        WHERE node_calculate_parameter_id IN (SELECT child_id FROM temp_result WHERE need_update_balancing_node = 1);
        UPDATE rul_node_calculate_parameter SET commercial_node_calculate_parameter_id = v_commercial_node_calculate_parameter_id
        WHERE node_calculate_parameter_id IN (SELECT child_id FROM temp_result WHERE need_update_commercial_node = 1);
        DROP TABLE temp_result;
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_accounting_type_node(IN p_rul_accounting_type_node_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_accounting_type_node(IN p_rul_accounting_type_node_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
    SELECT COUNT(*), COALESCE(string_agg(rch.note,','),'')
    INTO v_cnt, v_err_name
    FROM rul_charge rch
    join rul_connection rc on rch.connection_id = rc.connection_id
    join rul_accounting_type_node ratn on rc.node_calculate_parameter_id = ratn.node_calculate_parameter_id
    WHERE rch.start_date >= ratn.start_date and COALESCE(rch.end_date, CURRENT_DATE) <= COALESCE(ratn.end_date, CURRENT_DATE) and ratn.accounting_type_node_id= ANY(p_rul_accounting_type_node_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_ACCOUNTING_TYPE_NODE',v_err_name);
    ELSE
    	SELECT array_agg(average_value_id) INTO v_ids FROM rul_average_value
        WHERE accounting_type_node_id = ANY (p_rul_accounting_type_node_ids);
    	DELETE FROM rul_average_value_argument WHERE average_value_id = ANY (v_ids);
        DELETE FROM rul_average_value WHERE average_value_id = ANY (v_ids);
        DELETE FROM rul_pipe_value WHERE accounting_type_node_id = ANY (p_rul_accounting_type_node_ids);
        DELETE FROM  rul_node_panel_argument
        WHERE accounting_type_node_id = ANY (p_rul_accounting_type_node_ids);
        DELETE FROM  rul_losses_params
        WHERE accounting_type_node_id = ANY (p_rul_accounting_type_node_ids);
        DELETE FROM rul_accounting_type_node
        WHERE accounting_type_node_id = ANY (p_rul_accounting_type_node_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_agreement(IN p_agreement_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_agreement(IN p_agreement_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_parent_agreement_id bigint;
BEGIN
    SELECT parent_agreement_id
    INTO v_parent_agreement_id
    FROM rul_agreement
    WHERE agreement_id = ANY (p_agreement_ids)  ;
    IF v_parent_agreement_id is NOT NULL THEN
    	DELETE FROM rul_agreement
        WHERE agreement_id = ANY (p_agreement_ids);
    ELSE
        SELECT COUNT(*), COALESCE(string_agg(agreement_name,','),'')
        INTO v_cnt, v_err_name
        FROM rul_agreement
        WHERE owner_agreement_id = ANY (p_agreement_ids)  ;
        IF v_cnt != 0 THEN
            RAISE EXCEPTION '%', get_message('ERR_AGREEMENT_OWNER',v_err_name);
        end if;
        SELECT COUNT(*), COALESCE(string_agg(connection_name,','),'')
        INTO v_cnt, v_err_name
        FROM rul_connection
        WHERE agreement_id = ANY (p_agreement_ids)  ;
        IF v_cnt != 0 THEN
            RAISE EXCEPTION '%', get_message('ERR_AGREEMENT_CONNECTION',v_err_name);
        end if;
        DELETE FROM  rul_agreement_service_type
        WHERE agreement_id = ANY (p_agreement_ids);
    	DELETE FROM rul_agreement
        WHERE parent_agreement_id  = ANY (p_agreement_ids);
    	DELETE FROM rul_agreement
        WHERE agreement_id = ANY (p_agreement_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_attribute_section_value(IN p_line_parameter_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_attribute_section_value(IN p_line_parameter_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_formula_id bigint;
    v_line_id bigint;
    v_formula_id_all bigint[];
    v_section_id_all bigint[];
    v_attribute_section_id bigint[];
BEGIN
SELECT formula_id, line_id into v_formula_id,v_line_id FROM rul_line_parameter WHERE line_parameter_id = ANY(p_line_parameter_ids);
SELECT array_agg(DISTINCT formula_id) into v_formula_id_all FROM rul_line_parameter WHERE line_id = v_line_id AND formula_id IS NOT NULL ;
SELECT  array_agg(section_id) into v_section_id_all FROM rul_section WHERE line_id = v_line_id;
SELECT array_agg(asf.attribute_section_id) into v_attribute_section_id
FROM rul_attribute_section_formula asf
WHERE asf.formula_id = v_formula_id
  AND NOT EXISTS (
        SELECT 1
        FROM rul_attribute_section_formula asf2
        WHERE asf2.attribute_section_id = asf.attribute_section_id
          AND asf2.formula_id = ANY (v_formula_id_all)
          AND asf2.formula_id <> v_formula_id
  );
	DELETE FROM rul_attribute_section_value WHERE section_id = ANY (v_section_id_all) and attribute_section_id= ANY(v_attribute_section_id);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_average_value_argument(IN p_average_value_argument_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_average_value_argument(IN p_average_value_argument_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
	DELETE FROM rul_average_value_argument WHERE average_value_argument_id = ANY (p_average_value_argument_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_balancing(IN p_balancing_id bigint[])
CREATE OR REPLACE PROCEDURE public.delete_balancing(IN p_balancing_id bigint[])
 LANGUAGE plpgsql
AS $procedure$
    DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
	-- Очищаем балансировки из расходов и начислений. Начисления надо как-то пересчитать
    -- Удаляем балансировку
    UPDATE rul_charge SET balancing_id = NULL
    WHERE balancing_id = ANY(p_balancing_id)
    AND invoice_id is NOT NULL;
    UPDATE rul_charge SET balancing_id = NULL,
      balancing_coefficient = NULL,
      sum_consumption = ROUND(rul_charge.sum_consumption / rul_charge.balancing_coefficient, 3),
      amount = ROUND(ROUND(rul_charge.sum_consumption / rul_charge.balancing_coefficient, 3) * rul_charge.base_value, 2),
      nds_rub = ROUND(ROUND(ROUND(rul_charge.sum_consumption / rul_charge.balancing_coefficient, 3) * rul_charge.base_value, 2) * rul_charge.nds_percent / 100, 2),
      amount_nds = ROUND(ROUND(rul_charge.sum_consumption / rul_charge.balancing_coefficient, 3) * rul_charge.base_value, 2) + ROUND(ROUND(ROUND(rul_charge.sum_consumption / rul_charge.balancing_coefficient, 3) * rul_charge.base_value, 2) * rul_charge.nds_percent / 100, 2)
    WHERE balancing_id = ANY(p_balancing_id) AND invoice_id is NULL;
    UPDATE rul_consumption_load SET balancing_id = NULL, balancing_coefficient = NULL WHERE balancing_id = ANY(p_balancing_id);
    UPDATE rul_consumption_standard SET balancing_id = NULL, balancing_coefficient = NULL WHERE balancing_id = ANY(p_balancing_id);
    UPDATE rul_consumption_source_connection SET balancing_id = NULL, balancing_coefficient = NULL WHERE balancing_id = ANY(p_balancing_id);
    DELETE FROM rul_consumption_losses WHERE balancing_id = ANY(p_balancing_id) AND is_balancing_losses = 1;
    UPDATE rul_consumption_losses SET balancing_id = NULL, balancing_coefficient = NULL WHERE balancing_id = ANY(p_balancing_id);
    DELETE FROM rul_balancing WHERE balancing_id = ANY (p_balancing_id);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_bank_account(IN p_bank_account_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_bank_account(IN p_bank_account_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
	DELETE FROM rul_bank_account WHERE bank_account_id = ANY (p_bank_account_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_brand(IN p_brand_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_brand(IN p_brand_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_panel_ids bigint[];
BEGIN
    SELECT COUNT(*), string_agg(serial_number,',')
    INTO v_cnt, v_err_name
    FROM rul_meter
    WHERE brand_id = ANY (p_brand_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_BRAND',v_err_name);
    ELSE
    	SELECT array_agg(panel_id) INTO v_panel_ids FROM rul_panel WHERE brand_id = ANY (p_brand_ids);
    	CALL delete_panel(v_panel_ids);
        DELETE FROM rul_brand_parameter WHERE brand_id = ANY (p_brand_ids);
    	DELETE FROM rul_brand_service_type WHERE brand_id = ANY (p_brand_ids);
    	DELETE FROM rul_brand WHERE brand_id = ANY (p_brand_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_charge(IN p_charge_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_charge(IN p_charge_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
	SELECT COUNT(*), string_agg(''::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_charge
    WHERE charge_id = ANY (p_charge_ids)
    AND charge_checked = 1;
	IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_CHARGE');
    ELSE
	    DELETE FROM rul_charge_detail WHERE charge_id = ANY (p_charge_ids);
		DELETE FROM rul_charge WHERE charge_id = ANY (p_charge_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_classifier(IN p_classifier_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_classifier(IN p_classifier_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	DELETE FROM rul_classifier_network_fragment WHERE classifier_id = ANY (p_classifier_ids);
    DELETE FROM rul_classifier WHERE classifier_id = ANY (p_classifier_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_client(IN p_client_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_client(IN p_client_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
    SELECT COUNT(*), string_agg(network_fragment_name::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_network_fragment
    WHERE client_id = ANY (p_client_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_CLIENT_NETWORF_FRAGMENT', v_err_name);
    ELSE
    	SELECT COUNT(*), string_agg(line_name::varchar,', ')
        INTO v_cnt, v_err_name
        FROM rul_line
        WHERE client_id = ANY (p_client_ids);
        IF v_cnt != 0 THEN
            RAISE EXCEPTION '%', get_message('ERR_DELETE_CLIENT_LINE', v_err_name);
        ELSE
        	SELECT COUNT(*), string_agg(serial_number::varchar,', ')
        	INTO v_cnt, v_err_name
           	FROM rul_meter
            WHERE client_id = ANY (p_client_ids)
            OR responsible_client_id = ANY (p_client_ids);
            IF v_cnt != 0 THEN
                RAISE EXCEPTION '%', get_message('ERR_DELETE_CLIENT_METER', v_err_name);
            ELSE
            	SELECT COUNT(*), string_agg(agreement_name::varchar,', ')
                INTO v_cnt, v_err_name
                FROM rul_agreement
                WHERE supplier_client_id = ANY (p_client_ids)
                OR owner_client_id = ANY (p_client_ids)
                OR customer_client_id = ANY (p_client_ids);
                IF v_cnt != 0 THEN
                    RAISE EXCEPTION '%', get_message('ERR_DELETE_CLIENT_AGREEMENT', v_err_name);
                ELSE
        			SELECT array_agg(bank_account_id)
                    INTO v_ids
                    FROM rul_bank_account
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_bank_account(v_ids);
                    SELECT array_agg(classifier_id)
                    INTO v_ids
                    FROM rul_classifier
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_classifier(v_ids);
                  	DELETE FROM rul_client_client_type WHERE client_id = ANY (p_client_ids);
                    SELECT array_agg(client_control_id)
                    INTO v_ids
                    FROM rul_client_control
                    WHERE curator_client_id = ANY (p_client_ids)
                    OR dependent_client_id = ANY (p_client_ids);
                	CALL delete_client_control(v_ids);
                    SELECT array_agg(client_object_id)
                    INTO v_ids
                    FROM rul_client_object
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_client_object(v_ids);
                	DELETE FROM rul_client_service_type WHERE client_id = ANY (p_client_ids);
                    -- Пока нет интерфейсов ведения групп и адм. назначений, поэтому удаление напрямую, но это неправильно.
                    DELETE FROM rul_group_consumption WHERE client_id = ANY (p_client_ids);
                    -- Пока нет интерфейсов ведения групп и адм. назначений, поэтому удаление напрямую, но это неправильно.
                    DELETE FROM rul_purpose_consumption WHERE client_id = ANY (p_client_ids);
                    -- Удаляем без метода, т.к. при удалении клиента надо удалить все его юр. лица
                    DELETE FROM rul_juristic_company WHERE client_id = ANY (p_client_ids);
                    SELECT array_agg(line_id)
                    INTO v_ids
                    FROM rul_line
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_line(v_ids);
                    SELECT array_agg(meter_id)
                    INTO v_ids
                    FROM rul_meter
                    WHERE client_id = ANY (p_client_ids)
                    OR responsible_client_id = ANY (p_client_ids);
                	CALL delete_meter(v_ids);
                    SELECT array_agg(network_fragment_id)
                    INTO v_ids
                    FROM rul_network_fragment
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_network_fragment(v_ids);
                    SELECT array_agg(node_id)
                    INTO v_ids
                    FROM rul_node
                    WHERE responsible_client_id = ANY (p_client_ids);
                	CALL delete_node(v_ids);
                    DELETE FROM rul_object_type WHERE client_id = ANY (p_client_ids);
                    SELECT array_agg(observation_id)
                    INTO v_ids
                    FROM rul_observation
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_observation(v_ids);
                	SELECT array_agg(operation_template_id)
                    INTO v_ids
                    FROM rul_operation_template
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_operation_template(v_ids);
                    SELECT array_agg(precipitation_id)
                    INTO v_ids
                    FROM rul_precipitation
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_precipitation(v_ids);
                    SELECT array_agg(rate_id)
                    INTO v_ids
                    FROM rul_rate
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_rate(v_ids);
                    DELETE FROM rul_report_type WHERE client_id = ANY (p_client_ids);
                    DELETE FROM rul_service WHERE client_id = ANY (p_client_ids);
                    SELECT array_agg(standard_id)
                    INTO v_ids
                    FROM rul_standard
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_standard(v_ids);
                    SELECT COALESCE(array_agg(correlation_transaction_id), '{}')
                    INTO v_ids
                    FROM rul_transaction
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_transaction(v_ids);
                    SELECT array_agg(user_id)
                    INTO v_ids
                    FROM rul_user
                    WHERE client_id = ANY (p_client_ids);
                	CALL delete_user(v_ids);
                    DELETE FROM rul_client WHERE client_id = ANY (p_client_ids);
            	END IF;
        	END IF;
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_client_control(IN p_client_control_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_client_control(IN p_client_control_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
	DELETE FROM rul_client_control WHERE client_control_id = ANY (p_client_control_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_client_object(IN p_client_object_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_client_object(IN p_client_object_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg(connection_name::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_connection
    WHERE client_object_id = ANY (p_client_object_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_CLIENT_OBJECT',v_err_name);
    ELSE
    	DELETE FROM rul_client_object WHERE client_object_id = ANY (p_client_object_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_connection(IN p_connection_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_connection(IN p_connection_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
    SELECT COUNT(*), string_agg(''::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_charge
    WHERE connection_id = ANY (p_connection_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_CONNECTION_CHARGE');
    ELSE
    	SELECT COUNT(*), string_agg(''::varchar,', ')
        INTO v_cnt, v_err_name
        FROM rul_connection_connection
        WHERE source_connection_id = ANY (p_connection_ids);
        IF v_cnt != 0 THEN
            RAISE EXCEPTION '%', get_message('ERR_DELETE_CONNECTION_SOURCE_CONNECTION');
        ELSE
			DELETE FROM rul_consumption_load WHERE connection_id = ANY (p_connection_ids);
            DELETE FROM rul_consumption_standard WHERE connection_id = ANY (p_connection_ids);
            DELETE FROM rul_consumption_average WHERE connection_id = ANY (p_connection_ids);
            DELETE FROM rul_consumption_pipe WHERE connection_id = ANY (p_connection_ids);
            DELETE FROM rul_consumption_source_connection WHERE connection_id = ANY (p_connection_ids);
            DELETE FROM rul_consumption_losses WHERE connection_id = ANY (p_connection_ids);
            SELECT array_agg(planned_consumption_id)
            INTO v_ids
            FROM rul_planned_consumption
            WHERE connection_id = ANY (p_connection_ids);
            CALL delete_planned_consumption(v_ids);
			DELETE FROM rul_connection_connection WHERE destination_connection_id = ANY (p_connection_ids);
            SELECT array_agg(version_load_standard_id)
            INTO v_ids
            FROM rul_version_load_standard rvls
            JOIN rul_formula_connection rfc
            	ON rvls.formula_connection_id = rfc.formula_connection_id
            WHERE rfc.connection_id = ANY (p_connection_ids);
            CALL delete_version_load_standard(v_ids);
            DELETE FROM rul_formula_connection WHERE connection_id = ANY (p_connection_ids);
            DELETE FROM rul_connection WHERE connection_id = ANY (p_connection_ids);
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_connection_connection(IN p_connection_connection_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_connection_connection(IN p_connection_connection_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
      DELETE FROM rul_connection_connection WHERE connection_connection_id = ANY (p_connection_connection_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_currency_rate(IN p_currency_rate_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_currency_rate(IN p_currency_rate_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    	DELETE FROM rul_currency_rate
        WHERE currency_rate_id = ANY (p_currency_rate_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_district(IN p_district_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_district(IN p_district_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg((select shortname from rul_juristic_company where client_id = rc.client_id
    							 and start_date <= now() and coalesce(end_date,now())>=now() )::varchar,',')
    INTO v_cnt, v_err_name
    FROM rul_client rc
    WHERE rc.district_id = ANY (p_district_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_DISTRICT_COMPANY',v_err_name);
    ELSE
    	SELECT COUNT(*), string_agg(shortname::varchar,', ')
        INTO v_cnt, v_err_name
        FROM rul_juristic_company rjc
        WHERE rjc.district_id = ANY (p_district_ids);
        IF v_cnt != 0 THEN
    		RAISE EXCEPTION '%', get_message('ERR_DELETE_DISTRICT_COMPANY',v_err_name);
    	ELSE
        	SELECT COUNT(*), string_agg(ro.object_name::varchar,', ')
            INTO v_cnt, v_err_name
            FROM rul_object ro
            WHERE ro.district_id = ANY (p_district_ids);
            IF v_cnt != 0 THEN
    			RAISE EXCEPTION '%', get_message('ERR_DELETE_DISTRICT_OBJECT',v_err_name);
    		ELSE
            	SELECT COUNT(*), string_agg(rl.locality_name::varchar,', ')
                INTO v_cnt, v_err_name
                FROM rul_locality rl
                WHERE rl.district_id = ANY (p_district_ids);
                IF v_cnt != 0 THEN
                    RAISE EXCEPTION '%', get_message('ERR_DELETE_DISTRICT_LOCALITY',v_err_name);
                ELSE
                	DELETE FROM rul_district WHERE district_id = ANY (p_district_ids);
                END IF;
            END IF;
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_entity(IN p_code character varying, IN p_entity_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_entity(IN p_code character varying, IN p_entity_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    IF p_code = 'rate' THEN
    	CALL delete_rate(p_entity_ids);
    ELSIF p_code = 'rate_value' THEN
    	CALL delete_rate_value(p_entity_ids);
    ELSIF p_code = 'brand' THEN
    	CALL delete_brand(p_entity_ids);
    ELSIF p_code = 'meter' THEN
    	CALL delete_meter(p_entity_ids);
    ELSIF p_code = 'panel' THEN
    	CALL delete_panel(p_entity_ids);
    ELSIF p_code = 'meter_check' THEN
    	CALL delete_meter_check(p_entity_ids);
    ELSIF p_code = 'operation_template' THEN
    	CALL delete_operation_template(p_entity_ids);
    ELSIF p_code = 'transaction_template' THEN
    	CALL delete_transaction_template(p_entity_ids);
    ELSIF p_code = 'classifier' THEN
    	CALL delete_classifier(p_entity_ids);
    ELSIF p_code = 'line_parameter' THEN
    	CALL delete_line_parameter(p_entity_ids);
    ELSIF p_code = 'section' THEN
    	CALL delete_section(p_entity_ids);
    ELSIF p_code = 'line' THEN
    	CALL delete_line(p_entity_ids);
    ELSIF p_code = 'network_fragment' THEN
    	CALL delete_network_fragment(p_entity_ids);
    ELSIF p_code = 'street' THEN
    	CALL delete_street(p_entity_ids);
    ELSIF p_code = 'locality' THEN
    	CALL delete_locality(p_entity_ids);
    ELSIF p_code = 'district' THEN
    	CALL delete_district(p_entity_ids);
    ELSIF p_code = 'balancing' THEN
    	CALL delete_balancing(p_entity_ids);
    ELSIF p_code = 'node' THEN
    	CALL delete_node(p_entity_ids);
    ELSIF p_code = 'node_panel_value' THEN
    	CALL delete_node_panel_value(p_entity_ids);
    ELSIF p_code = 'node_panel' THEN
    	CALL delete_node_panel(p_entity_ids);
    ELSIF p_code = 'node_meter' THEN
    	CALL delete_node_meter(p_entity_ids);
    ELSIF p_code = 'accounting_type_node' THEN
    	CALL delete_accounting_type_node(p_entity_ids);
    ELSIF p_code = 'node_calculate_parameter' THEN
    	CALL delete_node_calculate_parameter(p_entity_ids);
    ELSIF p_code = 'precipitation' THEN
    	CALL delete_precipitation(p_entity_ids);
    ELSIF p_code = 'observation' THEN
    	CALL delete_observation(p_entity_ids);
    ELSIF p_code = 'version_load_standard' THEN
    	CALL delete_version_load_standard(p_entity_ids);
    ELSIF p_code = 'planned_consumption' THEN
    	CALL delete_planned_consumption(p_entity_ids);
    ELSIF p_code = 'connection' THEN
    	CALL delete_connection(p_entity_ids);
    ELSIF p_code = 'holiday' THEN
    	CALL delete_holiday(p_entity_ids);
    ELSIF p_code = 'agreement' THEN
    	CALL delete_agreement(p_entity_ids);
    ELSIF p_code = 'currency_rate' THEN
    	CALL delete_currency_rate(p_entity_ids);
    ELSIF p_code = 'standard' THEN
    	CALL delete_standard(p_entity_ids);
    ELSIF p_code = 'version_constant' THEN
    	CALL delete_version_constant(p_entity_ids);
    ELSIF p_code = 'object' THEN
    	CALL delete_object(p_entity_ids);
    ELSIF p_code = 'client_object' THEN
    	CALL delete_client_object(p_entity_ids);
    ELSIF p_code = 'juristic_company' THEN
    	CALL delete_juristic_company(p_entity_ids);
    ELSIF p_code = 'bank_account' THEN
    	CALL delete_bank_account(p_entity_ids);
    ELSIF p_code = 'client_control' THEN
    	CALL delete_client_control(p_entity_ids);
    ELSIF p_code = 'user' THEN
    	CALL delete_user(p_entity_ids);
    ELSIF p_code = 'client' THEN
    	CALL delete_client(p_entity_ids);
    ELSIF p_code = 'line_parameter_child' THEN
    	CALL delete_line_parameter_child(p_entity_ids);
    ELSIF p_code = 'charge' THEN
    	CALL delete_charge(p_entity_ids);
    ELSIF p_code = 'invoice' THEN
    	CALL delete_invoice(p_entity_ids);
    ELSIF p_code = 'connection_connection' THEN
    	CALL delete_connection_connection(p_entity_ids);
    ELSIF p_code = 'formula_connection' THEN
    	CALL delete_formula_connection(p_entity_ids);
    ELSIF p_code = 'average_value_argument' THEN
    	CALL delete_average_value_argument(p_entity_ids);
    ELSIF p_code = 'invoice' THEN
    	CALL delete_invoice(p_entity_ids);
    ELSIF p_code = 'transaction' THEN
    	CALL delete_transaction(p_entity_ids);
    ELSIF p_code = 'node_panel_argument' THEN
    	CALL delete_node_panel_argument(p_entity_ids);
    END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_formula_connection(IN p_formula_connection_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_formula_connection(IN p_formula_connection_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
	SELECT COUNT(*), string_agg(''::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_version_load_standard
    WHERE formula_connection_id = ANY (p_formula_connection_ids);
	IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_FORMULA_CONNECTION');
    ELSE
		DELETE FROM rul_formula_connection WHERE formula_connection_id = ANY (p_formula_connection_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_holiday(IN p_holiday_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_holiday(IN p_holiday_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    DELETE FROM rul_holiday WHERE holiday_id = ANY (p_holiday_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_invoice(IN p_invoice_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_invoice(IN p_invoice_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_invoice_id bigint;
    v_confirm_id bigint;
    v_start_date timestamp;
    v_end_date timestamp;
BEGIN
	-- Метод для удаления счетов.
	-- Цикл по всем переданным счетам для рассчетов
    -- При удалении должны переформировываться затронутые счета :(
    FOREACH v_invoice_id IN ARRAY p_invoice_ids
    LOOP
    	SELECT date_trunc('month',billing_start_date),date_trunc('month',billing_start_date) + interval '1 month' - interval '1 second',invoice_confirm_status_id
        INTO v_start_date,v_end_date,v_confirm_id FROM rul_invoice WHERE invoice_id = v_invoice_id;
        IF (SELECT count(*) FROM rul_invoice
          WHERE agreement_id in (SELECT agreement_id FROM rul_agreement
          							WHERE code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = v_invoice_id)))
            AND billing_start_date >= v_end_date) != 0 THEN
          RAISE EXCEPTION '%', get_message('ERR_DELETE_INVOICE_CHARGES');
      	END IF;
        -- Ошибка при наличии подтвержденных счетов
        IF v_confirm_id in (2,3)
        	THEN
            	RAISE EXCEPTION '%', get_message('ERR_DELETE_INVOICE');
            	--RAISE EXCEPTION '%', '[[Невозможно выполнить операцию, т.к. один или несколько связанных счетов-фактур уже подтверждены]]' USING ERRCODE = '25002';
        END IF;
    	CALL public.clear_all_for_recalculate_invoice(v_invoice_id,v_start_date,v_end_date);
        UPDATE rul_charge SET invoice_id = NULL WHERE invoice_id = v_invoice_id;
        DELETE FROM rul_invoice WHERE invoice_id = v_invoice_id;
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_juristic_company(IN p_juristic_company_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_juristic_company(IN p_juristic_company_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
    SELECT COUNT(*), string_agg(shortname::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_juristic_company
    WHERE client_id IN (SELECT client_id FROM rul_juristic_company WHERE juristic_company_id = ANY (p_juristic_company_ids));
    IF v_cnt = 1 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_JURISTIC_COMPANY');
    ELSE
        DELETE FROM rul_juristic_company WHERE juristic_company_id = ANY (p_juristic_company_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_line(IN p_line_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_line(IN p_line_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_ids BIGINT[];
BEGIN
	SELECT array_agg(section_id)
    INTO v_ids
    FROM rul_section
    WHERE line_id = ANY (p_line_ids);
	CALL delete_section(v_ids);
    SELECT array_agg(line_parameter_id)
    INTO v_ids
    FROM rul_line_parameter
    WHERE line_id = ANY (p_line_ids);
    CALL delete_line_parameter(v_ids);
	DELETE FROM rul_line WHERE line_id = ANY (p_line_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_line_parameter(IN p_line_parameter_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_line_parameter(IN p_line_parameter_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_line_parameter_child bigint[];
BEGIN
	--Проверка на фронте
	/*FOREACH v_line_parameter IN ARRAY p_line_parameter_ids
    LOOP
      SELECT COUNT(*), string_agg(line_id::varchar,', ')
      INTO v_cnt, v_err_name
      FROM rul_line_parameter
      WHERE line_id = (SELECT line_id FROM rul_line_parameter WHERE line_parameter_id = v_line_parameter);
      IF v_cnt = 1 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_LINE_PARAMETER');
      END IF;
    END LOOP;*/
	CALL delete_attribute_section_value(p_line_parameter_ids);
	SELECT array_agg(line_parameter_child_id)
    INTO v_line_parameter_child
    FROM rul_line_parameter_child
    WHERE line_parameter_id = ANY (p_line_parameter_ids);
	CALL delete_entity('line_parameter_child',v_line_parameter_child);
	DELETE FROM rul_line_parameter WHERE line_parameter_id = ANY (p_line_parameter_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_line_parameter_child(IN p_line_parameter_child_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_line_parameter_child(IN p_line_parameter_child_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
	SELECT array_agg(node_calculate_parameter_id)
    INTO v_ids
    FROM rul_line_parameter_child
    WHERE line_parameter_child_id = ANY (p_line_parameter_child_ids);
	DELETE FROM rul_line_parameter_child WHERE line_parameter_child_id = ANY (p_line_parameter_child_ids);
    IF v_ids IS NOT NULL
    THEN
        CALL define_balancing_node(v_ids);
    END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_locality(IN p_locality_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_locality(IN p_locality_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg((select shortname from rul_juristic_company where client_id = rc.client_id
    							 and start_date <= now() and coalesce(end_date,now())>=now() )::varchar,',')
    INTO v_cnt, v_err_name
    FROM rul_client rc
    WHERE rc.locality_id = ANY (p_locality_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_LOCALITY_COMPANY',v_err_name);
    ELSE
    	SELECT COUNT(*), string_agg(shortname::varchar,', ')
        INTO v_cnt, v_err_name
        FROM rul_juristic_company rjc
        WHERE rjc.locality_id = ANY (p_locality_ids);
        IF v_cnt != 0 THEN
    		RAISE EXCEPTION '%', get_message('ERR_DELETE_LOCALITY_COMPANY',v_err_name);
    	ELSE
        	SELECT COUNT(*), string_agg(ro.object_name::varchar,', ')
            INTO v_cnt, v_err_name
            FROM rul_object ro
            WHERE ro.locality_id = ANY (p_locality_ids);
            IF v_cnt != 0 THEN
    			RAISE EXCEPTION '%', get_message('ERR_DELETE_LOCALITY_OBJECT',v_err_name);
    		ELSE
            	SELECT COUNT(*), string_agg(rp.precipitation_id::varchar,', ')
                INTO v_cnt, v_err_name
                FROM rul_precipitation rp
                WHERE rp.locality_id = ANY (p_locality_ids);
                IF v_cnt != 0 THEN
                    RAISE EXCEPTION '%', get_message('ERR_DELETE_LOCALITY_PRECIPITATION');
                ELSE
                	SELECT COUNT(*), string_agg(ro.observation_id::varchar,', ')
                    INTO v_cnt, v_err_name
                    FROM rul_observation ro
                    WHERE ro.locality_id = ANY (p_locality_ids);
                    IF v_cnt != 0 THEN
                        RAISE EXCEPTION '%', get_message('ERR_DELETE_LOCALITY_OBSERVATION');
                    ELSE
                    	SELECT COUNT(*), string_agg(rs.street_name::varchar,', ')
                        INTO v_cnt, v_err_name
                        FROM rul_street rs
                        WHERE rs.locality_id = ANY (p_locality_ids);
                        IF v_cnt != 0 THEN
                            RAISE EXCEPTION '%', get_message('ERR_DELETE_LOCALITY_STREET',v_err_name);
                        ELSE
                            DELETE FROM rul_locality WHERE locality_id = ANY (p_locality_ids);
                        END IF;
                    END IF;
                END IF;
            END IF;
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_meter(IN p_meter_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_meter(IN p_meter_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg(rn.node_name::varchar,' ,')
    INTO v_cnt, v_err_name
    FROM rul_node_meter rnm
    JOIN rul_node rn
    	ON rn.node_id = rnm.node_id
    WHERE rnm.meter_id = ANY (p_meter_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_METER',v_err_name);
    ELSE
    	DELETE FROM rul_meter_check WHERE meter_id = ANY (p_meter_ids);
    	DELETE FROM rul_meter WHERE meter_id = ANY (p_meter_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_meter_check(IN p_meter_check_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_meter_check(IN p_meter_check_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    DELETE FROM rul_meter_check WHERE meter_check_id = ANY (p_meter_check_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_network_fragment(IN p_network_fragment_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_network_fragment(IN p_network_fragment_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg(line_name,', ')
    INTO v_cnt, v_err_name
    FROM rul_line
    WHERE network_fragment_id = ANY (p_network_fragment_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_NETWORK_FRAGMENT',v_err_name);
    ELSE
    	DELETE FROM rul_network_fragment_link
        WHERE parent_network_fragment_id = ANY (p_network_fragment_ids)
        OR child_network_fragment_id = ANY (p_network_fragment_ids);
    	DELETE FROM rul_classifier_network_fragment WHERE network_fragment_id = ANY (p_network_fragment_ids);
        DELETE FROM rul_network_fragment WHERE network_fragment_id = ANY (p_network_fragment_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_node(IN p_node_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_node(IN p_node_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_node_calculate_parameter_ids bigint[];
BEGIN
    SELECT COUNT(*), COALESCE(string_agg(line_name,','),'')
    INTO v_cnt, v_err_name
    FROM rul_line
    WHERE node_id = ANY (p_node_ids) or  child_node_id = ANY (p_node_ids) ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_NODE_LINE',v_err_name);
    ELSE
        select array_agg(node_calculate_parameter_id)
        into v_node_calculate_parameter_ids
        from rul_node_calculate_parameter where node_id = ANY (p_node_ids);
        call delete_node_calculate_parameter(v_node_calculate_parameter_ids);
        DELETE FROM rul_node
        WHERE node_id = ANY (p_node_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_node_calculate_parameter(IN p_node_calculate_parameter_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_node_calculate_parameter(IN p_node_calculate_parameter_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_accounting_type_node_ids bigint[];
BEGIN
    SELECT COUNT(*), COALESCE(string_agg(connection_name,','),'')
    INTO v_cnt, v_err_name
    FROM rul_connection
    WHERE node_calculate_parameter_id = ANY (p_node_calculate_parameter_ids) ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_NODE_CALCULATE_PARAMETER_CONNECT',v_err_name);
    end if;
    SELECT COUNT(*),
       COALESCE(string_agg(
       (SELECT line_name FROM rul_line
       WHERE line_id IN (SELECT line_id FROM rul_line_parameter
       					 WHERE line_parameter_id = t.line_parameter_id
       						))::text, ',') , '')
    INTO v_cnt, v_err_name
    FROM (
        SELECT line_parameter_id
        FROM rul_line_parameter
        WHERE node_calculate_parameter_id = ANY (p_node_calculate_parameter_ids)
        UNION ALL
        SELECT line_parameter_id
        FROM rul_line_parameter_child
        WHERE node_calculate_parameter_id = ANY (p_node_calculate_parameter_ids)
    ) t;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_NODE_CALCULATE_PARAMETER_LINE',v_err_name);
    ELSE
        select array_agg(accounting_type_node_id)
        into v_accounting_type_node_ids
        from rul_accounting_type_node where node_calculate_parameter_id = ANY (p_node_calculate_parameter_ids);
        call delete_accounting_type_node(v_accounting_type_node_ids);
        DELETE FROM rul_node_calculate_parameter
        WHERE node_calculate_parameter_id = ANY (p_node_calculate_parameter_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_node_meter(IN p_rul_node_meter_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_node_meter(IN p_rul_node_meter_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_node_panel_ids bigint[];
BEGIN
select array_agg(node_panel_id)
        into v_node_panel_ids
        from rul_node_panel where node_meter_id = ANY (p_rul_node_meter_ids);
        call delete_node_panel(v_node_panel_ids);
        DELETE FROM rul_node_meter
        WHERE node_meter_id = ANY (p_rul_node_meter_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_node_panel(IN p_rul_node_panel_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_node_panel(IN p_rul_node_panel_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), COALESCE(string_agg(''::varchar,' ,'),'')
    INTO v_cnt, v_err_name
    FROM rul_node_panel_value
    WHERE node_panel_id = ANY (p_rul_node_panel_ids) ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_NODE_PANEL_VALUE');
    end if;
    SELECT COUNT(*), COALESCE(string_agg(node_panel_argument_id::varchar,','),'')
    INTO v_cnt, v_err_name
    FROM rul_node_panel_argument
    WHERE node_panel_id = ANY (p_rul_node_panel_ids) ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_NODE_PANEL_ARGUMENT');
    ELSE
        DELETE FROM rul_node_panel
        WHERE node_panel_id = ANY (p_rul_node_panel_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_node_panel_argument(IN p_rul_node_panel_argument_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_node_panel_argument(IN p_rul_node_panel_argument_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	DELETE FROM rul_preconsumption
        WHERE node_panel_argument_id = ANY (p_rul_node_panel_argument_ids);
	DELETE FROM rul_consumption
    	WHERE node_panel_argument_id = ANY (p_rul_node_panel_argument_ids);
   	DELETE FROM rul_node_panel_argument
        WHERE node_panel_argument_id = ANY (p_rul_node_panel_argument_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_node_panel_value(IN p_rul_node_panel_value_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_node_panel_value(IN p_rul_node_panel_value_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    	DELETE FROM rul_node_panel_value
        WHERE node_panel_value_id = ANY (p_rul_node_panel_value_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_object(IN p_object_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_object(IN p_object_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
    SELECT COUNT(*), string_agg(node_name::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_node
    WHERE object_id = ANY (p_object_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_OBJECT',v_err_name);
    ELSE
    	SELECT array_agg(client_object_id)
        INTO v_ids
        FROM rul_client_object
        WHERE object_id = ANY (p_object_ids);
    	CALL delete_client_object(v_ids);
        DELETE FROM rul_inspector_route WHERE object_id = ANY (p_object_ids);
    	DELETE FROM rul_object WHERE object_id = ANY (p_object_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_observation(IN p_observation_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_observation(IN p_observation_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    	DELETE FROM rul_observation
        WHERE observation_id = ANY (p_observation_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_operation_template(IN p_operation_template_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_operation_template(IN p_operation_template_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_operation_template_ids bigint[];
BEGIN
    SELECT COUNT(*), COALESCE(string_agg(invoice_code,','),'')
    INTO v_cnt, v_err_name
    FROM rul_operation join rul_invoice ri on rul_operation.invoice_id = ri.invoice_id
    WHERE operation_template_id = ANY (p_operation_template_ids) ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_OPERATION_TEMPLATE',v_err_name);
    end if;
    SELECT COUNT(*), COALESCE(string_agg(agreement_name,','),'')
    INTO v_cnt, v_err_name
    FROM rul_agreement
    WHERE penalty_operation_template_id = ANY (p_operation_template_ids) ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_OPERATION_TEMPLATE_AGREEMENT',v_err_name);
    end if;
    SELECT COUNT(*), COALESCE(string_agg(connection_name,','),'')
    INTO v_cnt, v_err_name
    FROM rul_connection
    WHERE service_operation_template_id = ANY (p_operation_template_ids) or indexing_operation_template_id = ANY (p_operation_template_ids) or advance_operation_template_id = ANY (p_operation_template_ids) ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_OPERATION_TEMPLATE_CONNECTION',v_err_name);
    ELSE
        select array_agg(transaction_template_id)
        into v_operation_template_ids
        from rul_transaction_template where operation_template_id = ANY (p_operation_template_ids);
        call delete_transaction_template(v_operation_template_ids);
        DELETE FROM rul_operation_template
        WHERE operation_template_id = ANY (p_operation_template_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_panel(IN p_panel_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_panel(IN p_panel_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg(rn.node_name::varchar,' ,')
    INTO v_cnt, v_err_name
    FROM rul_node_panel rnp
    JOIN rul_node_meter rnm
    	ON rnp.node_meter_id = rnm.node_meter_id
    JOIN rul_node rn
    	ON rn.node_id = rnm.node_id
    WHERE rnp.panel_id = ANY (p_panel_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_PANEL',v_err_name);
    ELSE
    	DELETE FROM rul_panel_parameter WHERE panel_id = ANY (p_panel_ids);
    	DELETE FROM rul_panel WHERE panel_id = ANY (p_panel_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_planned_consumption(IN p_planned_consumption_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_planned_consumption(IN p_planned_consumption_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg((select connection_name from rul_connection where connection_id = rch.connection_id)::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_charge rch
    JOIN rul_planned_consumption rpc
    ON rch.connection_id = rpc.connection_id
    AND rch.billing_start_date <= rpc.start_date
    AND rch.billing_end_date >= rpc.end_date
    WHERE rch.source_id = 4::BIGINT
    AND rpc.planned_consumption_id = ANY (p_planned_consumption_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_PLANNED_CONSUMPTION',v_err_name);
    ELSE
        DELETE FROM rul_planned_consumption WHERE planned_consumption_id = ANY (p_planned_consumption_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_precipitation(IN p_precipitation_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_precipitation(IN p_precipitation_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    	DELETE FROM rul_precipitation
        WHERE precipitation_id = ANY (p_precipitation_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_rate(IN p_rate_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_rate(IN p_rate_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_rate_value_ids bigint[];
BEGIN
    SELECT COUNT(*), COALESCE(string_agg(rul_connection.connection_name,','),'')
    INTO v_cnt, v_err_name
    FROM rul_connection
    WHERE rate_id = ANY (p_rate_ids) or losses_rate_id= ANY(p_rate_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_RATE',v_err_name);
    ELSE
        select array_agg(rate_value_id)
        into v_rate_value_ids
        from rul_rate_value where rate_id = ANY (p_rate_ids);
        call delete_rate_value(v_rate_value_ids);
        DELETE FROM rul_rate
        WHERE rate_id = ANY (p_rate_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_rate_value(IN p_rate_value_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_rate_value(IN p_rate_value_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    	DELETE FROM rul_rate_value
        WHERE rate_value_id = ANY (p_rate_value_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_section(IN p_section_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_section(IN p_section_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	--DELETE FROM rul_consumption_losses WHERE section_id = ANY (p_section_ids);
	DELETE FROM rul_attribute_section_value WHERE section_id = ANY (p_section_ids);
	DELETE FROM rul_section WHERE section_id = ANY (p_section_ids);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_standard(IN p_standard_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_standard(IN p_standard_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids BIGINT[];
BEGIN
    SELECT COUNT(*), string_agg(rc.connection_name::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_standard rs
    JOIN rul_formula_connection rfc
    	ON rs.formula_id = rfc.formula_id
    JOIN rul_connection rc
    	ON rc.connection_id = rfc.connection_id
    WHERE rs.standard_id = ANY (p_standard_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_STANDARD',v_err_name);
    ELSE
    	SELECT COUNT(*), string_agg(rc.connection_name::varchar,', ')
        INTO v_cnt, v_err_name
        FROM rul_standard rs
        JOIN rul_connection_connection rcc
            ON rs.formula_id = rcc.formula_id
        JOIN rul_connection rc
            ON rc.connection_id = rcc.destination_connection_id
        WHERE rs.standard_id = ANY (p_standard_ids);
        IF v_cnt != 0 THEN
            RAISE EXCEPTION '%', get_message('ERR_DELETE_STANDARD',v_err_name);
        ELSE
          SELECT array_agg(rvc.version_constant_id)
          INTO v_ids
          FROM rul_version_constant rvc
          JOIN rul_standard rs
              ON rs.formula_id = rvc.formula_id
          WHERE rs.standard_id = ANY (p_standard_ids);
          CALL delete_version_constant(v_ids);
          SELECT array_agg(rs.formula_id)
          INTO v_ids
          FROM rul_standard rs
          WHERE rs.standard_id = ANY (p_standard_ids);
          DELETE FROM rul_standard WHERE standard_id = ANY (p_standard_ids);
          DELETE FROM rul_argument_formula WHERE formula_id = ANY (v_ids);
          DELETE FROM rul_formula WHERE formula_id = ANY (v_ids);
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_street(IN p_street_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_street(IN p_street_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg((select shortname from rul_juristic_company where client_id = rc.client_id
    							 and start_date <= now() and coalesce(end_date,now())>=now() )::varchar,',')
    INTO v_cnt, v_err_name
    FROM rul_client rc
    WHERE rc.street_id = ANY (p_street_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_STREET_COMPANY',v_err_name);
    ELSE
    	SELECT COUNT(*), string_agg(shortname::varchar,',')
        INTO v_cnt, v_err_name
        FROM rul_juristic_company rjc
        WHERE rjc.street_id = ANY (p_street_ids);
        IF v_cnt != 0 THEN
    		RAISE EXCEPTION '%', get_message('ERR_DELETE_STREET_COMPANY',v_err_name);
    	ELSE
        	SELECT COUNT(*), string_agg(ro.object_name::varchar,',')
            INTO v_cnt, v_err_name
            FROM rul_object ro
            WHERE ro.street_id = ANY (p_street_ids);
            IF v_cnt != 0 THEN
    			RAISE EXCEPTION '%', get_message('ERR_DELETE_STREET_OBJECT',v_err_name);
    		ELSE
                DELETE FROM rul_street WHERE street_id = ANY (p_street_ids);
            END IF;
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_transaction(IN p_correlation_transaction_id bigint[])
CREATE OR REPLACE PROCEDURE public.delete_transaction(IN p_correlation_transaction_id bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	i record;
	v_start_date timestamp;
    v_end_date timestamp;
    v_correlation_transaction_id BIGINT;
BEGIN
	FOREACH v_correlation_transaction_id IN ARRAY p_correlation_transaction_id
    LOOP
    	v_start_date := (SELECT date_trunc('month',MAX(operation_date)) FROM rul_transaction
    							WHERE correlation_transaction_id = v_correlation_transaction_id);
        v_end_date := (SELECT date_trunc('month',MAX(operation_date)) + interval '1 month' - interval '1 second' FROM rul_transaction
    							WHERE correlation_transaction_id = v_correlation_transaction_id);
      -- Надо понять какя транзакция удаляется, сторнирующая или нет.
      IF EXISTS (
                  SELECT 1
                  FROM rul_transaction_reversal
                  WHERE source_correlation_transaction_id = v_correlation_transaction_id
                     OR storn_correlation_transaction_id = v_correlation_transaction_id
              )
      THEN
          RAISE NOTICE 'Удаление сторнирующей проводки';
          -- Значит это удаление сторнированной проводки
          -- Помечаем удаленной запись
          UPDATE rul_transaction_reversal SET deleted = 1::smallint where (storn_correlation_transaction_id = v_correlation_transaction_id
                                                                          or source_correlation_transaction_id = v_correlation_transaction_id);
          UPDATE rul_transaction SET deleted = 1::smallint where correlation_transaction_id = v_correlation_transaction_id;
      ELSE
          RAISE NOTICE 'Удаление обычной проводки';
          -- Значит удаление обычной проводки
          -- Помечаем удаленной запись
          UPDATE rul_transaction SET deleted = 1::smallint where correlation_transaction_id = v_correlation_transaction_id;
      END IF;
      -- Когда будут добавлены подтверждения, нужно будет выдавать ошибки.
      -- Мы должны все пересчитать по коду АУ этой транзакции
      -- В пересчет надо добавить, что записи помеченные на удаление удаляются после метода очистки
      FOR i IN
          SELECT invoice_id FROM rul_invoice
          WHERE agreement_id IN (SELECT agreement_id FROM rul_agreement
                                 WHERE code_ay IN (SELECT code_ay FROM rul_transaction
                                                   WHERE correlation_transaction_id = v_correlation_transaction_id
                                                   )
                                )
          AND billing_start_date <= v_start_date
          AND billing_end_date <= v_end_date
      LOOP
          RAISE NOTICE 'Запуск пересчета по %, %, %', i.invoice_id, v_start_date, v_end_date;
          CALL public.process_invoice(ARRAY[i.invoice_id], v_start_date, v_end_date);
      END LOOP;
	END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_transaction_template(IN p_transaction_template_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_transaction_template(IN p_transaction_template_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), COALESCE(string_agg(content,','),'')
    INTO v_cnt, v_err_name
    FROM rul_transaction
    WHERE transaction_template_id = ANY (p_transaction_template_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_TRANSACTION_TEMPLATE',v_err_name);
    ELSE
        DELETE FROM rul_transaction_template
        WHERE transaction_template_id = ANY (p_transaction_template_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_user(IN p_user_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_user(IN p_user_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
    v_ids bigint[];
BEGIN
    SELECT COUNT(*), string_agg(agreement_name::varchar,',')
    INTO v_cnt, v_err_name
    FROM rul_agreement
    WHERE supplier_user_id = ANY (p_user_ids)
    OR customer_user_id = ANY (p_user_ids)
    OR owner_user_id = ANY (p_user_ids)
    OR supplier_responsible_user_id = ANY (p_user_ids)
    OR customer_responsible_user_id = ANY (p_user_ids)
    OR second_supplier_user_id = ANY (p_user_ids)
    ;
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_USER', v_err_name);
    ELSE
    	DELETE FROM rul_inspector_route WHERE user_id = ANY (p_user_ids);
        DELETE FROM rul_task WHERE user_id = ANY (p_user_ids);
        DELETE FROM rul_last_month_node_panel_value WHERE changed_user_id = ANY (p_user_ids);
        SELECT array_agg(node_panel_value_id)
        INTO v_ids
        FROM rul_node_panel_value WHERE changed_user_id = ANY (p_user_ids);
        CALL delete_node_panel_value(v_ids);
        DELETE FROM rul_user WHERE user_id = ANY (p_user_ids);
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_version_constant(IN p_version_constant_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_version_constant(IN p_version_constant_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg(rc.connection_name::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_version_constant rvc
    JOIN rul_formula_connection rfc
    	ON rvc.formula_id = rfc.formula_id
    JOIN rul_connection rc
    	ON rc.connection_id = rfc.connection_id
    JOIN rul_charge rch
    	ON rch.connection_id = rc.connection_id
        AND rch.start_date <= coalesce(rvc.end_date,'2100-04-30 23:59:59+03')
        AND rch.end_date >= rvc.start_date
    WHERE rvc.version_constant_id = ANY (p_version_constant_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_VERSION_CONSTANT',v_err_name);
    ELSE
    	SELECT COUNT(*), string_agg(rc.connection_name::varchar,', ')
        INTO v_cnt, v_err_name
        FROM rul_version_constant rvc
        JOIN rul_connection_connection rfc
            ON rvc.formula_id = rfc.formula_id
        JOIN rul_connection rc
            ON rc.connection_id = rfc.destination_connection_id
        JOIN rul_charge rch
            ON rch.connection_id = rc.connection_id
            AND rch.start_date <= coalesce(rvc.end_date,'2100-04-30 23:59:59+03')
            AND rch.end_date >= rvc.start_date
        WHERE rvc.version_constant_id = ANY (p_version_constant_ids);
        IF v_cnt != 0 THEN
            RAISE EXCEPTION '%', get_message('ERR_DELETE_VERSION_CONSTANT',v_err_name);
        ELSE
          DELETE FROM rul_constant_value WHERE version_constant_id = ANY (p_version_constant_ids);
          DELETE FROM rul_version_constant WHERE version_constant_id = ANY (p_version_constant_ids);
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.delete_version_load_standard(IN p_version_load_standard_ids bigint[])
CREATE OR REPLACE PROCEDURE public.delete_version_load_standard(IN p_version_load_standard_ids bigint[])
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_cnt bigint:=0;
    v_err_name varchar(1024);
BEGIN
    SELECT COUNT(*), string_agg((select connection_name from rul_connection where connection_id = rch.connection_id)::varchar,', ')
    INTO v_cnt, v_err_name
    FROM rul_charge rch
    JOIN rul_consumption_load rcl
    ON rch.connection_id = rcl.connection_id
    AND rch.billing_start_date <= rcl.start_date
    AND rch.billing_end_date >= rcl.end_date
    WHERE source_id = 1::BIGINT
    AND rcl.version_load_standard_id = ANY (p_version_load_standard_ids);
    IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_LOAD_VALUE',v_err_name);
    ELSE
    	SELECT COUNT(*), string_agg((select connection_name from rul_connection where connection_id = rch.connection_id)::varchar,', ')
        INTO v_cnt, v_err_name
        FROM rul_charge rch
        JOIN rul_consumption_standard rcl
        ON rch.connection_id = rcl.connection_id
        AND rch.billing_start_date <= rcl.start_date
        AND rch.billing_end_date >= rcl.end_date
        WHERE source_id = 1::BIGINT
        AND rcl.version_load_standard_id = ANY (p_version_load_standard_ids);
    	IF v_cnt != 0 THEN
    	RAISE EXCEPTION '%', get_message('ERR_DELETE_STANDARD_VALUE',v_err_name);
        ELSE
        	DELETE FROM rul_version_specific_load WHERE version_load_standard_id = ANY (p_version_load_standard_ids);
        	DELETE FROM rul_load_standard_value WHERE version_load_standard_id = ANY (p_version_load_standard_ids);
            DELETE FROM rul_version_load_standard WHERE version_load_standard_id = ANY (p_version_load_standard_ids);
        END IF;
	END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_charges_losses(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.new_process_charges_losses(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO
      public.rul_charge
    (
      connection_id,
      sum_consumption,
      amount,
      nds_percent,
      note,
      start_date,
      end_date,
      base_value,
      billing_start_date,
      billing_end_date,
      charge_type_id,
      nds_rub,
      amount_nds,
      cost_factor,
      currency_rate,
      comitet_resolution,
      source_id,
      invoice_group_index
    )
    SELECT
        connection_id,
        ROUND(consumption,3),
        ROUND(ROUND(consumption,3) * rrv.base_value,2),
        rrv.nds,
        note,
        rcd.start_date,
        rcd.end_date,
        rrv.base_value,
        p_start_date,
        p_end_date,
        1,
        ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
        ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
        rrv.cost_factor,
        rrv.currency_rate,
        rrv.comitet_resolution,
        2,
        rcd.invoice_group_index
    FROM (
          select
              connection_id,
              sum(consumption) as consumption,
              string_agg(note, ';') as note,
              rate_value_id,
              min(start_date) as start_date,
              max(end_date) as end_date,
              invoice_group_index
          from (
                WITH calc_charges AS (
                SELECT
                    rc.connection_id,
                    SUM(rcc.value*rcc.coefficient) AS consumption,
                    MIN(rcc.start_date) AS start_date,
                    MAX(rcc.end_date) AS end_date,
                    rc.rate_id,
                    string_agg(rcc.note, E'\r\n' order by rcc.start_date) as note
                    --,rcc.accounting_type_node_id
                    ,rc.invoice_group_index
                FROM rul_consumption_losses rcc
                JOIN rul_connection rc
                    ON rc.connection_id = rcc.connection_id
                    AND rc.connection_id IN (select connection_id FROM rul_connection WHERE connection_id = ANY(p_connection_ids)
                    						 AND invoice_group_index IS NOT NULL)
                WHERE rcc.theoretical_calculation is false -- Надо поставить false, пока заглушка
                and rcc.start_date >= p_start_date
                and rcc.end_date <= p_end_date
                GROUP BY rc.connection_id, rc.rate_id, rc.invoice_group_index
                ),
                time_calcs AS (
                    SELECT
                        charges.connection_id,
                        charges.consumption,
                        rrv.start_date AS rate_start,
                        rrv.end_date AS rate_end,
                        LEAST(COALESCE(rrv.end_date, charges.end_date),
                                     charges.end_date) AS period_end,
                        GREATEST(rrv.start_date, charges.start_date) AS period_start,
                        EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                        rrv.rate_value_id, -- Добавляем rate_value_id сюда
                        charges.note,
                        charges.invoice_group_index
                    FROM calc_charges charges
                    JOIN rul_rate_value rrv
                        ON charges.rate_id = rrv.rate_id
                        AND charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                        AND charges.end_date >= rrv.start_date
                )
                SELECT
                    --t.accounting_type_node_id,
                    t.period_start as start_date,
                    t.period_end as end_date,
                    ROUND(CASE
                        WHEN t.total_days = 0 THEN 0
                        ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                    END,3) AS consumption,
                    t.rate_value_id,
                    t.connection_id,
                    t.note,
                    t.invoice_group_index
                FROM time_calcs t
                ) as rul_charge_detail
        where 1=1
        and start_date >= p_start_date
        and end_date <= p_end_date
        AND connection_id IN (select connection_id FROM rul_connection WHERE connection_id = ANY(p_connection_ids))
        group by connection_id,rate_value_id,invoice_group_index
        ) rcd
    JOIN
        rul_rate_value rrv
    ON
        rcd.rate_value_id = rrv.rate_value_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_charges_new(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.new_process_charges_new(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_connection_ids BIGINT[];
BEGIN
    /*DELETE FROM rul_charge WHERE charge_type_id != 2 AND start_date >= p_start_date AND end_date <= p_end_date
      AND connection_id = ANY(p_connection_ids)
      AND charge_checked = 0;*/
	SELECT array_agg(connection_id) INTO v_connection_ids FROM (
    SELECT DISTINCT connection_id FROM rul_connection WHERE connection_id = ANY(p_connection_ids)
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE charge_checked = 1
      								AND billing_start_date >= p_start_date
									AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL) conn;
	-- Собирает начисления по группе таблиц, а не по каждой отдельно
    INSERT INTO
      public.rul_charge
    (
      connection_id,
      sum_consumption,
      amount,
      nds_percent,
      note,
      start_date,
      end_date,
      base_value,
      billing_start_date,
      billing_end_date,
      charge_type_id,
      nds_rub,
      amount_nds,
      cost_factor,
      currency_rate,
      comitet_resolution,
      invoice_group_index
    )
    SELECT
        connection_id,
        ROUND(consumption::numeric,3),
        ROUND((ROUND(consumption::numeric,3) * rrv.base_value)::numeric,2),
        rrv.nds,
        note,
        rcd.start_date,
        rcd.end_date,
        rrv.base_value,
        p_start_date,
        p_end_date,
        1,
        ROUND((ROUND((consumption * rrv.base_value)::numeric ,2) * rrv.nds / 100)::numeric,2),
        ROUND((consumption * rrv.base_value)::numeric,2) + ROUND((ROUND((consumption * rrv.base_value)::numeric,2) * rrv.nds::numeric / 100)::numeric,2),
        rrv.cost_factor,
        rrv.currency_rate,
        rrv.comitet_resolution,
        rcd.invoice_group_index
    FROM (
          select
              connection_id,
              sum(consumption) as consumption,
              string_agg(coalesce(note,' '), ';') as note,
              rate_value_id,
              min(start_date) as start_date,
              max(end_date) as end_date,
              invoice_group_index
          from (
                WITH calc_charges AS (
                SELECT
                    rc.connection_id,
                    SUM(rcc.value * rcc.coefficient) AS consumption,
                    MIN(rcc.start_date) AS start_date,
                    MAX(rcc.end_date) AS end_date,
                    rc.rate_id,
                    rcc.accounting_type_node_id,
                    string_agg(rcc.note, E'\r\n' order by rcc.start_date) as note,
                    rc.invoice_group_index
                FROM (
                      SELECT rcl.connection_id,rcl.value,rcl.coefficient,rcl.start_date,rcl.end_date,
                        rcl.accounting_type_node_id,rcl.note,rcl.theoretical_calculation
                      FROM rul_consumption_load rcl
                      UNION ALL
                      SELECT rcs.connection_id,rcs.value,rcs.coefficient,rcs.start_date,rcs.end_date,
                              rcs.accounting_type_node_id,rcs.note,rcs.theoretical_calculation
                      FROM rul_consumption_standard rcs
                      UNION ALL
                      SELECT rcsс.connection_id,rcsс.value,rcsс.coefficient,rcsс.start_date,rcsс.end_date,
                              rcsс.accounting_type_node_id,rcsс.note,rcsс.theoretical_calculation
                      FROM rul_consumption_source_connection rcsс
                      ) rcc
                JOIN rul_connection rc
                    ON rc.connection_id = rcc.connection_id
                    --AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
                    AND rc.connection_id = ANY (v_connection_ids)
                WHERE rcc.theoretical_calculation is false -- Надо поставить false, пока заглушка
                and rcc.start_date >= p_start_date
                and rcc.end_date <= p_end_date
                GROUP BY rc.connection_id, rc.rate_id, rcc.accounting_type_node_id, rc.invoice_group_index
                ),
                time_calcs AS (
                    SELECT
                        charges.connection_id,
                        charges.consumption,
                        rrv.start_date AS rate_start,
                        rrv.end_date AS rate_end,
                        LEAST(COALESCE(rrv.end_date, charges.end_date),charges.end_date) AS period_end,
                        GREATEST(rrv.start_date, charges.start_date) AS period_start,
                        EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                        charges.accounting_type_node_id,
                        rrv.rate_value_id, -- Добавляем rate_value_id сюда
                        charges.note,
                        charges.invoice_group_index
                    FROM calc_charges charges
                    JOIN rul_rate_value rrv
                        ON charges.rate_id = rrv.rate_id
                        AND charges.start_date <= COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                        AND COALESCE(charges.end_date, '2100-01-01 00:00:00+03'::timestamp) > rrv.start_date
                )
                SELECT
                    t.accounting_type_node_id,
                    t.period_start as start_date,
                    t.period_end as end_date,
                    ROUND(CASE
                        WHEN t.total_days = 0 THEN t.consumption
                        ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                    END,3) AS consumption,
                    t.rate_value_id,
                    t.connection_id,
                    t.note,
                    t.invoice_group_index
                FROM time_calcs t
                ) as rul_charge_detail
        WHERE 1=1
        AND start_date >= p_start_date
        AND end_date <= p_end_date
        --AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
        AND connection_id = ANY (v_connection_ids)
        GROUP BY connection_id,rate_value_id,accounting_type_node_id,invoice_group_index
        ) rcd
    JOIN
        rul_rate_value rrv
    ON
        rcd.rate_value_id = rrv.rate_value_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_charges_planned_consumption(IN p_agreement_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.new_process_charges_planned_consumption(IN p_agreement_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    DELETE FROM rul_charge WHERE start_date >= p_start_date
                            AND end_date <= p_end_date
                            AND connection_id IN (SELECT connection_id FROM rul_connection WHERE agreement_id = ANY(p_agreement_ids)
                                    AND invoice_group_index IS NOT NULL)
                            AND source_id = 4;
        INSERT INTO
              public.rul_charge
            (
              connection_id,
              sum_consumption,
              amount,
              nds_percent,
              note,
              start_date,
              end_date,
              base_value,
              billing_start_date,
              billing_end_date,
              charge_type_id,
              nds_rub,
              amount_nds,
              cost_factor,
        	  currency_rate,
              comitet_resolution,
              source_id,
              invoice_group_index
            )
        SELECT
            connection_id,
            ROUND(consumption,3),
            ROUND(ROUND(consumption,3) * rrv.base_value,2),
            rrv.nds,
            note,
            rcd.end_date, -- Делаем, чтобы день начала и конца совпадал
            rcd.end_date,
            rrv.base_value,
            p_start_date,
            p_end_date,
            1,
            ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            rrv.cost_factor,
        	rrv.currency_rate,
            rrv.comitet_resolution,
            4,
            invoice_group_index
        FROM (
              select
                  connection_id,
                  sum(consumption) as consumption,
                  string_agg(note, ' - ') as note,
                  rate_value_id,
                  min(start_date) as start_date,
                  max(end_date) as end_date,
                  invoice_group_index
              from (
                  	WITH calc_charges AS (
                    SELECT
                        rc.connection_id,
                        SUM(rcc.planned_consumption_value * rcc.advance_payment_percent / 100) AS consumption,
                        GREATEST(MAX(rcc.start_date),MAX(ratn.start_date)) AS start_date,
                        MIN(rcc.payment_date) AS end_date,
                        rc.rate_id,
                        ratn.accounting_type_node_id,
                        string_agg(rcc.description, ' - ') as description,
                        rcc.planned_consumption_id,
                        rc.invoice_group_index
                    FROM rul_planned_consumption rcc
                    JOIN rul_connection rc
                        ON rc.connection_id = rcc.connection_id
                        AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = ANY(p_agreement_ids)
                        						 AND invoice_group_index IS NOT NULL)
                    JOIN rul_accounting_type_node ratn
                    	ON rc.node_calculate_parameter_id = ratn.node_calculate_parameter_id
                        AND ratn.start_date <= rcc.payment_date
                        AND COALESCE(ratn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= rcc.payment_date
                    LEFT JOIN rul_charge rch
                    	ON rch.connection_id = rc.connection_id
                        AND rch.start_date = rcc.payment_date
                    WHERE 1=1 -- Надо поставить false, пока заглушка
                    and rcc.start_date >= p_start_date
                    and rcc.end_date <= p_end_date
                    and rch.charge_id is null
                    GROUP BY rc.connection_id, rc.rate_id, ratn.accounting_type_node_id, rcc.planned_consumption_id, rc.invoice_group_index
                    ),
                    time_calcs AS (
                        SELECT
                            charges.connection_id,
                            charges.consumption,
                            rrv.start_date AS rate_start,
                            rrv.end_date AS rate_end,
                            LEAST(COALESCE(rrv.end_date, charges.end_date),
                                         charges.end_date) AS period_end,
                            GREATEST(rrv.start_date, charges.start_date) AS period_start,
                            EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                            charges.accounting_type_node_id,
                            rrv.rate_value_id, -- Добавляем rate_value_id сюда
                            charges.description,
                            charges.planned_consumption_id,
                            charges.invoice_group_index
                        FROM calc_charges charges
                        JOIN rul_rate_value rrv
                            ON charges.rate_id = rrv.rate_id
                            AND ((charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                            AND charges.end_date > rrv.start_date) OR
                             (rrv.start_date BETWEEN charges.start_date AND charges.end_date
                                OR COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                                    BETWEEN charges.start_date AND charges.end_date))
                    )
                    SELECT
                        t.accounting_type_node_id,
                        t.period_start as start_date,
                        t.period_end as end_date,
                        ROUND(CASE
                            WHEN t.total_days = 0 THEN 0
                            ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                        END,3) AS consumption,
                        t.rate_value_id,
                        t.connection_id,
                        t.description as note,
                        t.planned_consumption_id,
                        t.invoice_group_index
                    FROM time_calcs t
                    ) as rul_charge_detail
            where 1=1
            and start_date >= p_start_date
            and end_date <= p_end_date
            AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = ANY(p_agreement_ids))
            group by connection_id,rate_value_id,accounting_type_node_id,planned_consumption_id,invoice_group_index
            ) rcd
        JOIN
            rul_rate_value rrv
        ON
            rcd.rate_value_id = rrv.rate_value_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_formuls_load(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.new_process_formuls_load(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
i record;
v_connection_ids BIGINT[];
v_count BIGINT;
v_locality_id BIGINT;
BEGIN
	-- Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    -- В этом списке будут подключения по договору, которые не подтверждены (т.е. у них на них не сформирован счет)
    SELECT array_agg(connection_id) INTO v_connection_ids FROM (
    SELECT DISTINCT connection_id FROM rul_connection WHERE connection_id = ANY(p_connection_ids)
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date
                                )
      AND invoice_group_index IS NOT NULL) conn;
    DELETE FROM rul_consumption_load
    -- Дописываем, чтобы подтвержденные начисления не удалялись при пересчете
    WHERE connection_id = ANY (v_connection_ids)
    --WHERE connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
    --AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL)
    AND start_date >= p_start_date and end_date <= p_end_date;
    -- Расчет формулы 26 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'51'->>'value')::NUMERIC * (details->'52'->>'value')::NUMERIC  / 100 *
        (extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1) AS value,
        conn2.accounting_type_node_id,
        (details->'51'->>'code')::varchar||': '||(details->'51'->>'value')::varchar||' '||(details->'51'->>'unit')::varchar||', '||
        (details->'52'->>'code')::varchar||': '||(details->'52'->>'value')::varchar||' '||(details->'52'->>'unit')::varchar||', '||
        (details->'53'->>'code')::varchar||': '||(extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)||' '||(details->'53'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(26::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 27 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'54'->>'value')::NUMERIC * (details->'55'->>'value')::NUMERIC / 100 * (extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1) AS value,
        conn2.accounting_type_node_id,
        (details->'54'->>'code')::varchar||': '||(details->'54'->>'value')::varchar||' '||(details->'54'->>'unit')::varchar||', '||
        (details->'55'->>'code')::varchar||': '||(details->'55'->>'value')::varchar||' '||(details->'55'->>'unit')::varchar||', '||
        (details->'56'->>'code')::varchar||': '||(extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)||' '||(details->'56'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(27::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    AND load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date;
    --Проверка на то, заведены ли температуры для подключений, которые считаются по определенной формуле формуле
    --Выдаем ошибку. если не хватает
    --Можно скорее всего добавить проверку по всем формулам сразу, в которых участвует температура.
    FOR i IN
    	SELECT rc.connection_id, rc.connection_name
        FROM rul_connection rc
    	JOIN rul_formula_connection rfc
        	ON rc.connection_id = rfc.connection_id
        WHERE rfc.formula_id = 28
        AND rc.connection_id = ANY(v_connection_ids)
    LOOP
    	select count(*),obs.locality_id into v_count, v_locality_id
        from rul_observation obs
        join rul_object obj on obs.locality_id = obj.locality_id
        join rul_node n on n.object_id = obj.object_id
        join rul_node_calculate_parameter ncp on ncp.node_id = n.node_id
        where observation_type_id = 1 --Воздух
        and observation_period_id = 1 --Среднесуточная
        and node_calculate_parameter_id = (SELECT node_calculate_parameter_id FROM rul_connection WHERE connection_id = i.connection_id)
        and observation_date >= p_start_date
        and observation_date <= p_end_date
        group by obs.locality_id;
      IF coalesce(v_count,0) != extract('day' from p_end_date - p_start_date) + 1
      THEN
      RAISE EXCEPTION '[[Не заведены температуры для: %]]', (select rl.locality_name
                                                              from rul_locality rl
                                                              join rul_object obj on rl.locality_id = obj.locality_id
                                                              join rul_node n on n.object_id = obj.object_id
                                                              join rul_node_calculate_parameter ncp on ncp.node_id = n.node_id
                                                              join rul_connection rc on rc.node_calculate_parameter_id = ncp.node_calculate_parameter_id
                                                              WHERE connection_id = i.connection_id)
      USING ERRCODE = '25001';
      END IF;
    END LOOP;
    -- Расчет формулы 28 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'57'->>'value')::NUMERIC * 24 *
        (extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)
        * ((details->'60'->>'value')::NUMERIC
        - get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        )/((details->'60'->>'value')::NUMERIC - (details->'62'->>'value')::NUMERIC) AS value,
        conn2.accounting_type_node_id,
        (details->'57'->>'code')::varchar||': '||(details->'57'->>'value')::varchar||' '||(details->'57'->>'unit')::varchar||', '||
        (details->'59'->>'code')::varchar||': '||(extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)||' '||(details->'59'->>'unit')::varchar||', '||
        (details->'60'->>'code')::varchar||': '||(details->'60'->>'value')::varchar||' '||(details->'60'->>'unit')::varchar||', '||
        (details->'61'->>'code')::varchar||': '||
        get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        ||' '||(details->'61'->>'unit')::varchar||', '||
        (details->'62'->>'code')::varchar||': '||(details->'62'->>'value')::varchar||' '||(details->'62'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(28::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 70 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'172'->>'value')::NUMERIC *
        ( (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'183'->>'value')::NUMERIC
        	+
          (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'184'->>'value')::NUMERIC
        )
        * (((details->'174'->>'value')::NUMERIC -
        	get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        )/((details->'174'->>'value')::NUMERIC - (details->'177'->>'value')::NUMERIC)) AS value,
        conn2.accounting_type_node_id,
        (details->'172'->>'code')::varchar||': '||(details->'172'->>'value')::varchar||' '||(details->'172'->>'unit')::varchar||', '||
        (details->'173'->>'code')::varchar||': '||((count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'183'->>'value')::NUMERIC
        	+ (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'184'->>'value')::NUMERIC
        )||' '||(details->'173'->>'unit')::varchar||', '||
        (details->'174'->>'code')::varchar||': '||(details->'174'->>'value')::varchar||' '||(details->'174'->>'unit')::varchar||', '||
        (details->'176'->>'code')::varchar||': '||
        get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        ||' '||(details->'176'->>'unit')::varchar||', '||
        (details->'177'->>'code')::varchar||': '||(details->'177'->>'value')::varchar||' '||(details->'177'->>'unit')::varchar||', '||
        (details->'183'->>'code')::varchar||': '||(details->'183'->>'value')::varchar||' '||(details->'183'->>'unit')::varchar||', '||
        (details->'184'->>'code')::varchar||': '||(details->'184'->>'value')::varchar||' '||(details->'184'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(70::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 71 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'178'->>'value')::NUMERIC *
        ( (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'189'->>'value')::NUMERIC
        	+
          (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'190'->>'value')::NUMERIC
        )
        / (details->'187'->>'value')::NUMERIC
         AS value,
        conn2.accounting_type_node_id,
        (details->'178'->>'code')::varchar||': '||(details->'178'->>'value')::varchar||' '||(details->'178'->>'unit')::varchar||', '||
        (details->'179'->>'code')::varchar||': '||((count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'189'->>'value')::NUMERIC
        	+ (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'190'->>'value')::NUMERIC
        )||' '||(details->'179'->>'unit')::varchar||', '||
        (details->'187'->>'code')::varchar||': '||(details->'187'->>'value')::varchar||' '||(details->'187'->>'unit')::varchar||', '||
        (details->'189'->>'code')::varchar||': '||(details->'189'->>'value')::varchar||' '||(details->'189'->>'unit')::varchar||', '||
        (details->'190'->>'code')::varchar||': '||(details->'190'->>'value')::varchar||' '||(details->'190'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(71::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 98 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'276'->>'value')::NUMERIC
        * (extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)
        * 10 *
        (
        (details->'273'->>'value')::NUMERIC * coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,1,2)),0) --Жидкие
        +
        (details->'275'->>'value')::NUMERIC * coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,2,2)),0) --Твердые
        )
        / (extract (day from p_end_date - p_start_date) + 1)
        AS value,
        conn2.accounting_type_node_id,
        (details->'272'->>'code')::varchar||': '||coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,1,2)),0)||' '||(details->'272'->>'unit')::varchar||', '||
        (details->'273'->>'code')::varchar||': '||(details->'273'->>'value')::varchar||' '||(details->'273'->>'unit')::varchar||', '||
        (details->'274'->>'code')::varchar||': '||coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,2,2)),0)||' '||(details->'274'->>'unit')::varchar||', '||
        (details->'275'->>'code')::varchar||': '||(details->'275'->>'value')::varchar||' '||(details->'275'->>'unit')::varchar||', '||
        (details->'276'->>'code')::varchar||': '||(details->'276'->>'value')::varchar||' '||(details->'276'->>'unit')::varchar||', '||
        (details->'278'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'278'->>'unit')::varchar||', '||
        (details->'279'->>'code')::varchar||': '||(extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)||' '||(details->'279'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(98::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
     -- Расчет формулы 99 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        ((details->'280'->>'value')::NUMERIC * (extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)
        * 10 * (details->'281'->>'value')::NUMERIC * (details->'282'->>'value')::NUMERIC * (details->'283'->>'value')::NUMERIC)
        /
        (extract (day from p_end_date - p_start_date) + 1) AS value,
        conn2.accounting_type_node_id,
        (details->'280'->>'code')::varchar||': '||(details->'280'->>'value')::varchar||' '||(details->'280'->>'unit')::varchar||', '||
        (details->'281'->>'code')::varchar||': '||(details->'281'->>'value')::varchar||' '||(details->'281'->>'unit')::varchar||', '||
        (details->'282'->>'code')::varchar||': '||(details->'282'->>'value')::varchar||' '||(details->'282'->>'unit')::varchar||', '||
        (details->'283'->>'code')::varchar||': '||(details->'283'->>'value')::varchar||' '||(details->'283'->>'unit')::varchar||', '||
        (details->'285'->>'code')::varchar||': '||(extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)||' '||(details->'285'->>'unit')::varchar||', '||
        (details->'286'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'286'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(99::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 100 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        1.5 * (details->'288'->>'value')::NUMERIC * (details->'289'->>'value')::NUMERIC * (details->'290'->>'value')::NUMERIC
        AS value,
        conn2.accounting_type_node_id,
        (details->'288'->>'code')::varchar||': '||(details->'288'->>'value')::varchar||' '||(details->'288'->>'unit')::varchar||', '||
        (details->'289'->>'code')::varchar||': '||(details->'289'->>'value')::varchar||' '||(details->'289'->>'unit')::varchar||', '||
        (details->'290'->>'code')::varchar||': '||(details->'290'->>'value')::varchar||' '||(details->'290'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(100::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 154 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'567'->>'value')::NUMERIC *
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'572'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'573'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'574'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'575'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'576'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'577'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'578'->>'value')::NUMERIC
                      ELSE 1 END)
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'579'->>'value')::SMALLINT)
        )
        * (((details->'569'->>'value')::NUMERIC -
        	get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        )/((details->'569'->>'value')::NUMERIC - (details->'571'->>'value')::NUMERIC)) AS value,
        conn2.accounting_type_node_id,
        (details->'567'->>'code')::varchar||': '||(details->'567'->>'value')::varchar||' '||(details->'567'->>'unit')::varchar||', '||
        (details->'568'->>'code')::varchar||': '||
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'572'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'573'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'574'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'575'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'576'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'577'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'578'->>'value')::NUMERIC
                      ELSE 1 END)
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'579'->>'value')::SMALLINT)
        )::varchar||' '||(details->'568'->>'unit')::varchar||', '||
        (details->'569'->>'code')::varchar||': '||(details->'569'->>'value')::varchar||' '||(details->'569'->>'unit')::varchar||', '||
        (details->'570'->>'code')::varchar||': '||
        get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        ||' '||(details->'570'->>'unit')::varchar||', '||
        (details->'571'->>'code')::varchar||': '||(details->'571'->>'value')::varchar||' '||(details->'571'->>'unit')::varchar||', '||
        (details->'572'->>'code')::varchar||': '||(details->'572'->>'value')::varchar||' '||(details->'572'->>'unit')::varchar||', '||
        (details->'573'->>'code')::varchar||': '||(details->'573'->>'value')::varchar||' '||(details->'573'->>'unit')::varchar||', '||
        (details->'574'->>'code')::varchar||': '||(details->'574'->>'value')::varchar||' '||(details->'574'->>'unit')::varchar||', '||
        (details->'575'->>'code')::varchar||': '||(details->'575'->>'value')::varchar||' '||(details->'575'->>'unit')::varchar||', '||
        (details->'576'->>'code')::varchar||': '||(details->'576'->>'value')::varchar||' '||(details->'576'->>'unit')::varchar||', '||
        (details->'577'->>'code')::varchar||': '||(details->'577'->>'value')::varchar||' '||(details->'577'->>'unit')::varchar||', '||
        (details->'578'->>'code')::varchar||': '||(details->'578'->>'value')::varchar||' '||(details->'578'->>'unit')::varchar||', '||
        (details->'579'->>'code')::varchar||': '||(details->'579'->>'value')::varchar||' '||(details->'579'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(154::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 155 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'556'->>'value')::NUMERIC *
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'559'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'560'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'561'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'562'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'563'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'564'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'565'->>'value')::NUMERIC
                      ELSE 1 END) :: NUMERIC
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'566'->>'value')::SMALLINT)
        )
        /
        (details->'558'->>'value')::NUMERIC
         AS value,
        conn2.accounting_type_node_id,
        (details->'556'->>'code')::varchar||': '||(details->'556'->>'value')::varchar||' '||(details->'556'->>'unit')::varchar||', '||
        (details->'557'->>'code')::varchar||': '||
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'559'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'560'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'561'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'562'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'563'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'564'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'565'->>'value')::NUMERIC
                      ELSE 1 END) :: NUMERIC
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'566'->>'value')::SMALLINT)
        )::varchar||' '||(details->'557'->>'unit')::varchar||', '||
        (details->'558'->>'code')::varchar||': '||(details->'558'->>'value')::varchar||' '||(details->'558'->>'unit')::varchar||', '||
        (details->'559'->>'code')::varchar||': '||(details->'559'->>'value')::varchar||' '||(details->'559'->>'unit')::varchar||', '||
        (details->'560'->>'code')::varchar||': '||(details->'560'->>'value')::varchar||' '||(details->'560'->>'unit')::varchar||', '||
        (details->'561'->>'code')::varchar||': '||(details->'561'->>'value')::varchar||' '||(details->'561'->>'unit')::varchar||', '||
        (details->'562'->>'code')::varchar||': '||(details->'562'->>'value')::varchar||' '||(details->'562'->>'unit')::varchar||', '||
        (details->'563'->>'code')::varchar||': '||(details->'563'->>'value')::varchar||' '||(details->'563'->>'unit')::varchar||', '||
        (details->'564'->>'code')::varchar||': '||(details->'564'->>'value')::varchar||' '||(details->'564'->>'unit')::varchar||', '||
        (details->'565'->>'code')::varchar||': '||(details->'565'->>'value')::varchar||' '||(details->'565'->>'unit')::varchar||', '||
        (details->'566'->>'code')::varchar||': '||(details->'566'->>'value')::varchar||' '||(details->'566'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(155::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_formuls_pipe(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.new_process_formuls_pipe(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	-- Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    -- В этом списке будут подключения по договору, которые не подтверждены (т.е. у них на них не сформирован счет)
    CREATE TEMP TABLE temp_connections AS
    SELECT DISTINCT connection_id FROM rul_connection WHERE connection_id = ANY(p_connection_ids)
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL;
    DELETE FROM rul_consumption_pipe
    WHERE connection_id IN (SELECT connection_id FROM temp_connections)
    --WHERE connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
    AND start_date >= p_start_date and end_date <= p_end_date;
    -- Расчет формулы 62 по сечению
    INSERT INTO rul_consumption_pipe
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        conn2.start_date,
        conn2.end_date,
        3.1415 * pipe1.value ^ 2 / 4 * 2.0 * 0.000001 * 86400 * (extract (day from (conn2.end_date - conn2.start_date)) + 1) AS value,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id
    FROM (
    SELECT
        conn.connection_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone),p_end_date) AS end_date,
            c.node_calculate_parameter_id
        FROM rul_connection c
        WHERE
        	--Проверяем действует ли подключение в переданном расчетном периоде
            c.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (c.start_date < p_start_date AND COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id
        FROM rul_accounting_type_node atn
        WHERE
        	--Проверяем действует ли способ учета в переданном расчетном периоде
            atn.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (atn.start_date < p_start_date AND COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    and (acc.start_date >= conn.start_date AND acc.start_date <= conn.end_date
            OR acc.end_date > conn.start_date AND acc.end_date <= conn.end_date
            OR (acc.start_date < conn.start_date AND acc.end_date > conn.end_date))
    ) conn2
    JOIN (
        SELECT
            pv.accounting_type_node_id,
            af.formula_id,
            pv.value
        FROM rul_pipe_value pv
        JOIN rul_argument_formula af
            ON pv.argument_formula_id = af.argument_formula_id
        WHERE af.formula_id = 62
        ) pipe1
    ON pipe1.accounting_type_node_id = conn2.accounting_type_node_id
    ;
    -- Расчет формулы 60 по сечению
    INSERT INTO rul_consumption_pipe
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        conn2.start_date,
        conn2.end_date,
        3.1415 * pipe1.value ^ 2 / 4 * 2.0 * 0.000001 * 86400 * (extract (day from (conn2.end_date - conn2.start_date)) + 1) AS value,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id
    FROM (
    SELECT
        conn.connection_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone),p_end_date) AS end_date,
            c.node_calculate_parameter_id
        FROM rul_connection c
        WHERE
        	--Проверяем действует ли подключение в переданном расчетном периоде
            c.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (c.start_date < p_start_date AND COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id
        FROM rul_accounting_type_node atn
        WHERE
        	--Проверяем действует ли способ учета в переданном расчетном периоде
            atn.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (atn.start_date < p_start_date AND COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    and (acc.start_date >= conn.start_date AND acc.start_date <= conn.end_date
            OR acc.end_date > conn.start_date AND acc.end_date <= conn.end_date
            OR (acc.start_date < conn.start_date AND acc.end_date > conn.end_date))
    ) conn2
    JOIN (
        SELECT
            pv.accounting_type_node_id,
            af.formula_id,
            pv.value
        FROM rul_pipe_value pv
        JOIN rul_argument_formula af
            ON pv.argument_formula_id = af.argument_formula_id
        WHERE af.formula_id = 60
        ) pipe1
    ON pipe1.accounting_type_node_id = conn2.accounting_type_node_id
    ;
    -- Расчет формулы 63 по сечению
    INSERT INTO rul_consumption_pipe
    SELECT
       conn2.connection_id,
       conn2.connection_name,
        conn2.start_date,
        conn2.end_date,
        3.1415 * pipe1.value ^ 2 / 4 * 1.5 * 0.000001 * 86400 * (extract (day from (conn2.end_date - conn2.start_date)) + 1) AS value,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id
    FROM (
    SELECT
        conn.connection_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone),p_end_date) AS end_date,
            c.node_calculate_parameter_id
        FROM rul_connection c
        WHERE
        	--Проверяем действует ли подключение в переданном расчетном периоде
            c.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (c.start_date < p_start_date AND COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id
        FROM rul_accounting_type_node atn
        WHERE
        	--Проверяем действует ли способ учета в переданном расчетном периоде
            atn.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (atn.start_date < p_start_date AND COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    and (acc.start_date >= conn.start_date AND acc.start_date <= conn.end_date
            OR acc.end_date > conn.start_date AND acc.end_date <= conn.end_date
            OR (acc.start_date < conn.start_date AND acc.end_date > conn.end_date))
    ) conn2
    JOIN (
        SELECT
            pv.accounting_type_node_id,
            af.formula_id,
            pv.value
        FROM rul_pipe_value pv
        JOIN rul_argument_formula af
            ON pv.argument_formula_id = af.argument_formula_id
        WHERE af.formula_id = 63
        ) pipe1
    ON pipe1.accounting_type_node_id = conn2.accounting_type_node_id
    ;
    DROP TABLE temp_connections;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_formuls_standard(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.new_process_formuls_standard(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_connection_ids BIGINT[];
BEGIN
	-- Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    -- В этом списке будут подключения по договору, которые не подтверждены (т.е. у них на них не сформирован счет)
    SELECT array_agg(connection_id) INTO v_connection_ids FROM (
    SELECT DISTINCT connection_id FROM rul_connection WHERE connection_id = ANY(p_connection_ids)
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL) conn;
    DELETE FROM rul_consumption_standard
    WHERE connection_id = ANY (v_connection_ids)
    --WHERE connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
    AND start_date >= p_start_date and end_date <= p_end_date;
    -- Расчет формулы 61 по нагрузке, надо переделать на расчет по методу, а не формуле.
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        (details->'K'->>'value')::NUMERIC * standard.value * (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1) AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'K'->>'code')::varchar||': '||(details->'K'->>'value')::varchar||' '||(details->'K'->>'unit')::varchar||', '||
        (details->'Vуд'->>'code')::varchar||': '||standard.value||' '||(details->'Vуд'->>'unit')::varchar||', '||
        (details->'Д<авто>'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Д<авто>'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,1::BIGINT) load1
    ON load1.connection_id = conn2.connection_id
      AND load1.start_date <= conn2.end_date
      AND load1.end_date >= conn2.start_date
    ) conn3
    join
    (select value, vc.formula_id,
    GREATEST(vc.start_date, p_start_date) as start_date,
    LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date) as end_date
    from rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 1
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    ) standard
    ON standard.start_date <= conn3.end_date
    	AND standard.end_date >= conn3.start_date
    	AND standard.formula_id = conn3.formula_id
    ;
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        standard.value_148 * (details->'F'->>'value')::NUMERIC * (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)
        * ((standard.value_150 -
        get_temperature(GREATEST(conn3.start_date, standard.start_date),LEAST(conn3.end_date, standard.end_date),conn3.node_calculate_parameter_id,1,1)
        )/(standard.value_150 - standard.value_152))
         AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'F'->>'code')::varchar||': '||(details->'F'->>'value')::varchar||' '||(details->'F'->>'unit')::varchar||', '||
        (details->'Qуд'->>'code')::varchar||': '||standard.value_148||' '||(details->'Qуд'->>'unit')::varchar||', '||
        (details->'tвн'->>'code')::varchar||': '||standard.value_150||' '||(details->'tвн'->>'unit')::varchar||', '||
        (details->'tнар.баз'->>'code')::varchar||': '||standard.value_152||' '||(details->'tнар.баз'->>'unit')::varchar||', '||
        (details->'tнар.ф<авто>'->>'code')::varchar||': '||get_temperature(GREATEST(conn3.start_date, standard.start_date),LEAST(conn3.end_date, standard.end_date),conn3.node_calculate_parameter_id,1,1)||' '||(details->'tнар.ф<авто>'->>'unit')::varchar||', '||
        (details->'Дфакт<авто>'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Дфакт<авто>'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,2::BIGINT) load1
    ON load1.connection_id = conn2.connection_id
    	AND load1.start_date <= conn2.end_date
    	AND load1.end_date >= conn2.start_date
    ) conn3
    JOIN
    (
    SELECT vc.formula_id,
        MAX(CASE WHEN af.argument_formula_code = 'Qуд' THEN cv.value END) AS value_148,
        MAX(CASE WHEN af.argument_formula_code = 'tвн' THEN cv.value END) AS value_150,
        MAX(CASE WHEN af.argument_formula_code = 'tнар.баз' THEN cv.value END) AS value_152,
        MAX(GREATEST(vc.start_date, p_start_date)) as start_date,
        MAX(LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date)) as end_date
    FROM rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 2
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    GROUP BY vc.formula_id,vc.version_constant_id
    ) standard
    ON  standard.start_date <= conn3.end_date
    	AND standard.end_date >= conn3.start_date
    	AND standard.formula_id = conn3.formula_id
    ;
    -- По методу 4 для формулы шаблона 101
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        standard.value_292 * (details->'Vф'->>'value')::NUMERIC
        *
        (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)
        /
        (extract (day from p_end_date - p_start_date) + 1)
         AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'Vф'->>'code')::varchar||': '||(details->'Vф'->>'value')::varchar||' '||(details->'Vф'->>'unit')::varchar||', '||
        (details->'Дм'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'Дм'->>'unit')::varchar||', '||
        (details->'Др'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Др'->>'unit')::varchar||', '||
        (details->'Qуд'->>'code')::varchar||': '||standard.value_292||' '||(details->'Qуд'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,4::BIGINT) load1
    	ON load1.connection_id = conn2.connection_id
    		AND load1.start_date <= conn2.end_date
    		AND load1.end_date >= conn2.start_date
    ) conn3
    JOIN
    (
    SELECT vc.formula_id,
        MAX(CASE WHEN af.argument_formula_code = 'Qуд' THEN cv.value END) AS value_292,
        MAX(GREATEST(vc.start_date, p_start_date)) as start_date,
        MAX(LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date)) as end_date
    FROM rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 4
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    GROUP BY vc.formula_id,vc.version_constant_id
    ) standard
    ON standard.start_date <= conn3.end_date
    AND standard.end_date >= conn3.start_date
    AND standard.formula_id = conn3.formula_id
    ;
    -- По методу 5 для формулы шаблона 153
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        standard.value_292 * (details->'F'->>'value')::NUMERIC
        *
        (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)
        /
        (extract (day from p_end_date - p_start_date) + 1)
         AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'F'->>'code')::varchar||': '||(details->'F'->>'value')::varchar||' '||(details->'F'->>'unit')::varchar||', '||
        (details->'Дм'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'Дм'->>'unit')::varchar||', '||
        (details->'Др'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Др'->>'unit')::varchar||', '||
        (details->'Qуд'->>'code')::varchar||': '||standard.value_292||' '||(details->'Qуд'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,5::BIGINT) load1
    	ON load1.connection_id = conn2.connection_id
    		AND load1.start_date <= conn2.end_date
    		AND load1.end_date >= conn2.start_date
    ) conn3
    JOIN
    (
    SELECT vc.formula_id,
        MAX(CASE WHEN af.argument_formula_code = 'Qуд' THEN cv.value END) AS value_292,
        MAX(GREATEST(vc.start_date, p_start_date)) as start_date,
        MAX(LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date)) as end_date
    FROM rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 5
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    GROUP BY vc.formula_id,vc.version_constant_id
    ) standard
    ON standard.start_date <= conn3.end_date
    AND standard.end_date >= conn3.start_date
    AND standard.formula_id = conn3.formula_id
    ;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_group_accounting3(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_use_by boolean)
CREATE OR REPLACE PROCEDURE public.new_process_group_accounting3(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_use_by boolean DEFAULT false)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    row_record record;
    row_record2 record;
    coef numeric;
    PY_SUM numeric;
    Losses numeric;
    Charges numeric;
BEGIN
  --Меняем принцип, теперь сначала нужно построить дерево, а потом
    --Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    FOR row_record IN
    SELECT
        acc.start_date AS start_date
        ,acc.end_date AS end_date
        ,acc.accounting_type_node_id as accounting_type_node_id
        ,acc.node_calculate_parameter_id as node_calculate_parameter_id
        ,acc.accounting_type_id as accounting_type_id
    FROM
    (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id,
            atn.accounting_type_id
        FROM rul_accounting_type_node atn
        WHERE atn.start_date <= p_end_date
        	AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
        	AND atn.node_calculate_parameter_id = p_node_calculate_parameter_id
    ) acc
    where
    	(p_use_by IS TRUE AND acc.accounting_type_id in (2,5,17,19))
    		OR
    	(p_use_by IS FALSE AND acc.accounting_type_id in (2,5,19))
    LOOP
        -- Обработка каждой строки
        RAISE NOTICE 'Processing row: node_calculate_parameter_id=%, accounting_type_node_id=%', row_record.node_calculate_parameter_id, row_record.accounting_type_node_id;
       CREATE TEMP TABLE new_temp_results
    	as
        WITH RECURSIVE tree_cte AS (
            -- Базовый случай: выбираем корневые элементы
            SELECT
                zero_level.name as line_name,
                null::bigint as line_id,
                zero_level.node_id AS node_id,
                zero_level.node_id AS child_id,
                0 AS level,
                ARRAY[zero_level.node_id] AS path,
                zero_level.node_id::TEXT AS path_str,
                -- Добавляем даты для корневого элемента
                GREATEST(conn3.start_date, row_record.start_date) AS start_date,
                LEAST(conn3.end_date, row_record.end_date) AS end_date,
                conn3.accounting_type_id as accounting_type_id,
        		conn3.accounting_type_node_id as accounting_type_node_id,
                null::bigint as formula_id
    		FROM (select 'zero_level' as name, row_record.node_calculate_parameter_id as node_id) zero_level
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, row_record.start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                    	atn.start_date < row_record.end_date::timestamp without time zone
        				AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date::timestamp without time zone
                AND ((p_use_by IS TRUE AND atn.accounting_type_id in (2,5,17,19))
                        OR
                      (p_use_by IS FALSE AND atn.accounting_type_id in (2,5,19)))
            ) conn3 ON conn3.node_calculate_parameter_id = zero_level.node_id
            UNION ALL
            -- Рекурсивный случай: присоединяем детей с учетом дат родителя
            SELECT
                rl.line_name,
                rl.line_id,
                rlp.node_calculate_parameter_id AS node_id,
                rlpc.node_calculate_parameter_id AS child_id,
                t.level + 1,
                t.path || rlpc.node_calculate_parameter_id,
                t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                -- Вычисляем даты дочернего элемента на основе родительского
                GREATEST(conn3.start_date, t.start_date) AS start_date,
                LEAST(conn3.end_date, t.end_date) AS end_date,
                conn3.accounting_type_id,
                conn3.accounting_type_node_id as accounting_type_node_id,
                rlp.formula_id
            FROM tree_cte t
            JOIN public.rul_line_parameter rlp
                ON t.child_id = rlp.node_calculate_parameter_id
            JOIN public.rul_line_parameter_child rlpc
                ON rlpc.line_parameter_id = rlp.line_parameter_id
            JOIN public.rul_line rl
                ON rl.line_id = rlp.line_id
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, row_record.start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                        atn.start_date < row_record.end_date
                        AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
            ) conn3 ON conn3.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
            WHERE rl.client_id IS NOT NULL
              -- Ограничиваем даты дочерних элементов датами родителя
              AND conn3.start_date <= t.end_date
              AND conn3.end_date >= t.start_date
              AND (t.accounting_type_id = 17 OR t.child_id = row_record.node_calculate_parameter_id) -- Дерево строиться только при безучетном расходе
        )
        SELECT
        	tree_cte.line_id,
            tree_cte.line_name,
            tree_cte.node_id,
            tree_cte.child_id,
            tree_cte.level,
            tree_cte.path,
            tree_cte.path_str,
            GREATEST(tree_cte.start_date,conn.start_date) as start_date,
            LEAST(tree_cte.end_date,conn.end_date) as end_date,
            tree_cte.accounting_type_id,
            tree_cte.accounting_type_node_id,
            conn.connection_id,
            -- Добавление подтвеждений влияет на то, что подключение должно счиаться непересчитываемым по ГПУ и не подлежащим балансировке
            -- Также не должно сформироваться в новые начисления и расходы.
            -- если подтверждено подключение, мы его считает не перерасчитываемым по ГПУ
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM rul_charge rc
                    WHERE rc.connection_id = conn.connection_id
                      AND rc.charge_checked = 1
                      AND rc.billing_start_date <= p_end_date
                      AND rc.billing_end_date >= p_start_date
                ) THEN 4
                ELSE conn.group_recalculation_attitude_id
            END AS group_recalculation_attitude_id,
            conn.allocation_source_consumption_id,
            tree_cte.formula_id
        FROM tree_cte
        left join  (
            SELECT
                c.connection_id,
                c.connection_name,
                GREATEST(c.start_date, row_record.start_date) AS start_date,
                LEAST(COALESCE(c.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                c.node_calculate_parameter_id,
                c.unaccounted_source_consumption_id,
                c.allocation_source_consumption_id,
                c.group_recalculation_attitude_id
            FROM rul_connection c
            WHERE
                c.start_date <= row_record.end_date
                AND COALESCE(c.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
        ) conn ON tree_cte.child_id = conn.node_calculate_parameter_id
            AND tree_cte.start_date <= conn.end_date
            AND tree_cte.end_date >= conn.start_date
        ORDER BY path, node_id, child_id, tree_cte.start_date;
        CREATE TEMPORARY TABLE temp_results AS
        SELECT
        	tree_cte.line_id,
            tree_cte.line_name,
            tree_cte.node_id,
            tree_cte.child_id,
            tree_cte.level,
            tree_cte.path,
            tree_cte.path_str,
            case when tree_cte.accounting_type_id = 2 then  tree_cte.start_date else GREATEST(cons.start_date, tree_cte.start_date) end as start_date,
            case when tree_cte.accounting_type_id = 2 then tree_cte.end_date else LEAST(cons.end_date,tree_cte.end_date) end as end_date,
            tree_cte.accounting_type_id,
            tree_cte.accounting_type_node_id,
            tree_cte.connection_id,
            -- Добавление подтвеждений влияет на то, что подключение должно счиаться непересчитываемым по ГПУ и не подлежащим балансировке
            -- Также не должно сформироваться в новые начисления и расходы.
            -- если подтверждено подключение, мы его считает не перерасчитываемым по ГПУ
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM rul_charge rc
                    WHERE rc.connection_id = tree_cte.connection_id
                      AND rc.charge_checked = 1
                      AND rc.billing_start_date <= p_end_date
                      AND rc.billing_end_date >= p_start_date
                ) THEN 4
                ELSE tree_cte.group_recalculation_attitude_id
            END AS group_recalculation_attitude_id,
            tree_cte.allocation_source_consumption_id,
            cons.value,
            cons.connection_name,
            case when (extract (day from (date_trunc('day',LEAST(cons.end_date,tree_cte.end_date))
            - GREATEST(cons.start_date, tree_cte.start_date)))) + 1 = 0
                    then 1
                 else (extract (day from (date_trunc('day',LEAST(cons.end_date,tree_cte.end_date))
            - GREATEST(cons.start_date, tree_cte.start_date)))) + 1
            end
              *
            cons.value /
                case when (extract (day from (date_trunc('day',cons.end_date) - cons.start_date))) + 1 = 0
                then 1
                else (extract (day from (date_trunc('day',cons.end_date) - cons.start_date))) + 1
                end
            / case when tree_cte.accounting_type_id <> 17 and tree_cte.level != 0
            then count(*) over (partition by tree_cte.child_id,GREATEST(cons.start_date, tree_cte.start_date)
            ,LEAST(cons.end_date,tree_cte.end_date))
            else 1 end
                as val,
            cons.source_consumption_id,
            tree_cte.formula_id,
            cons.note
        FROM new_temp_results tree_cte
        LEFT JOIN
        (
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id, note
              , 1 as source_consumption_id
            FROM
              public.rul_consumption_load
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id, note
              , 2 as source_consumption_id
            FROM
              public.rul_consumption_standard
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id, note
              , 4 as source_consumption_id
            FROM
              public.rul_consumption_source_connection
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  '-----', start_date,  end_date,  value, accounting_type_node_id, 2 as accounting_type_id, null, null
            FROM
              public.rul_consumption
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND value != 0
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 19 as accounting_type_id, null, null
            FROM
              public.rul_consumption_pipe
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 5 as accounting_type_id, null, null
            FROM
              public.rul_consumption_average
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
        ) cons ON (cons.connection_id = tree_cte.connection_id or cons.accounting_type_id = 2)
                AND cons.accounting_type_node_id = tree_cte.accounting_type_node_id
                AND (
                    (tree_cte.allocation_source_consumption_id = cons.source_consumption_id
                    and (tree_cte.accounting_type_id = 17 or tree_cte.child_id = row_record.node_calculate_parameter_id))
                    or (tree_cte.accounting_type_id = 2 and cons.accounting_type_id = 2 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 5 and cons.accounting_type_id = 5 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 19 and cons.accounting_type_id = 19 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    )
                AND tree_cte.start_date <= cons.end_date
                AND tree_cte.end_date >= cons.start_date
        ORDER BY path, node_id, child_id, tree_cte.start_date;
        --call public.process_formuls_losses(row_record.node_calculate_parameter_id,row_record.start_date,row_record.end_date);
        call public.process_formuls_losses(row_record.node_calculate_parameter_id,p_start_date,p_end_date);
        IF row_record.accounting_type_id = 17 THEN
        	coef := 1;
        ELSE
            IF row_record.accounting_type_id = 2 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
                  from public.rul_consumption rcons
                  left join public.rul_accounting_type_node ratn
                    on rcons.accounting_type_node_id = ratn.accounting_type_node_id
                    and ratn.accounting_type_node_id =  row_record.accounting_type_node_id
                  where ratn.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                  	and rcons.start_date < p_end_date
                  	and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') >= p_start_date
                ) py;
            ELSIF row_record.accounting_type_id = 5 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct ratn.node_calculate_parameter_id,
                      rcona.start_date,
                      rcona.end_date,
                      rcona.value
                  from public.rul_consumption_average rcona
                  left join public.rul_accounting_type_node ratn
                    on rcona.accounting_type_node_id = ratn.accounting_type_node_id
                    and ratn.accounting_type_node_id =  row_record.accounting_type_node_id
                  where ratn.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                  	and rcona.start_date < p_end_date
                  	and COALESCE(rcona.end_date, '2100-04-30 23:59:59+03') >= p_start_date
                ) py;
            ELSIF row_record.accounting_type_id = 19 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct ratn.node_calculate_parameter_id,rconp.start_date,rconp.end_date,rconp.value
                  from public.rul_consumption_pipe rconp
                  left join public.rul_accounting_type_node ratn
                    on rconp.accounting_type_node_id = ratn.accounting_type_node_id
                    and ratn.accounting_type_node_id =  row_record.accounting_type_node_id
                  where rconp.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                  	and rconp.start_date < p_end_date
                  	and COALESCE(rconp.end_date, '2100-04-30 23:59:59+03') >= p_start_date
                ) py;
            END IF;
            select sum(rcl.value) into Losses
            from temp_results tr
            join rul_consumption_losses rcl
            	on tr.line_id = rcl.line_id
            	and tr.start_date <= rcl.start_date
            	and tr.end_date >= rcl.end_date
            ;
            -- Выбираем ручные начисления которые возьмем в формулу расчета коэффициента
            select sum(rc.sum_consumption) into Charges
            from temp_results tr
            join rul_charge rc
            	on tr.connection_id = rc.connection_id
            	and tr.start_date <= rc.billing_start_date
            	and tr.end_date >= rc.billing_start_date
            	and (rc.charge_type_id = 2 -- Ручные
            	or  tr.group_recalculation_attitude_id = 4);
            with result_for_coefficient as (
            select
            Losses as poteri,
            Charges as charges_nepodl,
            PY_SUM as PY,
            SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END) as indication,
            SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END) as nepodl,
            SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END) as podl_vniz,
            SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN coalesce(val,0) END) as podl,
            case when
            coalesce(PY_SUM,0) - coalesce(Losses,0) - coalesce(Charges,0)
            - coalesce(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),0)
            - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END),0)
            - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0)
            - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN coalesce(val,0) END),0)
            > 0 then 1 else 0 end as X
            from temp_results)
            select (coalesce(PY,0) - coalesce(indication,0) - coalesce(nepodl,0) - coalesce(charges_nepodl,0) - ( X * coalesce(podl_vniz,0) ))
                    /
                    ( case when ( ( 1 - X ) * coalesce(podl_vniz,0) + coalesce(podl,0) + coalesce(poteri,0)) = 0 then 1 else ( ( 1 - X ) * coalesce(podl_vniz,0) + coalesce(podl,0) + coalesce(poteri,0)) end )
                    into coef
            from result_for_coefficient;
        END IF;
        RAISE NOTICE 'COEFICIENT %',coef;
        INSERT INTO public.rul_consumption_load
      	(
        connection_id, start_date, end_date, value, accounting_type_node_id, coefficient, theoretical_calculation, note
      	)
      	SELECT connection_id, start_date, end_date, val, accounting_type_node_id,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1)) THEN coef
        ELSE 1 END
        , false ,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1))
        THEN get_notes(row_record.accounting_type_id,temp_results.val::numeric,coef,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        ELSE get_notes(row_record.accounting_type_id,temp_results.val::numeric,1::bigint,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        END
        FROM temp_results WHERE source_consumption_id = 1
        AND NOT EXISTS (
            SELECT 1
            FROM rul_charge rc
            WHERE rc.connection_id = temp_results.connection_id
              AND rc.charge_checked = 1
              AND rc.billing_start_date <= p_end_date
              AND rc.billing_end_date >= p_start_date
        );
        INSERT INTO public.rul_consumption_standard
      	(
        connection_id, start_date, end_date, value, accounting_type_node_id, coefficient, theoretical_calculation, note
      	)
      	SELECT connection_id, start_date, end_date, val, accounting_type_node_id,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1)) THEN coef
        ELSE 1 END, FALSE ,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1))
        THEN get_notes(row_record.accounting_type_id,temp_results.val::numeric,coef,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        ELSE get_notes(row_record.accounting_type_id,temp_results.val::numeric,1::bigint,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        END
        FROM temp_results where source_consumption_id = 2
        AND NOT EXISTS (
            SELECT 1
            FROM rul_charge rc
            WHERE rc.connection_id = temp_results.connection_id
              AND rc.charge_checked = 1
              AND rc.billing_start_date <= p_end_date
              AND rc.billing_end_date >= p_start_date
        );
        INSERT INTO public.rul_consumption_source_connection
      	(
        connection_id, connection_name, start_date, end_date, value, accounting_type_node_id, coefficient, theoretical_calculation, note
      	)
      	SELECT connection_id, (select connection_name from rul_connection where connection_id = temp_results.connection_id), start_date, end_date, val, accounting_type_node_id,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1)) THEN coef
        ELSE 1 END, FALSE ,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1))
        THEN get_notes(row_record.accounting_type_id,temp_results.val::numeric,coef,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        ELSE get_notes(row_record.accounting_type_id,temp_results.val::numeric,1::bigint,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        END
        FROM temp_results  where source_consumption_id = 4
        AND NOT EXISTS (
            SELECT 1
            FROM rul_charge rc
            WHERE rc.connection_id = temp_results.connection_id
              AND rc.charge_checked = 1
              AND rc.billing_start_date <= p_end_date
              AND rc.billing_end_date >= p_start_date
        );
        UPDATE public.rul_consumption_losses
        SET coefficient = coef
        WHERE line_id in (select distinct line_id from temp_results)
        and start_date >= p_start_date
        and end_date <= p_end_date;
        drop table temp_results;
        drop table new_temp_results;
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.new_process_group_accounting_new(IN p_agreement_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.new_process_group_accounting_new(IN p_agreement_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    row_record record;
    row_record2 record;
    coef numeric;
    v_conn_conn_ids bigint[];
    v_connection_ids BIGINT[];
    v_connection_ids_formuls BIGINT[];
    v_agreement_ids BIGINT[];
BEGIN
	CREATE TEMP TABLE IF NOT EXISTS processed_child_ids (
    child_id INT PRIMARY KEY
	) ON COMMIT DROP;
    select array_agg(node_calculate_parameter_id) into v_conn_conn_ids
    from rul_connection rc
    join rul_connection_connection rcc
    on rc.connection_id = rcc.source_connection_id;
	-- В цикле получаем Верхние узлы деревьев из договора
    FOR row_record IN
    SELECT
        MIN(acc.start_date) AS start_date
        ,MAX(acc.end_date) AS end_date
        --,acc.accounting_type_node_id as accounting_type_node_id
        ,acc.node_calculate_parameter_id as node_calculate_parameter_id
        ,case when acc.node_calculate_parameter_id = ANY (v_conn_conn_ids) then 1
              when (SELECT parameter_id FROM rul_node_calculate_parameter
                        WHERE node_calculate_parameter_id = acc.node_calculate_parameter_id) IN (1,2,7)
                then 0.5
        else 0 end as sort
    FROM
    (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id,
            atn.accounting_type_id
        FROM rul_accounting_type_node atn
        WHERE atn.start_date <= p_end_date
        	AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
        	) acc
    where acc.accounting_type_id in (2,5,17,19)
    AND acc.node_calculate_parameter_id IN
    (
        SELECT DISTINCT rncp.commercial_node_calculate_parameter_id
        FROM rul_node_calculate_parameter rncp
        JOIN rul_connection rc
            ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
            AND rc.agreement_id = ANY(p_agreement_ids)
        WHERE rncp.commercial_node_calculate_parameter_id IS NOT NULL
    )
    GROUP BY
        acc.node_calculate_parameter_id
	ORDER BY sort DESC
    LOOP
        -- Обработка каждой строки
        RAISE NOTICE 'Обработка дерева по договорам : node_calculate_parameter_id=%', row_record.node_calculate_parameter_id;
        --Создаем temp дерево для цикла расчета по нему.
        CREATE TEMP TABLE tree AS
        WITH RECURSIVE tree_cte AS (
              -- Базовый случай: выбираем корневые элементы
              SELECT
                  zero_level.name as line_name,
                  zero_level.node_id AS node_id,
                  zero_level.node_id AS child_id,
                  0 AS level,
                  ARRAY[zero_level.node_id] AS path,
                  zero_level.node_id::TEXT AS path_str,
                  -- Добавляем даты для корневого элемента
                  GREATEST(conn3.start_date, row_record.start_date) AS start_date,
                  LEAST(conn3.end_date, row_record.end_date) AS end_date,
                  conn3.accounting_type_id as accounting_type_id,
                  conn3.accounting_type_node_id as accounting_type_node_id
              FROM (select 'zero_level' as name, row_record.node_calculate_parameter_id as node_id) zero_level
              JOIN (
                      SELECT
                          atn.accounting_type_node_id,
                          GREATEST(atn.start_date, row_record.start_date) AS start_date,
                          LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                          atn.node_calculate_parameter_id,
                          atn.accounting_type_id
                      FROM rul_accounting_type_node atn
                      WHERE
                          atn.start_date < row_record.end_date
                          AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                          AND atn.accounting_type_id in (2,5,17,19)
              ) conn3 ON conn3.node_calculate_parameter_id = zero_level.node_id
              UNION ALL
              -- Рекурсивный случай: присоединяем детей с учетом дат родителя
              SELECT
                  rl.line_name,
                  rlp.node_calculate_parameter_id AS node_id,
                  rlpc.node_calculate_parameter_id AS child_id,
                  t.level + 1,
                  t.path || rlpc.node_calculate_parameter_id,
                  t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                  -- Вычисляем даты дочернего элемента на основе родительского
                  GREATEST(conn3.start_date, t.start_date) AS start_date,
                  LEAST(conn3.end_date, t.end_date) AS end_date,
                  conn3.accounting_type_id,
                  conn3.accounting_type_node_id as accounting_type_node_id
              FROM tree_cte t
              JOIN public.rul_line_parameter rlp
                  ON t.child_id = rlp.node_calculate_parameter_id
              JOIN public.rul_line_parameter_child rlpc
                  ON rlpc.line_parameter_id = rlp.line_parameter_id
              JOIN public.rul_line rl
                  ON rl.line_id = rlp.line_id
              JOIN (
                      SELECT
                          atn.accounting_type_node_id,
                          GREATEST(atn.start_date, row_record.start_date) AS start_date,
                          LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                          atn.node_calculate_parameter_id,
                          atn.accounting_type_id
                      FROM rul_accounting_type_node atn
                      WHERE
                          atn.start_date < row_record.end_date
                          AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                          AND atn.accounting_type_id in (2,5,17,19)
              ) conn3 ON conn3.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
              WHERE rl.client_id IS NOT NULL
                -- Ограничиваем даты дочерних элементов датами родителя
                AND conn3.start_date <= t.end_date
                AND conn3.end_date >= t.start_date
        )
          SELECT
              tree_cte.child_id,
              tree_cte.accounting_type_id,
              tree_cte.accounting_type_node_id
          FROM tree_cte
          WHERE
              (tree_cte.accounting_type_id <> 17 OR tree_cte.level = 0)
          GROUP BY level, child_id, accounting_type_id, tree_cte.accounting_type_node_id
          ORDER BY level desc, child_id, accounting_type_id, tree_cte.accounting_type_node_id;
            --Теперь, зная все РП, считаем расхода для каждого РП (подключения)
            -- Рассчитать расходы для подключений по всем безучетным формулам
            -- Нужно ли пытаться выбрать подключения для конкретного способа учета? Или же пересчитать расходы по подключениям
            SELECT array_agg(DISTINCT rc.connection_id) INTO v_connection_ids_formuls
            FROM tree tr
            JOIN rul_connection rc ON tr.child_id = rc.node_calculate_parameter_id
            WHERE rc.connection_id IS NOT NULL;
            RAISE NOTICE '%', v_connection_ids_formuls;
            CALL public.new_process_formuls_load(v_connection_ids_formuls,p_start_date,p_end_date);
            CALL public.new_process_formuls_standard(v_connection_ids_formuls,p_start_date,p_end_date);
            CALL public.new_process_formuls_pipe(v_connection_ids_formuls,p_start_date,p_end_date);
            DELETE FROM rul_consumption_losses WHERE start_date >= p_start_date AND end_date <= p_end_date
            AND ((connection_id = ANY(v_connection_ids_formuls) OR connection_id IS NULL));
          FOR row_record2 IN (SELECT child_id, accounting_type_id, accounting_type_node_id FROM tree)
          LOOP
          	-- Нужно сохранять передаваемые в ГПУ параметры и не считать их повторно, кроме узла level=0. Не сделано!
			IF NOT EXISTS (SELECT 1 FROM processed_child_ids WHERE child_id = row_record2.child_id) THEN
        	-- Добавляем child_id в список обработанных
        	INSERT INTO processed_child_ids (child_id) VALUES (row_record2.child_id) ON CONFLICT (child_id) DO NOTHING;
                RAISE NOTICE 'Расчет следующего элемента для: node_calculate_parameter_id=%, accounting_type_node_id=%, accounting_type_node_id=%',
                      row_record2.child_id, row_record2.accounting_type_id, row_record2.accounting_type_node_id;
              IF row_record2.accounting_type_id = 5 THEN
                  RAISE NOTICE 'Расчет по среднему для: node_calculate_parameter_id=%, accounting_type_node_id=%, accounting_type_node_id=%',
                      row_record2.child_id, row_record2.accounting_type_id, row_record2.accounting_type_node_id;
                  --Расчет расхода для среднего
                  CALL public.process_formuls_average(p_start_date,p_end_date,row_record2.child_id);
                  --Расчет ГПУ для среднего
                  IF row_record2.child_id = row_record.node_calculate_parameter_id THEN
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,TRUE);
                  ELSE
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,FALSE);
                  --CALL public.process_group_accounting3(row_record2.child_id,row_record.start_date,row_record.end_date,row_record2.accounting_type_node_id);
              -- Безучетный считаем только если он в верхнеуровневом коммерческом узле, иначен е трогаем
                  END IF;
              ELSIF row_record2.accounting_type_id in (2,19,17) THEN
                  RAISE NOTICE 'Расчет приборного или по сечению для: node_calculate_parameter_id=%, accounting_type_id=%, accounting_type_node_id=%',
                      row_record2.child_id, row_record2.accounting_type_id, row_record2.accounting_type_node_id;
                  --Расчет ГПУ для приборного и по сечению
                  CALL public.process_formuls_average(p_start_date,p_end_date,row_record2.child_id);
                  IF row_record2.child_id = row_record.node_calculate_parameter_id THEN
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,TRUE);
                  ELSE
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,FALSE);
                  --CALL public.process_group_accounting3(row_record2.child_id,row_record.start_date,row_record.end_date,row_record2.accounting_type_node_id);
              -- Безучетный считаем только если он в верхнеуровневом коммерческом узле, иначен е трогаем
                  END IF;
              END IF;
            ELSE
                RAISE NOTICE 'Пропуск повторного расчета для child_id=%', row_record2.child_id;
            END IF;
          END LOOP;
        SELECT array_agg(rcc.destination_connection_id)
        INTO v_connection_ids
        FROM rul_connection_connection rcc
        JOIN rul_connection rc ON rcc.source_connection_id = rc.connection_id
        JOIN tree tr ON tr.child_id = rc.node_calculate_parameter_id;
        CALL public.process_formuls_source_connection(v_connection_ids,p_start_date,p_end_date);
        SELECT array_agg(DISTINCT rc.agreement_id)
        INTO v_agreement_ids
        FROM tree tr
        JOIN rul_connection rc
            ON tr.child_id = rc.node_calculate_parameter_id;
        -- Все это работает только в том случае, если происходит один проход для РП в рамках месяца.
        -- Чистка старых начислений, кроме ручных.
        -- Удаляем начисления и детали по подключениям входящим в сеть в рамках расчетного периода.
        DELETE FROM rul_charge WHERE charge_type_id != 2 AND start_date >= p_start_date AND end_date <= p_end_date
          AND connection_id = ANY(v_connection_ids_formuls)
          AND charge_checked = 0;
        CALL public.new_process_charges_new(v_connection_ids_formuls ,p_start_date, p_end_date);
        CALL public.new_process_charges_losses(v_connection_ids_formuls ,p_start_date, p_end_date);
        CALL public.new_process_charges_planned_consumption(v_agreement_ids ,p_start_date, p_end_date);
          DROP TABLE tree;
          RAISE NOTICE 'Дерево посчитано: node_calculate_parameter_id=%',
                      row_record.node_calculate_parameter_id;
        -- Ваша логика обработки здесь
    END LOOP;
    DROP TABLE processed_child_ids;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.old_process_charges(IN p_agreement_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.old_process_charges(IN p_agreement_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_ids BIGINT;
BEGIN
	FOREACH v_ids IN ARRAY p_agreement_id
    LOOP
      insert into rul_debug_log (debug_user_id,debug_module)
      values (current_setting('user_ctx.user_name',true),'Пересчет');
      -- Чистка старых начислений, кроме ручных.
      -- Удаляем начисления и детали по подключениям входящим договора в рамках расчетного периода.
      DELETE FROM rul_charge_detail WHERE start_date >= p_start_date AND end_date <= p_end_date
      AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = v_ids)
      AND (SELECT charge_checked FROM rul_charge WHERE charge_id = rul_charge_detail.charge_id) = 0;
      DELETE FROM rul_charge WHERE charge_type_id != 2 AND start_date >= p_start_date AND end_date <= p_end_date
      AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = v_ids)
      AND charge_checked = 0;
      DELETE FROM rul_consumption_losses WHERE start_date >= p_start_date AND end_date <= p_end_date
      AND ((connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = v_ids) OR connection_id is null)
      OR connection_id IS NULL);
      -- Формирование начислений по приборному учету.
      -- На данный момент просто формирует начисления исходя из показаний и формул.
      CALL public.process_consumption_data(p_start_date, p_end_date);
      CALL public.process_formuls_indication(p_start_date, p_end_date);
      -- Формирование теоретических нагрузочных расходов. Фактические будут считаться в ГПУ.
      --CALL public.process_formuls_load(v_ids,p_start_date,p_end_date);
      -- Формирование теоретических расходов для способа учета по норме.
      -- Использует для своих расчетов метод, а не формулу. Также вместо id параметров ссылается на имена параметров
      --CALL public.process_formuls_standard(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по подключению-источнику.
      --CALL public.process_formuls_source_connection(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по сечению.
      CALL public.process_formuls_pipe(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по среднему.
      -- CALL public.process_formuls_average(p_agreement_id,p_start_date,p_end_date); --Вызывается в ГПУ
      -- Расчет по ГПУ. Должен построить деревья в рамках договора. С нижних листов рассчитать Учетные способы учета.
      -- Затем произвести для каждого узла ГПУ.
      -- Так же должен обойти отдельные узлы, чтобы в них расчить начисления
      CALL public.process_group_accounting_new(v_ids, p_start_date, p_end_date);
      --25.11.2025 Новый расчет начислений, вернуть если что отдельные методы
      /*CALL public.process_charges_standard(p_agreement_id,p_start_date,p_end_date);
      CALL public.process_charges_load(p_agreement_id,p_start_date,p_end_date);
      CALL public.process_charges_source_connection(p_agreement_id,p_start_date,p_end_date);*/
      --CALL public.process_charges_new(v_ids,p_start_date,p_end_date);
      --Начисление по потерям надо создавать после балансировки
      CALL public.process_charges_losses(v_ids,p_start_date,p_end_date);
      CALL public.process_charges_planned_consumption(v_ids,p_start_date,p_end_date);
      --CALL public.process_zero_charges(v_ids,p_start_date,p_end_date);
      --CALL public.process_charges_pipe(p_agreement_id,p_start_date,p_end_date); -- Их не будет вообще походу
      --CALL public.process_charges_average(p_agreement_id,p_start_date,p_end_date); -- Их не будет вообще походу
      --CALL public.process_charges_indication(p_agreement_id,p_start_date,p_end_date); -- Их не будет вообще походу
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_balancing(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_balancing(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    row_record record;
    i record;
    coef numeric;
    PY_SUM numeric;
    Losses numeric;
    Losses_supplier numeric;
    Losses_customer_billed numeric;
    Losses_customer_unbilled numeric;
    Realize_consumption numeric;
    Charges_losses_customer_billed numeric;
    Charges_losses_customer_unbilled numeric;
    Charges_realized numeric;
    v_balancing_id bigint;
    Charges numeric;
    v_ids BIGINT[];
BEGIN
    -- Получаем ID балансировки для узла
    SELECT balancing_id INTO v_balancing_id
    FROM rul_balancing
    WHERE node_calculate_parameter_id = p_node_calculate_parameter_id
    AND start_date >= p_start_date
    AND end_date <= p_end_date
    LIMIT 1;
    IF v_balancing_id IS NOT NULL
    THEN
    	CALL delete_balancing(ARRAY[v_balancing_id]);
    END IF;
    FOR row_record IN
    SELECT
        acc.start_date AS start_date
        ,acc.end_date AS end_date
        ,acc.accounting_type_node_id as accounting_type_node_id
        ,acc.node_calculate_parameter_id as node_calculate_parameter_id
        ,acc.accounting_type_id as accounting_type_id
    FROM
    (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id,
            atn.accounting_type_id
        FROM rul_accounting_type_node atn
        WHERE atn.start_date <= p_end_date
        	AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) > p_start_date
        	AND atn.node_calculate_parameter_id = p_node_calculate_parameter_id
    ) acc
    where acc.accounting_type_id in (2,5,17,19)
    LOOP
        -- Обработка каждой строки
        RAISE NOTICE 'Processing row: node_calculate_parameter_id=%, accounting_type_node_id=%', row_record.node_calculate_parameter_id, row_record.accounting_type_node_id;
        --RAISE EXCEPTION '[[Для рассчета баланса требуется сбалансировать все нижележащие узлы ]]'  USING ERRCODE = '25000';
       CREATE TEMP TABLE temp_results
    	as
        WITH RECURSIVE tree_cte AS (
            -- Базовый случай: выбираем корневые элементы
            SELECT
                zero_level.name as line_name,
                null::bigint as line_id,
                zero_level.node_id AS node_id,
                zero_level.node_id AS child_id,
                0 AS level,
                ARRAY[zero_level.node_id] AS path,
                zero_level.node_id::TEXT AS path_str,
                -- Добавляем даты для корневого элемента
                GREATEST(conn3.start_date, row_record.start_date) AS start_date,
                LEAST(conn3.end_date, row_record.end_date) AS end_date,
                conn3.accounting_type_id as accounting_type_id,
        		conn3.accounting_type_node_id as accounting_type_node_id,
                null::bigint as formula_id,
                0 as after_indication_accounting,
                1 as balancing_line,
                3::BIGINT as node_type_id
    		FROM (select 'zero_level' as name, row_record.node_calculate_parameter_id::bigint as node_id) zero_level
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, row_record.start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                    	atn.start_date < row_record.end_date::timestamp without time zone
        				AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date::timestamp without time zone
            ) conn3 ON conn3.node_calculate_parameter_id = zero_level.node_id
            UNION ALL
            -- Рекурсивный случай: присоединяем детей с учетом дат родителя
            SELECT
                rl.line_name,
                rl.line_id,
                rlp.node_calculate_parameter_id AS node_id,
                rlpc.node_calculate_parameter_id AS child_id,
                t.level + 1,
                t.path || rlpc.node_calculate_parameter_id,
                t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                -- Вычисляем даты дочернего элемента на основе родительского
                GREATEST(conn3.start_date, t.start_date) AS start_date,
                LEAST(conn3.end_date, t.end_date) AS end_date,
                conn3.accounting_type_id,
                conn3.accounting_type_node_id as accounting_type_node_id,
                rlp.formula_id,
                case when t.after_indication_accounting = 1 then 1
                	 when t.accounting_type_id != 17 and t.child_id != row_record.node_calculate_parameter_id then 1
                     else 0 end as after_indication_accounting,
                --В balancing_line Пытаемся понять для каких линий уже созданы расходы и их соответственно не нужно считать.
                --Не нужно считать линни после балансного узла и коммерческого т.к. они обработаются пересчетом договора либо балансировкой.
                --Так же не нужно считать линии, которые на балансе абонентов, а не поставщика. Они также посчитаются с договором или балансировкой
                case when t.balancing_line = 0 then 0
                	 when (t.node_type_id = 1 or t.level = 0) then 1 -- Почему 1, а не 3 ???
                     when (rl.client_id != (select client_id from rul_user where user_id =
                     								(select case when current_setting('user_ctx.user_name',true) = '' then null::bigint
                     									else current_setting('user_ctx.user_name',true)::bigint end)))
                     then 0
                     else 0 end as balancing_line,
                rn.node_type_id
                     FROM tree_cte t
            JOIN public.rul_line_parameter rlp
                ON t.child_id = rlp.node_calculate_parameter_id
            JOIN public.rul_line_parameter_child rlpc
                ON rlpc.line_parameter_id = rlp.line_parameter_id
            JOIN public.rul_node_calculate_parameter rncp
            	ON rlpc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
            JOIN public.rul_node rn
            	ON rn.node_id = rncp.node_id
            JOIN public.rul_line rl
                ON rl.line_id = rlp.line_id
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, row_record.start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                        atn.start_date <= row_record.end_date
                        AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
            ) conn3 ON conn3.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
            WHERE rl.client_id IS NOT NULL
              -- Ограничиваем даты дочерних элементов датами родителя
              AND conn3.start_date <= t.end_date
              AND conn3.end_date >= t.start_date
              --AND (t.accounting_type_id = 17 OR t.child_id = row_record.node_calculate_parameter_id) -- Дерево строиться только при безучетном расходе
        )
        SELECT
        	tree_cte.line_id,
            tree_cte.line_name,
            tree_cte.node_id,
            tree_cte.child_id,
            tree_cte.level,
            tree_cte.path,
            tree_cte.path_str,
            GREATEST(cons.start_date, tree_cte.start_date,conn.start_date) as start_date,
            LEAST(cons.end_date,tree_cte.end_date,conn.end_date) as end_date,
            tree_cte.accounting_type_id,
            tree_cte.after_indication_accounting,
            tree_cte.balancing_line,
            tree_cte.accounting_type_node_id,
            conn.connection_id,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM rul_charge rc
                    WHERE rc.connection_id = conn.connection_id
                      AND rc.charge_checked = 1
                      AND rc.billing_start_date <= p_end_date
                      AND rc.billing_end_date >= p_start_date
                ) THEN 1
                ELSE conn.resource_balance_attitude_id
            END AS resource_balance_attitude_id,
            conn.allocation_source_consumption_id,
            cons.value,
            cons.connection_name,
            (extract (day from (date_trunc('day',LEAST(cons.end_date,tree_cte.end_date,conn.end_date)+ interval '1 second')
            - GREATEST(cons.start_date, tree_cte.start_date,conn.start_date))))  *
            cons.value /
            case when (extract (day from (date_trunc('day',cons.end_date + interval '1 second') - cons.start_date))) = 0
            	then 1
                else (extract (day from (date_trunc('day',cons.end_date + interval '1 second') - cons.start_date)))
                end
            / case when tree_cte.accounting_type_id <> 17 and tree_cte.level != 0
    		then count(*) over (partition by tree_cte.child_id,GREATEST(cons.start_date, tree_cte.start_date,conn.start_date)
            ,LEAST(cons.end_date,tree_cte.end_date,conn.end_date))
    		else 1 end
                as val,
            cons.source_consumption_id,
            tree_cte.formula_id,
            tree_cte.node_type_id
        FROM tree_cte
        left join  (
            SELECT
                c.connection_id,
                c.connection_name,
                GREATEST(c.start_date, row_record.start_date) AS start_date,
                LEAST(COALESCE(c.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                c.node_calculate_parameter_id,
                c.unaccounted_source_consumption_id,
                c.allocation_source_consumption_id,
                c.resource_balance_attitude_id
            FROM rul_connection c
            WHERE
                c.start_date <= row_record.end_date
                AND COALESCE(c.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
        ) conn ON tree_cte.child_id = conn.node_calculate_parameter_id
            AND tree_cte.start_date <= conn.end_date
            AND tree_cte.end_date >= conn.start_date
        LEFT JOIN
        (
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id
              , 1 as source_consumption_id
            FROM
              public.rul_consumption_load
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id
              , 2 as source_consumption_id
            FROM
              public.rul_consumption_standard
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id
              , 4 as source_consumption_id
            FROM
              public.rul_consumption_source_connection
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  '-----', start_date,  end_date,  value, accounting_type_node_id, 2 as accounting_type_id, null
            FROM
              public.rul_consumption
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND value != 0
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 19 as accounting_type_id, null
            FROM
              public.rul_consumption_pipe
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 5 as accounting_type_id, null
            FROM
              public.rul_consumption_average
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
        ) cons ON (cons.connection_id = conn.connection_id or cons.accounting_type_id = 2)
                AND cons.accounting_type_node_id = tree_cte.accounting_type_node_id
                AND tree_cte.after_indication_accounting != 1 --Для тех, которые следуют за расчитаным приборным учетом, не нужны расходы
                AND (
                    (conn.allocation_source_consumption_id = cons.source_consumption_id
                    and (tree_cte.accounting_type_id = 17 or tree_cte.child_id = row_record.node_calculate_parameter_id))
                    or (tree_cte.accounting_type_id = 2 and cons.accounting_type_id = 2 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 5 and cons.accounting_type_id = 5 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 19 and cons.accounting_type_id = 19 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    )
                AND GREATEST(tree_cte.start_date,conn.start_date) <= cons.end_date
                AND LEAST(tree_cte.end_date,conn.end_date) >= cons.start_date
        ORDER BY path, node_id, child_id, tree_cte.start_date;
        -- Получаем айдишники РП балансных узлов в дереве для проверки
        FOR i IN
        SELECT child_id FROM temp_results
        WHERE node_type_id = 3
        AND child_id != row_record.node_calculate_parameter_id
        LOOP
        -- Проверяем, есть ли хотя бы одна запись в rul_balancing для этого child_id в указанных датах
          PERFORM 1
          FROM rul_balancing
          WHERE node_calculate_parameter_id = i.child_id
            AND start_date >= p_start_date
            AND end_date <= p_end_date;
          -- Если НЕТ ни одной записи — вызываем ошибку
          IF NOT FOUND THEN
              RAISE EXCEPTION '[[Для расчета баланса требуется сбалансировать все нижележащие балансные узлы: %]]',
              (SELECT node_name || ' (' || ro.object_name || ')' FROM rul_node
              				    JOIN rul_object ro
                                ON rul_node.object_id = ro.object_id
              	WHERE node_id =
              		(SELECT node_id FROM rul_node_calculate_parameter where node_calculate_parameter_id = i.child_id))
              USING ERRCODE = '25000';
          END IF;
        END LOOP;
        --call public.process_formuls_losses(row_record.node_calculate_parameter_id,row_record.start_date,row_record.end_date);
        call public.process_formuls_losses(row_record.node_calculate_parameter_id,p_start_date,p_end_date,1::smallint);
        select sum(a.b) into Losses FROM
        (SELECT
        round(rcl.value,3) /
                      CASE WHEN (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id) = 0
                      THEN 1
                      ELSE (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id)
                      END b
        from temp_results tr
        join rul_consumption_losses rcl
        on tr.line_id = rcl.line_id
        and tr.accounting_type_node_id = rcl.accounting_type_node_id
        and tr.start_date <= rcl.end_date
        and tr.end_date >= rcl.start_date
        and (tr.balancing_line = 1 or tr.after_indication_accounting = 0)
        ) a;
        -- Выбираем ручные начисления которые возьмем в формулу расчета коэффициента
        select sum(rc.sum_consumption) into Charges
        from temp_results tr
        join rul_charge rc
        on tr.connection_id = rc.connection_id
        and tr.start_date <= rc.billing_start_date
        and tr.end_date >= rc.billing_start_date
        and rc.charge_type_id = 2 -- Ручные
        ;
        Charges_losses_customer_billed := (select sum(rc.sum_consumption)
        from temp_results tr
        join rul_charge rc
        on tr.connection_id = rc.connection_id
        and tr.start_date <= rc.billing_start_date
        and tr.end_date >= rc.billing_start_date
        and rc.charge_type_id = 2
        AND rc.source_id = 2
        AND (SELECT losses_policy_id FROM rul_connection WHERE connection_id = rc.connection_id) = 1
        )
        ;
        Charges_losses_customer_unbilled := (select sum(rc.sum_consumption)
        from temp_results tr
        join rul_charge rc
        on tr.connection_id = rc.connection_id
        and tr.start_date <= rc.billing_start_date
        and tr.end_date >= rc.billing_start_date
        and rc.charge_type_id = 2
        AND rc.source_id = 2
        AND (SELECT losses_policy_id FROM rul_connection WHERE connection_id = rc.connection_id) = 2
        )
        ;
        Charges_realized := (select sum(rc.sum_consumption)
        from temp_results tr
        join rul_charge rc
        on tr.connection_id = rc.connection_id
        and tr.start_date <= rc.billing_start_date
        and tr.end_date >= rc.billing_start_date
        and rc.charge_type_id = 2
        AND rc.source_id IN (1,3,5,6)
        )
        ;
        IF row_record.accounting_type_id = 17 THEN
        	coef := 1;
            Losses_customer_billed := (select sum(round(a.b,3)) + coalesce(Charges_losses_customer_billed,0) FROM
              (SELECT
              CASE WHEN (tr.balancing_line = 1 or tr.after_indication_accounting = 0) THEN coef
              	ELSE rcl.coefficient END *
              round(rcl.value,3) /
                  CASE WHEN (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id) = 0
                  THEN 1
                  ELSE (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id)
                  END b
              from temp_results tr
              join rul_consumption_losses rcl
              on tr.line_id = rcl.line_id
              and tr.accounting_type_node_id = rcl.accounting_type_node_id
              AND rcl.connection_id IS NOT NULL
              AND (SELECT losses_policy_id FROM rul_connection WHERE connection_id = rcl.connection_id) = 1
              and tr.start_date <= rcl.end_date
              and tr.end_date >= rcl.start_date
              ) a)
              ;
            Losses_customer_unbilled := (select sum(round(a.b,3)) + coalesce(Charges_losses_customer_unbilled,0) FROM
              (SELECT
              CASE WHEN (tr.balancing_line = 1 or tr.after_indication_accounting = 0) THEN coef
              	ELSE rcl.coefficient END *
              round(rcl.value,3) /
                  CASE WHEN (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id) = 0
                  THEN 1
                  ELSE (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id)
                  END b
              from temp_results tr
              join rul_consumption_losses rcl
              on tr.line_id = rcl.line_id
              and tr.accounting_type_node_id = rcl.accounting_type_node_id
              AND rcl.connection_id IS NOT NULL
              AND (SELECT losses_policy_id FROM rul_connection WHERE connection_id = rcl.connection_id) = 2
              and tr.start_date <= rcl.end_date
              and tr.end_date >= rcl.start_date
              ) a)
              ;
            Losses_supplier:= (select sum(round(a.b,3)) FROM
              (SELECT
              CASE WHEN (tr.balancing_line = 1 or tr.after_indication_accounting = 0) THEN coef
              	ELSE rcl.coefficient END
                * round(rcl.value,3) /
                    CASE WHEN (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id) = 0
                    THEN 1
                    ELSE (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id)
                    END b
              from temp_results tr
              join rul_consumption_losses rcl
              on tr.line_id = rcl.line_id
              and tr.accounting_type_node_id = rcl.accounting_type_node_id
              AND rcl.connection_id IS NULL
              and tr.start_date <= rcl.end_date
              and tr.end_date >= rcl.start_date
              ) a)
              ;
            Realize_consumption := (select sum(round(coalesce(val,0),3)) + coalesce(Losses_customer_billed,0) + coalesce(Charges_realized,0) from temp_results);
            PY_SUM := Realize_consumption + coalesce(Losses_supplier,0) + coalesce(Losses_customer_unbilled,0);
        ELSE
            IF row_record.accounting_type_id = 2 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
                  from public.rul_consumption rcons
                  left join public.rul_accounting_type_node ratn
                    on rcons.accounting_type_node_id = ratn.accounting_type_node_id
                  where ratn.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                  and rcons.start_date < p_end_date
                  and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') > p_start_date
                ) py;
            ELSIF row_record.accounting_type_id = 5 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct rconn.node_calculate_parameter_id,rcona.start_date,rcona.end_date,rcona.value
                  from public.rul_consumption_average rcona
                  left join public.rul_connection rconn
                    on rcona.connection_id = rconn.connection_id
                  where rconn.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                ) py;
            ELSIF row_record.accounting_type_id = 19 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct rconn.node_calculate_parameter_id,rconp.start_date,rconp.end_date,rconp.value
                  from public.rul_consumption_pipe rconp
                  left join public.rul_connection rconn
                    on rconp.connection_id = rconn.connection_id
                  where rconn.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                ) py;
            END IF;
            with result_for_coefficient as (
                SELECT
                round(Losses,3) as poteri,
                round(Charges,3) as charges_nepodl,
                round(PY_SUM,3) as PY,
                round(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),3) as indication,
                round(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and resource_balance_attitude_id = 1 THEN coalesce(val,0) END),3) as nepodl,
                round(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and resource_balance_attitude_id = 3 THEN coalesce(val,0) END),3) as podl_vniz,
                case when
                PY_SUM - Losses - coalesce(Charges,0)
                - coalesce(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),0)
                - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and resource_balance_attitude_id = 1 THEN coalesce(val,0) END),0)
                - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and resource_balance_attitude_id = 3 THEN coalesce(val,0) END),0)
                > 0 then 1 else 0 end as X
                from temp_results)
            select (PY - coalesce(indication,0) - coalesce(nepodl,0) - coalesce(charges_nepodl,0) - ( X * coalesce(podl_vniz,0) ))
                    /
                    ( case when ( ( 1 - X ) * coalesce(podl_vniz,0) + coalesce(poteri,0)) = 0 then 1 else ( ( 1 - X ) * coalesce(podl_vniz,0) + coalesce(poteri,0)) end )
                    into coef
            from result_for_coefficient;
            Losses_supplier:= (select sum(round(a.b,3)) FROM
              (SELECT
              CASE WHEN (tr.balancing_line = 1 or tr.after_indication_accounting = 0) THEN coef
              	ELSE rcl.coefficient END
                * round(rcl.value,3) /
                    CASE WHEN (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id) = 0
                    THEN 1
                    ELSE (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id)
                    END b
              from temp_results tr
              join rul_consumption_losses rcl
              on tr.line_id = rcl.line_id
              and tr.accounting_type_node_id = rcl.accounting_type_node_id
              AND rcl.connection_id IS NULL
              and tr.start_date <= rcl.end_date
              and tr.end_date >= rcl.start_date
              ) a)
              ;
            Losses_customer_unbilled := (select sum(round(a.b,3)) + coalesce(Charges_losses_customer_unbilled,0) FROM
              (SELECT
              CASE WHEN (tr.balancing_line = 1 or tr.after_indication_accounting = 0) THEN coef
              	ELSE rcl.coefficient END *
              round(rcl.value,3) /
                  CASE WHEN (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id) = 0
                  THEN 1
                  ELSE (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id)
                  END b
              from temp_results tr
              join rul_consumption_losses rcl
              on tr.line_id = rcl.line_id
              and tr.accounting_type_node_id = rcl.accounting_type_node_id
              AND rcl.connection_id IS NOT NULL
              AND (SELECT losses_policy_id FROM rul_connection WHERE connection_id = rcl.connection_id) = 2
              and tr.start_date <= rcl.end_date
              and tr.end_date >= rcl.start_date
              ) a)
              ;
            Losses_customer_billed := (select sum(round(a.b,3)) + coalesce(Charges_losses_customer_billed,0) FROM
              (SELECT
              CASE WHEN (tr.balancing_line = 1 or tr.after_indication_accounting = 0) THEN coef
              	ELSE rcl.coefficient END *
              round(rcl.value,3) /
                  CASE WHEN (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id) = 0
                  THEN 1
                  ELSE (SELECT COUNT(connection_id) FROM temp_results tr2 WHERE tr2.accounting_type_node_id = tr.accounting_type_node_id)
                  END b
              from temp_results tr
              join rul_consumption_losses rcl
              on tr.line_id = rcl.line_id
              and tr.accounting_type_node_id = rcl.accounting_type_node_id
              AND rcl.connection_id IS NOT NULL
              AND (SELECT losses_policy_id FROM rul_connection WHERE connection_id = rcl.connection_id) = 1
              AND tr.start_date <= rcl.end_date
              AND tr.end_date >= rcl.start_date
              ) a)
              ;
              Realize_consumption := (select sum(round(coalesce(val,0),3) *
                                            (CASE WHEN (tr.accounting_type_id = 17 OR tr.level = 0)
                                                AND tr.resource_balance_attitude_id = 3
                                                AND coef < 1
                                            THEN coef ELSE 1 END))
                                            + coalesce(Losses_customer_billed,0)
                                            + coalesce(Charges_realized,0)
                                            from temp_results tr);
        END IF;
        IF v_balancing_id IS NOT NULL
        THEN
        	INSERT INTO rul_balancing (balancing_id,node_calculate_parameter_id, start_date, end_date, balancing_indication, balancing_coefficient,
         		sum_losses_balance,realize_consumption,sum_losses_unbilled)
        	SELECT
              v_balancing_id,
              p_node_calculate_parameter_id,
              p_start_date,
              p_end_date,
              PY_SUM,
              coef,
              COALESCE(Losses_supplier,0) + COALESCE(Losses_customer_unbilled,0),
              Realize_consumption,
              COALESCE(Losses_customer_unbilled,0)
              ;
        ELSE
          -- Заполняем балансировку
          INSERT INTO rul_balancing (node_calculate_parameter_id, start_date, end_date, balancing_indication, balancing_coefficient,
           sum_losses_balance,realize_consumption,sum_losses_unbilled)
          SELECT
              p_node_calculate_parameter_id,
              p_start_date,
              p_end_date,
              PY_SUM,
              coef,
              COALESCE(Losses_supplier,0) + COALESCE(Losses_customer_unbilled,0),
              Realize_consumption,
              COALESCE(Losses_customer_unbilled,0)
          RETURNING balancing_id INTO v_balancing_id;
        END IF;
        RAISE NOTICE 'COEFICIENT %',coef;
        UPDATE public.rul_consumption_load
        SET balancing_coefficient = CASE WHEN (tr.accounting_type_id = 17 OR tr.level = 0) AND tr.resource_balance_attitude_id = 3 AND coef < 1
            THEN coef ELSE 1 END ,
        balancing_id = v_balancing_id
        FROM temp_results tr
        WHERE rul_consumption_load.connection_id = tr.connection_id
            AND rul_consumption_load.start_date = tr.start_date
            AND rul_consumption_load.accounting_type_node_id = tr.accounting_type_node_id
            AND tr.source_consumption_id = 1
            AND rul_consumption_load.theoretical_calculation = false
            AND NOT EXISTS (
            SELECT 1
            FROM rul_charge rc
            WHERE rc.connection_id = tr.connection_id
              AND rc.charge_checked = 0
        );
        WITH conditions AS (
            SELECT
                tr.*,
                CASE WHEN (tr.accounting_type_id = 17 OR tr.level = 0) AND tr.resource_balance_attitude_id = 3 AND coef < 1
                THEN coef ELSE 1 END AS finally_coef
            FROM temp_results tr
        )
        UPDATE public.rul_charge
        SET
            balancing_coefficient = CASE WHEN rul_charge.source_id = 2 THEN coef ELSE c.finally_coef END,
            balancing_id = v_balancing_id,
            sum_consumption = ROUND(rul_charge.sum_consumption * CASE WHEN rul_charge.source_id = 2 THEN coef ELSE c.finally_coef END, 3),
            amount = ROUND(ROUND(rul_charge.sum_consumption * CASE WHEN rul_charge.source_id = 2 THEN coef ELSE c.finally_coef END, 3) * rul_charge.base_value, 2),
            nds_rub = ROUND(ROUND(ROUND(rul_charge.sum_consumption * CASE WHEN rul_charge.source_id = 2 THEN coef ELSE c.finally_coef END, 3) * rul_charge.base_value, 2) * rul_charge.nds_percent / 100, 2),
            amount_nds = ROUND(ROUND(rul_charge.sum_consumption * CASE WHEN rul_charge.source_id = 2 THEN coef ELSE c.finally_coef END, 3) * rul_charge.base_value, 2) + ROUND(ROUND(ROUND(rul_charge.sum_consumption * CASE WHEN rul_charge.source_id = 2 THEN coef ELSE c.finally_coef END, 3) * rul_charge.base_value, 2) * rul_charge.nds_percent / 100, 2)
        FROM conditions c
        WHERE rul_charge.connection_id = c.connection_id
          -- Только для неподтвержденных начислений
          -- Ручные начисления тоже не трогаем, Но это все должно было быть учтено при построении дерева.
          AND rul_charge.start_date >= p_start_date
          AND rul_charge.end_date <= p_end_date
          AND rul_charge.invoice_id IS NULL;
        /*
        DELETE FROM public.rul_charge WHERE connection_id IN
        	(SELECT DISTINCT connection_id FROM temp_results WHERE NOT EXISTS
            	(
                  SELECT 1
                  FROM rul_charge rc
                  WHERE rc.connection_id = temp_results.connection_id
                    AND rc.charge_checked = 0
                )
        	)
        AND start_date >= p_start_date
        AND end_date <= p_end_date;
        DELETE FROM public.rul_charge_detail WHERE connection_id IN
        	(SELECT DISTINCT connection_id FROM temp_results WHERE NOT EXISTS
            	(
                  SELECT 1
                  FROM rul_charge rc
                  WHERE rc.connection_id = temp_results.connection_id
                    AND rc.charge_checked = 0
                )
        	)
        AND start_date >= p_start_date
        AND end_date <= p_end_date;
        */
        --CALL public.process_charges_load(null::bigint,p_start_date,p_end_date,1::smallint);
        UPDATE public.rul_consumption_standard
        SET balancing_coefficient = CASE
            WHEN (tr.accounting_type_id = 17 OR tr.level = 0)
                 AND tr.resource_balance_attitude_id = 3
                 AND coef < 1
            THEN coef
            ELSE 1
        END,
        balancing_id = v_balancing_id
        FROM temp_results tr
        WHERE rul_consumption_standard.connection_id = tr.connection_id
            AND rul_consumption_standard.start_date = tr.start_date
            AND rul_consumption_standard.accounting_type_node_id = tr.accounting_type_node_id
            AND tr.source_consumption_id = 2
            AND rul_consumption_standard.theoretical_calculation = false
            AND NOT EXISTS
            	(
                  SELECT 1
                  FROM rul_charge rc
                  WHERE rc.connection_id = tr.connection_id
                    AND rc.charge_checked = 0
                );
        UPDATE public.rul_consumption_source_connection
        SET balancing_coefficient = CASE
            WHEN (tr.accounting_type_id = 17 OR tr.level = 0)
                 AND tr.resource_balance_attitude_id = 3
                 AND coef < 1
            THEN coef
            ELSE 1
        END,
        balancing_id = v_balancing_id
        FROM temp_results tr
        WHERE rul_consumption_source_connection.connection_id = tr.connection_id
            AND rul_consumption_source_connection.start_date = tr.start_date
            AND rul_consumption_source_connection.accounting_type_node_id = tr.accounting_type_node_id
            AND tr.source_consumption_id = 4
            AND rul_consumption_source_connection.theoretical_calculation = false
            AND NOT EXISTS
            	(
                  SELECT 1
                  FROM rul_charge rc
                  WHERE rc.connection_id = tr.connection_id
                    AND rc.charge_checked = 0
                );
        UPDATE public.rul_consumption_losses
        SET balancing_coefficient = coef,
        balancing_id = v_balancing_id
        WHERE line_id in (select distinct line_id from temp_results where (balancing_line = 1 or after_indication_accounting = 0))
        and start_date >= p_start_date
        and end_date <= p_end_date;
        drop table temp_results;
        -- Ваша логика обработки здесь
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_charges(IN p_agreement_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_charges(IN p_agreement_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_ids BIGINT;
BEGIN
  -- Формирование расходов по приборному учету.
      -- На данный момент просто формирует расходы исходя из показаний и формул.
      CALL public.process_consumption_data(p_start_date, p_end_date);
      CALL public.process_formuls_indication(p_start_date, p_end_date);
	FOREACH v_ids IN ARRAY p_agreement_ids
    LOOP
      insert into rul_debug_log (debug_user_id,debug_module)
      values (current_setting('user_ctx.user_name',true),'Пересчет');
      -- Формирование теоретических нагрузочных расходов. Фактические будут считаться в ГПУ.
      --CALL public.process_formuls_load(v_ids,p_start_date,p_end_date);
      -- Формирование теоретических расходов для способа учета по норме.
      -- Использует для своих расчетов метод, а не формулу. Также вместо id параметров ссылается на имена параметров
      --CALL public.process_formuls_standard(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по подключению-источнику.
      --CALL public.process_formuls_source_connection(v_ids,p_start_date,p_end_date);
      -- Формирование расходов и начислений для способа учета по сечению.
      --CALL public.process_formuls_pipe(v_ids,p_start_date,p_end_date);
      -- Расчет по ГПУ. Должен построить деревья в рамках договора. С нижних листов рассчитать Учетные способы учета.
      -- Затем произвести для каждого узла ГПУ.
      -- Так же должен обойти отдельные узлы, чтобы в них расчить начисления
      --CALL public.process_charges_new(v_ids,p_start_date,p_end_date);
      --Начисление по потерям надо создавать после балансировки
      --CALL public.process_charges_losses(v_ids,p_start_date,p_end_date);
      --CALL public.process_charges_planned_consumption(v_ids,p_start_date,p_end_date);
      --CALL public.process_zero_charges(v_ids,p_start_date,p_end_date);
    END LOOP;
    CALL public.new_process_group_accounting_new(p_agreement_ids, p_start_date, p_end_date);
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_charges_load(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_mode smallint)
CREATE OR REPLACE PROCEDURE public.process_charges_load(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_mode smallint DEFAULT 0)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	IF p_mode = 0 THEN
        INSERT INTO
              public.rul_charge
            (
              connection_id,
              sum_consumption,
              amount,
              nds_percent,
              note,
              start_date,
              end_date,
              base_value,
              billing_start_date,
              billing_end_date,
              charge_type_id,
              nds_rub,
              amount_nds,
              cost_factor,
        	  currency_rate,
              comitet_resolution,
              invoice_group_index
            )
        SELECT
            connection_id,
            ROUND(consumption,3),
            ROUND(ROUND(consumption,3) * rrv.base_value,2),
            rrv.nds,
            note,
            rcd.start_date,
            rcd.end_date,
            rrv.base_value,
            p_start_date,
            p_end_date,
            1,
            ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            rrv.cost_factor,
        	rrv.currency_rate,
            rrv.comitet_resolution,
            rcd.invoice_group_index
        FROM (
              select
                  connection_id,
                  sum(consumption) as consumption,
                  string_agg(coalesce(note,' '), ' - ') as note,
                  rate_value_id,
                  min(start_date) as start_date,
                  max(end_date) as end_date,
                  invoice_group_index
              from (
                  	WITH calc_charges AS (
                    SELECT
                        rc.connection_id,
                        SUM(rcc.value * rcc.coefficient) AS consumption,
                        MIN(rcc.start_date) AS start_date,
                        MAX(rcc.end_date) AS end_date,
                        rc.rate_id,
                        rcc.accounting_type_node_id,
                        string_agg(rcc.description, ' - ') as description,
                        rc.invoice_group_index
                    FROM rul_consumption_load rcc
                    JOIN rul_connection rc
                        ON rc.connection_id = rcc.connection_id
                        AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
                    WHERE rcc.theoretical_calculation is false -- Надо поставить false, пока заглушка
                    and rcc.start_date >= p_start_date
                    and rcc.end_date <= p_end_date
                    GROUP BY rc.connection_id, rc.rate_id, rcc.accounting_type_node_id,rc.invoice_group_index
                    ),
                    time_calcs AS (
                        SELECT
                            charges.connection_id,
                            charges.consumption,
                            rrv.start_date AS rate_start,
                            rrv.end_date AS rate_end,
                            LEAST(COALESCE(rrv.end_date, charges.end_date),
                                         charges.end_date) AS period_end,
                            GREATEST(rrv.start_date, charges.start_date) AS period_start,
                            EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                            charges.accounting_type_node_id,
                            rrv.rate_value_id, -- Добавляем rate_value_id сюда
                            charges.description,
                            charges.invoice_group_index
                        FROM calc_charges charges
                        JOIN rul_rate_value rrv
                            ON charges.rate_id = rrv.rate_id
                            AND ((charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                            AND charges.end_date > rrv.start_date) OR
                             (rrv.start_date BETWEEN charges.start_date AND charges.end_date
                                OR COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                                    BETWEEN charges.start_date AND charges.end_date))
                    )
                    SELECT
                        t.accounting_type_node_id,
                        t.period_start as start_date,
                        t.period_end as end_date,
                        ROUND(CASE
                            WHEN t.total_days = 0 THEN 0
                            ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                        END,3) AS consumption,
                        t.rate_value_id,
                        t.connection_id,
                        t.description as note,
                        t.invoice_group_index
                    FROM time_calcs t
                    ) as rul_charge_detail
            where 1=1
            and start_date >= p_start_date
            and end_date <= p_end_date
            AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
            group by connection_id,rate_value_id,accounting_type_node_id,invoice_group_index
            ) rcd
        JOIN
            rul_rate_value rrv
        ON
            rcd.rate_value_id = rrv.rate_value_id;
    ELSE
    	INSERT INTO
          public.rul_charge_detail
        (
          accounting_type_node_id,
          start_date,
          end_date,
          consumption,
          note2,
          rate_value_id,
          connection_id,
          charge_id,
          note
        )
         WITH calc_charges AS (
        SELECT
            rc.connection_id,
            SUM(rcc.value * coalesce(rcc.balancing_coefficient,rcc.coefficient) ) AS consumption,
            MIN(rcc.start_date) AS start_date,
            MAX(rcc.end_date) AS end_date,
            rc.rate_id,
            'Нагрузочный учет' AS comment,
            rcc.accounting_type_node_id,
            string_agg(rcc.description, ' - ') as description
        FROM rul_consumption_load rcc
        JOIN rul_connection rc
            ON rc.connection_id = rcc.connection_id
            AND rc.connection_id IN (select distinct connection_id FROM temp_results)
        WHERE rcc.theoretical_calculation is false -- Надо поставить false, пока заглушка
        and rcc.start_date >= p_start_date
        and rcc.end_date <= p_end_date
        GROUP BY rc.connection_id, rc.rate_id, rcc.accounting_type_node_id
        ),
        time_calcs AS (
            SELECT
                charges.connection_id,
                charges.consumption,
                charges.comment,
                rrv.start_date AS rate_start,
                rrv.end_date AS rate_end,
                LEAST(COALESCE(rrv.end_date, charges.end_date),
                             charges.end_date) AS period_end,
                GREATEST(rrv.start_date, charges.start_date) AS period_start,
                EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                charges.accounting_type_node_id,
                rrv.rate_value_id, -- Добавляем rate_value_id сюда
                charges.description
            FROM calc_charges charges
            JOIN rul_rate_value rrv
                ON charges.rate_id = rrv.rate_id
                AND ((charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                AND charges.end_date > rrv.start_date) OR
                 (rrv.start_date BETWEEN charges.start_date AND charges.end_date
                    OR COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                        BETWEEN charges.start_date AND charges.end_date))
        ),
        unique_groups AS (
            -- Генерируем уникальные ID для каждой комбинации connection_id и rate_value_id
            SELECT DISTINCT
                connection_id,
                rate_value_id,
                accounting_type_node_id,
                nextval('rul_charge_charge_id_seq') AS charge_id
            FROM time_calcs
        )
        SELECT
            t.accounting_type_node_id,
            t.period_start,
            t.period_end,
            ROUND(CASE
                WHEN t.total_days = 0 THEN 0
                ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
            END,3) AS weighted_consumption,
            t.comment,
            t.rate_value_id,
            t.connection_id,
            ug.charge_id,  -- Используем ID из последовательности вместо ROW_NUMBER()
            t.description
        FROM time_calcs t
        JOIN unique_groups ug
            ON t.connection_id = ug.connection_id
            AND t.rate_value_id = ug.rate_value_id
            AND t.accounting_type_node_id = ug.accounting_type_node_id;
        --delete from rul_charge where charge_type_id != 2;
        INSERT INTO
              public.rul_charge
            (
              charge_id,
              connection_id,
              sum_consumption,
              amount,
              nds_percent,
              note,
              start_date,
              end_date,
              base_value,
              billing_start_date,
              billing_end_date,
              charge_type_id,
              nds_rub,
              amount_nds,
              cost_factor,
        	  currency_rate,
              comitet_resolution
            )
        SELECT
            charge_id,
            connection_id,
            ROUND(consumption,3),
            ROUND(ROUND(consumption,3) * rrv.base_value,2),
            rrv.nds,
            note,
            rcd.start_date,
            rcd.end_date,
            rrv.base_value,
            p_start_date,
            p_end_date,
            1,
            ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            rrv.cost_factor,
        	rrv.currency_rate,
            rrv.comitet_resolution
        FROM (
              select
                  charge_id,
                  connection_id,
                  sum(consumption) as consumption,
                  string_agg(note, ' - ') as note,
                  rate_value_id,
                  min(start_date) as start_date,
                  max(end_date) as end_date
              from rul_charge_detail
              where note2 = 'Нагрузочный учет'
              and start_date >= p_start_date
              and end_date <= p_end_date
              AND connection_id IN (select distinct connection_id FROM temp_results)
              group by charge_id,
                  connection_id,
                  rate_value_id,
                  accounting_type_node_id
            ) rcd
        JOIN
            rul_rate_value rrv
        ON
            rcd.rate_value_id = rrv.rate_value_id;
    END IF;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_charges_losses(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_charges_losses(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
   /* INSERT INTO
      public.rul_charge_detail
    (
      --accounting_type_node_id,
      start_date,
      end_date,
      consumption,
      note,
      rate_value_id,
      connection_id,
      charge_id
    )
     WITH calc_charges AS (
    SELECT
        rc.connection_id,
        SUM(rcc.value*rcc.coefficient) AS consumption,
        MIN(rcc.start_date) AS start_date,
        MAX(rcc.end_date) AS end_date,
        rc.rate_id,
        'Потери' AS comment
        --,rcc.accounting_type_node_id
    FROM rul_consumption_losses rcc
    JOIN rul_connection rc
        ON rc.connection_id = rcc.connection_id
        AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
    WHERE rcc.theoretical_calculation is false -- Надо поставить false, пока заглушка
    and rcc.start_date >= p_start_date
    and rcc.end_date <= p_end_date
    GROUP BY rc.connection_id, rc.rate_id--, rcc.accounting_type_node_id
    ),
    time_calcs AS (
        SELECT
            charges.connection_id,
            charges.consumption,
            charges.comment,
            rrv.start_date AS rate_start,
            rrv.end_date AS rate_end,
            LEAST(COALESCE(rrv.end_date, charges.end_date),
            			 charges.end_date) AS period_end,
            GREATEST(rrv.start_date, charges.start_date) AS period_start,
            EXTRACT(EPOCH FROM charges.end_date - charges.start_date) / 86400 AS total_days,
            --charges.accounting_type_node_id,
            rrv.rate_value_id -- Добавляем rate_value_id сюда
        FROM calc_charges charges
        JOIN rul_rate_value rrv
            ON charges.rate_id = rrv.rate_id
            AND ((charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
            AND charges.end_date > rrv.start_date) OR
             (rrv.start_date BETWEEN charges.start_date AND charges.end_date
                OR COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                    BETWEEN charges.start_date AND charges.end_date))
    ),
    unique_groups AS (
        -- Генерируем уникальные ID для каждой комбинации connection_id и rate_value_id
        SELECT DISTINCT
            connection_id,
            rate_value_id,
            --accounting_type_node_id,
            nextval('rul_charge_charge_id_seq') AS charge_id
        FROM time_calcs
    )
    SELECT
        --t.accounting_type_node_id,
        t.period_start,
        t.period_end,
        ROUND(CASE
            WHEN t.total_days = 0 THEN 0
            ELSE t.consumption * (EXTRACT(EPOCH FROM t.period_end - t.period_start) / 86400) / t.total_days
        END,3) AS weighted_consumption,
        t.comment,
        t.rate_value_id,
        t.connection_id,
        ug.charge_id  -- Используем ID из последовательности вместо ROW_NUMBER()
    FROM time_calcs t
    JOIN unique_groups ug
        ON t.connection_id = ug.connection_id
        AND t.rate_value_id = ug.rate_value_id
        --AND t.accounting_type_node_id = ug.accounting_type_node_id
        ;
    --delete from rul_charge where charge_type_id != 2;
    INSERT INTO
          public.rul_charge
        (
          charge_id,
          connection_id,
          sum_consumption,
          amount,
          nds_percent,
          note,
          start_date,
          end_date,
          base_value,
          billing_start_date,
          billing_end_date,
          charge_type_id,
          nds_rub,
          amount_nds,
          source_id,
          cost_factor,
          currency_rate,
          comitet_resolution
        )
    SELECT
     	charge_id,
        connection_id,
        ROUND(consumption,3),
        ROUND(ROUND(consumption,3) * rrv.base_value,2),
        rrv.nds,
        note,
        rcd.start_date,
        rcd.end_date,
        rrv.base_value,
        p_start_date,
        p_end_date,
        1,
        ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
        ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
        2, -- Источник потери
        rrv.cost_factor,
        rrv.currency_rate,
        rrv.comitet_resolution
    FROM (
          select
              charge_id,
              connection_id,
              sum(consumption) as consumption,
              string_agg(note, ' - ') as note,
              rate_value_id,
              min(start_date) as start_date,
              max(end_date) as end_date
          from rul_charge_detail
          where note = 'Потери'
          and start_date >= p_start_date
          and end_date <= p_end_date
          AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
          group by charge_id,
              connection_id,
              rate_value_id
              --,accounting_type_node_id
        ) rcd
    JOIN
        rul_rate_value rrv
    ON
        rcd.rate_value_id = rrv.rate_value_id;*/
    INSERT INTO
      public.rul_charge
    (
      connection_id,
      sum_consumption,
      amount,
      nds_percent,
      note,
      start_date,
      end_date,
      base_value,
      billing_start_date,
      billing_end_date,
      charge_type_id,
      nds_rub,
      amount_nds,
      cost_factor,
      currency_rate,
      comitet_resolution,
      source_id,
      invoice_group_index
    )
    SELECT
        connection_id,
        ROUND(consumption,3),
        ROUND(ROUND(consumption,3) * rrv.base_value,2),
        rrv.nds,
        note,
        rcd.start_date,
        rcd.end_date,
        rrv.base_value,
        p_start_date,
        p_end_date,
        1,
        ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
        ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
        rrv.cost_factor,
        rrv.currency_rate,
        rrv.comitet_resolution,
        2,
        rcd.invoice_group_index
    FROM (
          select
              connection_id,
              sum(consumption) as consumption,
              string_agg(note, ';') as note,
              rate_value_id,
              min(start_date) as start_date,
              max(end_date) as end_date,
              invoice_group_index
          from (
                WITH calc_charges AS (
                SELECT
                    rc.connection_id,
                    SUM(rcc.value*rcc.coefficient) AS consumption,
                    MIN(rcc.start_date) AS start_date,
                    MAX(rcc.end_date) AS end_date,
                    rc.rate_id,
                    string_agg(rcc.note, E'\r\n' order by rcc.start_date) as note
                    --,rcc.accounting_type_node_id
                    ,rc.invoice_group_index
                FROM rul_consumption_losses rcc
                JOIN rul_connection rc
                    ON rc.connection_id = rcc.connection_id
                    AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
                    						 AND invoice_group_index IS NOT NULL)
                WHERE rcc.theoretical_calculation is false -- Надо поставить false, пока заглушка
                and rcc.start_date >= p_start_date
                and rcc.end_date <= p_end_date
                GROUP BY rc.connection_id, rc.rate_id, rc.invoice_group_index
                ),
                time_calcs AS (
                    SELECT
                        charges.connection_id,
                        charges.consumption,
                        rrv.start_date AS rate_start,
                        rrv.end_date AS rate_end,
                        LEAST(COALESCE(rrv.end_date, charges.end_date),
                                     charges.end_date) AS period_end,
                        GREATEST(rrv.start_date, charges.start_date) AS period_start,
                        EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                        rrv.rate_value_id, -- Добавляем rate_value_id сюда
                        charges.note,
                        charges.invoice_group_index
                    FROM calc_charges charges
                    JOIN rul_rate_value rrv
                        ON charges.rate_id = rrv.rate_id
                        AND charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                        AND charges.end_date >= rrv.start_date
                )
                SELECT
                    --t.accounting_type_node_id,
                    t.period_start as start_date,
                    t.period_end as end_date,
                    ROUND(CASE
                        WHEN t.total_days = 0 THEN 0
                        ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                    END,3) AS consumption,
                    t.rate_value_id,
                    t.connection_id,
                    t.note,
                    t.invoice_group_index
                FROM time_calcs t
                ) as rul_charge_detail
        where 1=1
        and start_date >= p_start_date
        and end_date <= p_end_date
        AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
        group by connection_id,rate_value_id,invoice_group_index
        ) rcd
    JOIN
        rul_rate_value rrv
    ON
        rcd.rate_value_id = rrv.rate_value_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_charges_new(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_charges_new(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_connection_ids BIGINT[];
BEGIN
	SELECT array_agg(connection_id) INTO v_connection_ids FROM (
    SELECT DISTINCT connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      								AND billing_start_date >= p_start_date
									AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL) conn;
	-- Собирает начисления по группе таблиц, а не по каждой отдельно
    INSERT INTO
      public.rul_charge
    (
      connection_id,
      sum_consumption,
      amount,
      nds_percent,
      note,
      start_date,
      end_date,
      base_value,
      billing_start_date,
      billing_end_date,
      charge_type_id,
      nds_rub,
      amount_nds,
      cost_factor,
      currency_rate,
      comitet_resolution,
      invoice_group_index
    )
    SELECT
        connection_id,
        ROUND(consumption::numeric,3),
        ROUND((ROUND(consumption::numeric,3) * rrv.base_value)::numeric,2),
        rrv.nds,
        note,
        rcd.start_date,
        rcd.end_date,
        rrv.base_value,
        p_start_date,
        p_end_date,
        1,
        ROUND((ROUND((consumption * rrv.base_value)::numeric ,2) * rrv.nds / 100)::numeric,2),
        ROUND((consumption * rrv.base_value)::numeric,2) + ROUND((ROUND((consumption * rrv.base_value)::numeric,2) * rrv.nds::numeric / 100)::numeric,2),
        rrv.cost_factor,
        rrv.currency_rate,
        rrv.comitet_resolution,
        rcd.invoice_group_index
    FROM (
          select
              connection_id,
              sum(consumption) as consumption,
              string_agg(coalesce(note,' '), ';') as note,
              rate_value_id,
              min(start_date) as start_date,
              max(end_date) as end_date,
              invoice_group_index
          from (
                WITH calc_charges AS (
                SELECT
                    rc.connection_id,
                    SUM(rcc.value * rcc.coefficient) AS consumption,
                    MIN(rcc.start_date) AS start_date,
                    MAX(rcc.end_date) AS end_date,
                    rc.rate_id,
                    rcc.accounting_type_node_id,
                    string_agg(rcc.note, E'\r\n' order by rcc.start_date) as note,
                    rc.invoice_group_index
                FROM (
                      SELECT rcl.connection_id,rcl.value,rcl.coefficient,rcl.start_date,rcl.end_date,
                        rcl.accounting_type_node_id,rcl.note,rcl.theoretical_calculation
                      FROM rul_consumption_load rcl
                      UNION ALL
                      SELECT rcs.connection_id,rcs.value,rcs.coefficient,rcs.start_date,rcs.end_date,
                              rcs.accounting_type_node_id,rcs.note,rcs.theoretical_calculation
                      FROM rul_consumption_standard rcs
                      UNION ALL
                      SELECT rcsс.connection_id,rcsс.value,rcsс.coefficient,rcsс.start_date,rcsс.end_date,
                              rcsс.accounting_type_node_id,rcsс.note,rcsс.theoretical_calculation
                      FROM rul_consumption_source_connection rcsс
                      ) rcc
                JOIN rul_connection rc
                    ON rc.connection_id = rcc.connection_id
                    --AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
                    AND rc.connection_id = ANY (v_connection_ids)
                WHERE rcc.theoretical_calculation is false -- Надо поставить false, пока заглушка
                and rcc.start_date >= p_start_date
                and rcc.end_date <= p_end_date
                GROUP BY rc.connection_id, rc.rate_id, rcc.accounting_type_node_id, rc.invoice_group_index
                ),
                time_calcs AS (
                    SELECT
                        charges.connection_id,
                        charges.consumption,
                        rrv.start_date AS rate_start,
                        rrv.end_date AS rate_end,
                        LEAST(COALESCE(rrv.end_date, charges.end_date),charges.end_date) AS period_end,
                        GREATEST(rrv.start_date, charges.start_date) AS period_start,
                        EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                        charges.accounting_type_node_id,
                        rrv.rate_value_id, -- Добавляем rate_value_id сюда
                        charges.note,
                        charges.invoice_group_index
                    FROM calc_charges charges
                    JOIN rul_rate_value rrv
                        ON charges.rate_id = rrv.rate_id
                        AND charges.start_date <= COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                        AND COALESCE(charges.end_date, '2100-01-01 00:00:00+03'::timestamp) > rrv.start_date
                )
                SELECT
                    t.accounting_type_node_id,
                    t.period_start as start_date,
                    t.period_end as end_date,
                    ROUND(CASE
                        WHEN t.total_days = 0 THEN t.consumption
                        ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                    END,3) AS consumption,
                    t.rate_value_id,
                    t.connection_id,
                    t.note,
                    t.invoice_group_index
                FROM time_calcs t
                ) as rul_charge_detail
        WHERE 1=1
        AND start_date >= p_start_date
        AND end_date <= p_end_date
        --AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
        AND connection_id = ANY (v_connection_ids)
        GROUP BY connection_id,rate_value_id,accounting_type_node_id,invoice_group_index
        ) rcd
    JOIN
        rul_rate_value rrv
    ON
        rcd.rate_value_id = rrv.rate_value_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_charges_planned_consumption(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_charges_planned_consumption(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
        INSERT INTO
              public.rul_charge
            (
              connection_id,
              sum_consumption,
              amount,
              nds_percent,
              note,
              start_date,
              end_date,
              base_value,
              billing_start_date,
              billing_end_date,
              charge_type_id,
              nds_rub,
              amount_nds,
              cost_factor,
        	  currency_rate,
              comitet_resolution,
              source_id,
              invoice_group_index
            )
        SELECT
            connection_id,
            ROUND(consumption,3),
            ROUND(ROUND(consumption,3) * rrv.base_value,2),
            rrv.nds,
            note,
            rcd.end_date, -- Делаем, чтобы день начала и конца совпадал
            rcd.end_date,
            rrv.base_value,
            p_start_date,
            p_end_date,
            1,
            ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            rrv.cost_factor,
        	rrv.currency_rate,
            rrv.comitet_resolution,
            4,
            invoice_group_index
        FROM (
              select
                  connection_id,
                  sum(consumption) as consumption,
                  string_agg(note, ' - ') as note,
                  rate_value_id,
                  min(start_date) as start_date,
                  max(end_date) as end_date,
                  invoice_group_index
              from (
                  	WITH calc_charges AS (
                    SELECT
                        rc.connection_id,
                        SUM(rcc.planned_consumption_value * rcc.advance_payment_percent / 100) AS consumption,
                        GREATEST(MAX(rcc.start_date),MAX(ratn.start_date)) AS start_date,
                        MIN(rcc.payment_date) AS end_date,
                        rc.rate_id,
                        ratn.accounting_type_node_id,
                        string_agg(rcc.description, ' - ') as description,
                        rcc.planned_consumption_id,
                        rc.invoice_group_index
                    FROM rul_planned_consumption rcc
                    JOIN rul_connection rc
                        ON rc.connection_id = rcc.connection_id
                        AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
                        						 AND invoice_group_index IS NOT NULL)
                    JOIN rul_accounting_type_node ratn
                    	ON rc.node_calculate_parameter_id = ratn.node_calculate_parameter_id
                        AND ratn.start_date <= rcc.payment_date
                        AND COALESCE(ratn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= rcc.payment_date
                    LEFT JOIN rul_charge rch
                    	ON rch.connection_id = rc.connection_id
                        AND rch.start_date = rcc.payment_date
                    WHERE 1=1 -- Надо поставить false, пока заглушка
                    and rcc.start_date >= p_start_date
                    and rcc.end_date <= p_end_date
                    and rch.charge_id is null
                    GROUP BY rc.connection_id, rc.rate_id, ratn.accounting_type_node_id, rcc.planned_consumption_id, rc.invoice_group_index
                    ),
                    time_calcs AS (
                        SELECT
                            charges.connection_id,
                            charges.consumption,
                            rrv.start_date AS rate_start,
                            rrv.end_date AS rate_end,
                            LEAST(COALESCE(rrv.end_date, charges.end_date),
                                         charges.end_date) AS period_end,
                            GREATEST(rrv.start_date, charges.start_date) AS period_start,
                            EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                            charges.accounting_type_node_id,
                            rrv.rate_value_id, -- Добавляем rate_value_id сюда
                            charges.description,
                            charges.planned_consumption_id,
                            charges.invoice_group_index
                        FROM calc_charges charges
                        JOIN rul_rate_value rrv
                            ON charges.rate_id = rrv.rate_id
                            AND ((charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                            AND charges.end_date > rrv.start_date) OR
                             (rrv.start_date BETWEEN charges.start_date AND charges.end_date
                                OR COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                                    BETWEEN charges.start_date AND charges.end_date))
                    )
                    SELECT
                        t.accounting_type_node_id,
                        t.period_start as start_date,
                        t.period_end as end_date,
                        ROUND(CASE
                            WHEN t.total_days = 0 THEN 0
                            ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                        END,3) AS consumption,
                        t.rate_value_id,
                        t.connection_id,
                        t.description as note,
                        t.planned_consumption_id,
                        t.invoice_group_index
                    FROM time_calcs t
                    ) as rul_charge_detail
            where 1=1
            and start_date >= p_start_date
            and end_date <= p_end_date
            AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
            group by connection_id,rate_value_id,accounting_type_node_id,planned_consumption_id,invoice_group_index
            ) rcd
        JOIN
            rul_rate_value rrv
        ON
            rcd.rate_value_id = rrv.rate_value_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_consumption_data(IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_consumption_data(IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- 1) Очистка и заполнение rul_last_month_node_panel_value
    --для получения последних показаний предыдущего месяца
    DELETE FROM rul_last_month_node_panel_value; -- Сейчас идет полноценная очистка всего при пересчете
    INSERT INTO rul_last_month_node_panel_value
    SELECT a.node_panel_value_id, a.value_number, a.check_date, a.check_type_id, a.node_panel_id,
           a.op_user_id, a.op_date, a.deleted, a.is_correct, a.changed_user_id
    FROM (
        SELECT nd.node_panel_value_id, nd.value_number,
               DATE_TRUNC('month', p_end_date) - INTERVAL '1 second' AS check_date,
               nd.check_type_id, nd.node_panel_id, nd.op_user_id, nd.op_date,
               nd.deleted, nd.is_correct, nd.changed_user_id,
               LEAD(COALESCE(value_number, 0)) OVER (PARTITION BY nd.node_panel_id
                                                    ORDER BY nd.node_panel_id, nd.check_date) AS ld
        FROM rul_node_panel_value nd
        LEFT JOIN rul_node_panel np ON nd.node_panel_id = np.node_panel_id
        LEFT JOIN rul_panel p ON np.panel_id = p.panel_id
        WHERE check_date BETWEEN p_start_date - interval '1 month'
                            AND DATE_TRUNC('month', p_end_date) - INTERVAL '1 second'
    ) a
    WHERE a.ld IS NULL;
    -- 2) Очистка и заполнение rul_consumption_all
    DELETE FROM rul_consumption_all;
    INSERT INTO rul_consumption_all
    SELECT node_panel_id,
           CASE WHEN start_date < p_start_date THEN p_start_date ELSE start_date END,
           end_date, consumption, value_number
    FROM (
        SELECT nd.node_panel_id,
               COALESCE(
               LAG(check_date) OVER (PARTITION BY nd.node_panel_id
                                             ORDER BY nd.node_panel_id, nd.check_date)
                                             ,p_start_date
                        )
                        AS start_date,
               check_date AS end_date,
               CASE
                   WHEN p.indication_type_id = 2 THEN (COALESCE(value_number, 0) +
                        LAG(COALESCE(value_number, 0)) OVER (PARTITION BY nd.node_panel_id ORDER BY nd.node_panel_id, nd.check_date))/2
                   WHEN p.indication_type_id = 1 THEN COALESCE(value_number, 0) -
                        LAG(COALESCE(value_number, 0)) OVER (PARTITION BY nd.node_panel_id ORDER BY nd.node_panel_id, nd.check_date)
               END AS consumption,
               COALESCE(value_number, 0) AS value_number,
               nd.flag
        FROM (
            SELECT node_panel_value_id, value_number, check_date, check_type_id, node_panel_id,
                   op_user_id, op_date, deleted, is_correct, changed_user_id, 1 AS flag
            FROM rul_node_panel_value
            WHERE check_date BETWEEN p_start_date AND p_end_date
            AND is_correct = 1
            AND deleted = 0
            UNION ALL
            SELECT node_panel_value_id, value_number, check_date, check_type_id, node_panel_id,
                   op_user_id, op_date, deleted, is_correct, changed_user_id, 2 AS flag
            FROM rul_last_month_node_panel_value
            WHERE check_date BETWEEN p_start_date - interval '1 day' AND p_end_date
        ) nd
        LEFT JOIN rul_node_panel np ON nd.node_panel_id = np.node_panel_id
        LEFT JOIN rul_panel p ON np.panel_id = p.panel_id
        ORDER BY nd.flag, nd.node_panel_id, nd.check_date
    ) a
    WHERE flag = 1;
    -- 3) Очистка и заполнение rul_preconsumption
    -- Здесь формируются "расходики" по показаниям, т.е. формируется расход соседних показаний с интервалом дат
    DELETE FROM rul_preconsumption;
    INSERT INTO public.rul_preconsumption
    (
        accounting_type_node_id,
        start_date,
        end_date,
        consumption,
        node_panel_argument_id,
        value_number
    )
    SELECT ratn.accounting_type_node_id,
    date_trunc('day',min(rcd.start_date)),
     date_trunc('day',max(rcd.end_date))
     , sum(coalesce(rcd.consumption,0)),
    rnpa.node_panel_argument_id,
    MAX(rcd.value_number)
    FROM (
        SELECT node_calculate_parameter_id, start_date, accounting_type_id, accounting_type_node_id,
               COALESCE(end_date, p_end_date) AS end_date
        FROM rul_accounting_type_node
    ) ratn
    JOIN rul_node_calculate_parameter rncp
        ON ratn.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    JOIN rul_node_meter rnm
        ON rnm.node_id = rncp.node_id AND ratn.start_date >= rnm.start_date
    JOIN rul_node_panel rnp
        ON rnp.node_meter_id = rnm.node_meter_id
    JOIN rul_node_panel_argument rnpa
        ON ratn.accounting_type_node_id = rnpa.accounting_type_node_id
        AND rnpa.node_panel_id = rnp.node_panel_id
    JOIN rul_consumption_all rcd
        ON rcd.node_panel_id = rnp.node_panel_id
        AND ratn.start_date <= coalesce(rcd.start_date,p_start_date)
       	--AND ratn.end_date >= rcd.end_date
        AND ratn.end_date >= date_trunc('day',rcd.end_date)
    WHERE ratn.accounting_type_id = 2
    group by date_trunc('day', rcd.end_date),ratn.accounting_type_node_id,rnpa.node_panel_argument_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_formuls_average(IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_node_calculate_parameter_id bigint)
CREATE OR REPLACE PROCEDURE public.process_formuls_average(IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_node_calculate_parameter_id bigint)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    rec RECORD;
    v_overlap_start timestamp;
    v_overlap_end   timestamp;
    v_days          integer;
    v_average_value_id bigint;
    p_value numeric;
BEGIN
DELETE FROM rul_consumption_average rca
USING rul_accounting_type_node ratn
WHERE rca.accounting_type_node_id = ratn.accounting_type_node_id
  AND ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
  AND ratn.accounting_type_id = 5
  AND rca.start_date <= p_end_date
  AND rca.end_date   >= p_start_date;
FOR rec IN
        SELECT
            formula_id,
            accounting_type_node_id,
            start_date,
            COALESCE(end_date, timestamp '2100-01-01') AS end_date
        FROM rul_accounting_type_node
        WHERE node_calculate_parameter_id = p_node_calculate_parameter_id
          AND accounting_type_id = 5
          AND start_date <= p_end_date
          AND COALESCE(end_date, timestamp '2100-01-01') >= p_start_date
    LOOP
        v_overlap_start := GREATEST(rec.start_date, p_start_date);
        v_overlap_end   := LEAST(rec.end_date,   p_end_date);
        IF v_overlap_end < v_overlap_start THEN
            CONTINUE;
        END IF;
        v_days := extract(day from (v_overlap_end - v_overlap_start)) + 1;
        SELECT average_value_id
        INTO v_average_value_id
        FROM rul_average_value
        WHERE accounting_type_node_id = rec.accounting_type_node_id;
        IF v_average_value_id IS NULL THEN
            CONTINUE;
        END IF;
        CASE rec.formula_id
            WHEN 175 THEN
                p_value :=
                    get_average_value_argument(v_average_value_id, 666)
                    * v_days;
            WHEN 176 THEN
                p_value :=
                    get_average_value_argument(v_average_value_id, 668)
                    * v_days
                    * (
                        (get_average_value_argument(v_average_value_id, 670)
                         - get_average_value_argument(v_average_value_id, 671))
                        /
                        (get_average_value_argument(v_average_value_id, 670)
                         - get_average_value_argument(v_average_value_id, 672))
                      );
            WHEN 177 THEN
                p_value :=
                    get_average_value_argument(v_average_value_id, 673)
                    * v_days;
            WHEN 178 THEN
                p_value :=
                    get_average_value_argument(v_average_value_id, 675)
                    * v_days;
            WHEN 179 THEN
                p_value :=
                    get_average_value_argument(v_average_value_id, 677)
                    * v_days;
            ELSE
                CONTINUE;
        END CASE;
        INSERT INTO rul_consumption_average (
            start_date,
            end_date,
            value,
            accounting_type_node_id,
            node_calculate_parameter_id
        )
        VALUES (
            v_overlap_start,
            v_overlap_end,
            p_value,
            rec.accounting_type_node_id,
            p_node_calculate_parameter_id
        );
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_formuls_indication(IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_formuls_indication(IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Очистка таблицы rul_consumption перед вставкой (если требуется)
    DELETE FROM rul_consumption WHERE start_date >= p_start_date AND end_date <= p_end_date;
    -- С помощью формул формируются расходы. Принцип такой, что в формуле участвуют только те "расходики",
    -- которые подходят формуле и полность совпадают интервалами дат
    -- Вставка данных в rul_consumption V = V1 по формуле 1
    ----------------------------------------------------------------------------------------------
    INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value, note)
    select
         accounting_type_node_id,
         --case when rn = 1 then res.start_date else res.start_date + interval '1 day' end,
          case when rn = 1 then res.start_date else res.start_date end,
         end_date,
         value,
         avg,
         argument_formula_code||':'||value_number - consumption||'-'||value_number||' '||unit_name||';'
    from (
    SELECT
      accounting_type_node_id,
      start_date,
      end_date,
      value,
      unit_name,
      value /
      case when
          EXTRACT(EPOCH FROM end_date - start_date) = 0
          then 1
          else
          EXTRACT(EPOCH FROM end_date - start_date)
          end
      * 86400 as avg,
      row_number() over (partition by accounting_type_node_id,node_panel_argument_id order by end_date) as rn,
      value_number,
      get_serial_number(node_panel_id) as meter,
      argument_formula_code,
      consumption
    FROM get_indication_consumption(p_start_date,p_end_date,1,'V')
    ) res
    ;
    ----------------------------------------------------------------------------------------------
    -- Вставка данных в rul_consumption с вычитанием V = V1 - V2 по формуле 2
    INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value,  note)
    SELECT
        res.accounting_type_node_id,
        --case when rn = 1 then res.start_date else res.start_date + interval '1 day' end,
        case when rn = 1 then res.start_date else res.start_date end,
        res.end_date,
        res.value - res.value2 AS value,
        (res.value - res.value2) /
        case when
            	EXTRACT(EPOCH FROM res.end_date - res.start_date) = 0
                then 1
                else
                EXTRACT(EPOCH FROM res.end_date - res.start_date)
                end
         * 86400,
         res.afc1||':'||res.vn1 - res.c1||'-'||res.vn1||' '||res.unit_name||';'||res.afc2||':'||res.vn2 - res.c2||'-'||res.vn2||' '||res.unit_name||';'
    FROM (
    SELECT res1.start_date,res1.end_date,res1.value,res1.accounting_type_node_id,res2.value as value2,res1.unit_name,
    row_number() over (partition by res1.accounting_type_node_id,res1.node_panel_argument_id order by res1.end_date) as rn ,
    res1.value_number as vn1,res2.value_number as vn2,
    get_serial_number(res1.node_panel_id) as meter,
    res1.argument_formula_code afc1,
    res2.argument_formula_code afc2,
    res1.consumption as c1,
    res2.consumption as c2
    FROM get_indication_consumption(p_start_date,p_end_date,2,'V1') res1
    JOIN get_indication_consumption(p_start_date,p_end_date,2,'V2') res2
    	ON res1.start_date = res2.start_date
    	AND res1.end_date = res2.end_date
    	AND res1.accounting_type_node_id = res2.accounting_type_node_id
    ) res
    ;
    ----------------------------------------------------------------------------------------------
    -- Вставка данных в rul_consumption с вычитанием V = V2 + V1 по формуле 15
    INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value, note)
    SELECT
        res.accounting_type_node_id,
        --case when rn = 1 then res.start_date else res.start_date + interval '1 day' end,
        case when rn = 1 then res.start_date else res.start_date end,
        res.end_date,
        res.value + res.value2 AS value,
        (res.value + res.value2) /
        case when
            	EXTRACT(EPOCH FROM res.end_date - res.start_date) = 0
                then 1
                else
                EXTRACT(EPOCH FROM res.end_date - res.start_date)
                end
         * 86400,
         res.afc1||':'||res.vn1 - res.c1||'-'||res.vn1||' '||res.unit_name||';'||res.afc2||':'||res.vn2 - res.c2||'-'||res.vn2||' '||res.unit_name||';'
    FROM (
      SELECT res1.start_date,res1.end_date,res1.value,res1.accounting_type_node_id,res2.value as value2,res1.unit_name,
          row_number() over (partition by res1.accounting_type_node_id,res1.node_panel_argument_id order by res1.end_date) as rn,
          res1.value_number as vn1,res2.value_number as vn2,
          get_serial_number(res1.node_panel_id) as meter,
          res1.argument_formula_code afc1,
          res2.argument_formula_code afc2,
          res1.consumption as c1,
          res2.consumption as c2
      FROM get_indication_consumption(p_start_date,p_end_date,15,'V1') res1
      JOIN get_indication_consumption(p_start_date,p_end_date,15,'V2') res2
          ON res1.start_date = res2.start_date
          AND res1.end_date = res2.end_date
          AND res1.accounting_type_node_id = res2.accounting_type_node_id
    ) res
    ;
    ----------------------------------------------------------------------------------------------
    -- Вставка данных в rul_consumption V = V1 по формуле 16
    INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value, note)
    	select
        	   accounting_type_node_id,
               --case when rn = 1 then res.start_date else res.start_date + interval '1 day' end,
        		case when rn = 1 then res.start_date else res.start_date end,
               end_date,
               value,
               avg,
               argument_formula_code||':'||value_number - consumption||'-'||value_number||' '||unit_name||';'
        from (
        SELECT
        	accounting_type_node_id,
            start_date,
            end_date,
            value,
            unit_name,
            value /
            case when
            	EXTRACT(EPOCH FROM end_date - start_date) = 0
                then 1
                else
                EXTRACT(EPOCH FROM end_date - start_date)
                end
            * 86400 as avg,
            row_number() over (partition by accounting_type_node_id,node_panel_argument_id order by end_date) as rn,
            value_number,
            get_serial_number(node_panel_id) as meter,
            argument_formula_code,
            consumption
        FROM get_indication_consumption(p_start_date,p_end_date,16,'V')
        ) res
        ;
    ----------------------------------------------------------------------------------------------
    -- Вставка данных в rul_consumption Q = Q1 по формуле 17
    --INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value, connection_id)
    INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value, note)
    	select
        	   accounting_type_node_id,
               --case when rn = 1 then res.start_date else res.start_date + interval '1 day' end,
        		case when rn = 1 then res.start_date else res.start_date end,
               end_date,
               value,
               avg,
               argument_formula_code||':'||value_number - consumption||'-'||value_number||' '||unit_name||';'
        from (
        SELECT
        	accounting_type_node_id,
            start_date,
            end_date,
            value,
            unit_name,
            value /
            case when
            	EXTRACT(EPOCH FROM end_date - start_date) = 0
                then 1
                else
                EXTRACT(EPOCH FROM end_date - start_date)
                end
            * 86400 as avg,
            row_number() over (partition by accounting_type_node_id,node_panel_argument_id order by end_date) as rn,
            value_number,
            get_serial_number(node_panel_id) as meter,
            argument_formula_code,
            consumption
        FROM get_indication_consumption(p_start_date,p_end_date,17,'Q')
        ) res
        ;
    ----------------------------------------------------------------------------------------------
    -- Вставка данных в rul_consumption с вычитанием Q = Q1 - Q2 по формуле 18
    INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value, note)
    SELECT
        res.accounting_type_node_id,
        --case when rn = 1 then res.start_date else res.start_date + interval '1 day' end,
        case when rn = 1 then res.start_date else res.start_date end,
        res.end_date,
        res.value - res.value2 AS value,
        (res.value - res.value2) /
        case when
            	EXTRACT(EPOCH FROM res.end_date - res.start_date) = 0
                then 1
                else
                EXTRACT(EPOCH FROM res.end_date - res.start_date)
                end
         * 86400,
         res.afc1||':'||res.vn1 - res.c1||'-'||res.vn1||' '||res.unit_name||';'||res.afc2||':'||res.vn2 - res.c2||'-'||res.vn2||' '||res.unit_name||';'
    FROM (
      SELECT res1.start_date,res1.end_date,res1.value,res1.accounting_type_node_id,res2.value as value2,res1.unit_name,
          row_number() over (partition by res1.accounting_type_node_id,res1.node_panel_argument_id order by res1.end_date) as rn,
          res1.value_number as vn1,res2.value_number as vn2,
          get_serial_number(res1.node_panel_id) as meter,
          res1.argument_formula_code afc1,
          res2.argument_formula_code afc2,
          res1.consumption as c1,
          res2.consumption as c2
      FROM get_indication_consumption(p_start_date,p_end_date,18,'Q1') res1
      JOIN get_indication_consumption(p_start_date,p_end_date,18,'Q2') res2
          ON res1.start_date = res2.start_date
          AND res1.end_date = res2.end_date
          AND res1.accounting_type_node_id = res2.accounting_type_node_id
    ) res
    ;
    -- Вставка данных в rul_consumption T = T по формуле 80
    ----------------------------------------------------------------------------------------------
    INSERT INTO rul_consumption (accounting_type_node_id, start_date, end_date, value, avg_day_value, note)
    select
           accounting_type_node_id,
           --case when rn = 1 then res.start_date else res.start_date + interval '1 day' end,
            case when rn = 1 then res.start_date else res.start_date end,
           end_date,
           value,
           avg,
           argument_formula_code||':'||value_number - consumption||'-'||value_number||' '||unit_name||';'
    from (
    SELECT
        accounting_type_node_id,
        start_date,
        end_date,
        unit_name,
        value,
        value /
        case when
            EXTRACT(EPOCH FROM end_date - start_date) = 0
            then 1
            else
            EXTRACT(EPOCH FROM end_date - start_date)
            end
        * 86400 as avg,
        row_number() over (partition by accounting_type_node_id,node_panel_argument_id order by end_date) as rn,
        value_number,
        get_serial_number(node_panel_id) as meter,
        argument_formula_code,
        consumption
    FROM get_indication_consumption(p_start_date,p_end_date,80,'T')
    ) res
    ;
    ----------------------------------------------------------------------------------------------
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_formuls_load(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_formuls_load(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
i record;
v_connection_ids BIGINT[];
v_count BIGINT;
v_locality_id BIGINT;
BEGIN
	-- Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    -- В этом списке будут подключения по договору, которые не подтверждены (т.е. у них на них не сформирован счет)
    SELECT array_agg(connection_id) INTO v_connection_ids FROM (
    SELECT DISTINCT connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date
                                )
      AND invoice_group_index IS NOT NULL) conn;
    DELETE FROM rul_consumption_load
    -- Дописываем, чтобы подтвержденные начисления не удалялись при пересчете
    WHERE connection_id = ANY (v_connection_ids)
    --WHERE connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
    --AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL)
    AND start_date >= p_start_date and end_date <= p_end_date;
    -- Расчет формулы 26 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'51'->>'value')::NUMERIC * (details->'52'->>'value')::NUMERIC  / 100 *
        (extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1) AS value,
        conn2.accounting_type_node_id,
        (details->'51'->>'code')::varchar||': '||(details->'51'->>'value')::varchar||' '||(details->'51'->>'unit')::varchar||', '||
        (details->'52'->>'code')::varchar||': '||(details->'52'->>'value')::varchar||' '||(details->'52'->>'unit')::varchar||', '||
        (details->'53'->>'code')::varchar||': '||(extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)||' '||(details->'53'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(26::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 27 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'54'->>'value')::NUMERIC * (details->'55'->>'value')::NUMERIC / 100 * (extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1) AS value,
        conn2.accounting_type_node_id,
        (details->'54'->>'code')::varchar||': '||(details->'54'->>'value')::varchar||' '||(details->'54'->>'unit')::varchar||', '||
        (details->'55'->>'code')::varchar||': '||(details->'55'->>'value')::varchar||' '||(details->'55'->>'unit')::varchar||', '||
        (details->'56'->>'code')::varchar||': '||(extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)||' '||(details->'56'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(27::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    AND load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date;
    --Проверка на то, заведены ли температуры для подключений, которые считаются по определенной формуле формуле
    --Выдаем ошибку. если не хватает
    --Можно скорее всего добавить проверку по всем формулам сразу, в которых участвует температура.
    FOR i IN
    	SELECT rc.connection_id, rc.connection_name
        FROM rul_connection rc
    	JOIN rul_formula_connection rfc
        	ON rc.connection_id = rfc.connection_id
        WHERE rfc.formula_id = 28
        AND rc.connection_id = ANY(v_connection_ids)
    LOOP
    	select count(*),obs.locality_id into v_count, v_locality_id
        from rul_observation obs
        join rul_object obj on obs.locality_id = obj.locality_id
        join rul_node n on n.object_id = obj.object_id
        join rul_node_calculate_parameter ncp on ncp.node_id = n.node_id
        where observation_type_id = 1 --Воздух
        and observation_period_id = 1 --Среднесуточная
        and node_calculate_parameter_id = (SELECT node_calculate_parameter_id FROM rul_connection WHERE connection_id = i.connection_id)
        and observation_date >= p_start_date
        and observation_date <= p_end_date
        group by obs.locality_id;
      IF coalesce(v_count,0) != extract('day' from p_end_date - p_start_date) + 1
      THEN
      RAISE EXCEPTION '[[Не заведены температуры для: %]]', (select rl.locality_name
                                                              from rul_locality rl
                                                              join rul_object obj on rl.locality_id = obj.locality_id
                                                              join rul_node n on n.object_id = obj.object_id
                                                              join rul_node_calculate_parameter ncp on ncp.node_id = n.node_id
                                                              join rul_connection rc on rc.node_calculate_parameter_id = ncp.node_calculate_parameter_id
                                                              WHERE connection_id = i.connection_id)
      USING ERRCODE = '25001';
      END IF;
    END LOOP;
    -- Расчет формулы 28 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'57'->>'value')::NUMERIC * 24 *
        (extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)
        * ((details->'60'->>'value')::NUMERIC
        - get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        )/((details->'60'->>'value')::NUMERIC - (details->'62'->>'value')::NUMERIC) AS value,
        conn2.accounting_type_node_id,
        (details->'57'->>'code')::varchar||': '||(details->'57'->>'value')::varchar||' '||(details->'57'->>'unit')::varchar||', '||
        (details->'59'->>'code')::varchar||': '||(extract (day from (LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date))) + 1)||' '||(details->'59'->>'unit')::varchar||', '||
        (details->'60'->>'code')::varchar||': '||(details->'60'->>'value')::varchar||' '||(details->'60'->>'unit')::varchar||', '||
        (details->'61'->>'code')::varchar||': '||
        get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        ||' '||(details->'61'->>'unit')::varchar||', '||
        (details->'62'->>'code')::varchar||': '||(details->'62'->>'value')::varchar||' '||(details->'62'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(28::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 70 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'172'->>'value')::NUMERIC *
        ( (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'183'->>'value')::NUMERIC
        	+
          (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'184'->>'value')::NUMERIC
        )
        * (((details->'174'->>'value')::NUMERIC -
        	get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        )/((details->'174'->>'value')::NUMERIC - (details->'177'->>'value')::NUMERIC)) AS value,
        conn2.accounting_type_node_id,
        (details->'172'->>'code')::varchar||': '||(details->'172'->>'value')::varchar||' '||(details->'172'->>'unit')::varchar||', '||
        (details->'173'->>'code')::varchar||': '||((count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'183'->>'value')::NUMERIC
        	+ (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'184'->>'value')::NUMERIC
        )||' '||(details->'173'->>'unit')::varchar||', '||
        (details->'174'->>'code')::varchar||': '||(details->'174'->>'value')::varchar||' '||(details->'174'->>'unit')::varchar||', '||
        (details->'176'->>'code')::varchar||': '||
        get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        ||' '||(details->'176'->>'unit')::varchar||', '||
        (details->'177'->>'code')::varchar||': '||(details->'177'->>'value')::varchar||' '||(details->'177'->>'unit')::varchar||', '||
        (details->'183'->>'code')::varchar||': '||(details->'183'->>'value')::varchar||' '||(details->'183'->>'unit')::varchar||', '||
        (details->'184'->>'code')::varchar||': '||(details->'184'->>'value')::varchar||' '||(details->'184'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(70::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 71 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'178'->>'value')::NUMERIC *
        ( (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'189'->>'value')::NUMERIC
        	+
          (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'190'->>'value')::NUMERIC
        )
        / (details->'187'->>'value')::NUMERIC
         AS value,
        conn2.accounting_type_node_id,
        (details->'178'->>'code')::varchar||': '||(details->'178'->>'value')::varchar||' '||(details->'178'->>'unit')::varchar||', '||
        (details->'179'->>'code')::varchar||': '||((count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekdays * (details->'189'->>'value')::NUMERIC
        	+ (count_weekdays(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date))).weekends * (details->'190'->>'value')::NUMERIC
        )||' '||(details->'179'->>'unit')::varchar||', '||
        (details->'187'->>'code')::varchar||': '||(details->'187'->>'value')::varchar||' '||(details->'187'->>'unit')::varchar||', '||
        (details->'189'->>'code')::varchar||': '||(details->'189'->>'value')::varchar||' '||(details->'189'->>'unit')::varchar||', '||
        (details->'190'->>'code')::varchar||': '||(details->'190'->>'value')::varchar||' '||(details->'190'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(71::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 98 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'276'->>'value')::NUMERIC
        * (extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)
        * 10 *
        (
        (details->'273'->>'value')::NUMERIC * coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,1,2)),0) --Жидкие
        +
        (details->'275'->>'value')::NUMERIC * coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,2,2)),0) --Твердые
        )
        / (extract (day from p_end_date - p_start_date) + 1)
        AS value,
        conn2.accounting_type_node_id,
        (details->'272'->>'code')::varchar||': '||coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,1,2)),0)||' '||(details->'272'->>'unit')::varchar||', '||
        (details->'273'->>'code')::varchar||': '||(details->'273'->>'value')::varchar||' '||(details->'273'->>'unit')::varchar||', '||
        (details->'274'->>'code')::varchar||': '||coalesce((get_precipitation(p_start_date,conn2.node_calculate_parameter_id,2,2)),0)||' '||(details->'274'->>'unit')::varchar||', '||
        (details->'275'->>'code')::varchar||': '||(details->'275'->>'value')::varchar||' '||(details->'275'->>'unit')::varchar||', '||
        (details->'276'->>'code')::varchar||': '||(details->'276'->>'value')::varchar||' '||(details->'276'->>'unit')::varchar||', '||
        (details->'278'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'278'->>'unit')::varchar||', '||
        (details->'279'->>'code')::varchar||': '||(extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)||' '||(details->'279'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(98::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
     -- Расчет формулы 99 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        ((details->'280'->>'value')::NUMERIC * (extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)
        * 10 * (details->'281'->>'value')::NUMERIC * (details->'282'->>'value')::NUMERIC * (details->'283'->>'value')::NUMERIC)
        /
        (extract (day from p_end_date - p_start_date) + 1) AS value,
        conn2.accounting_type_node_id,
        (details->'280'->>'code')::varchar||': '||(details->'280'->>'value')::varchar||' '||(details->'280'->>'unit')::varchar||', '||
        (details->'281'->>'code')::varchar||': '||(details->'281'->>'value')::varchar||' '||(details->'281'->>'unit')::varchar||', '||
        (details->'282'->>'code')::varchar||': '||(details->'282'->>'value')::varchar||' '||(details->'282'->>'unit')::varchar||', '||
        (details->'283'->>'code')::varchar||': '||(details->'283'->>'value')::varchar||' '||(details->'283'->>'unit')::varchar||', '||
        (details->'285'->>'code')::varchar||': '||(extract (day from LEAST(conn2.end_date, load1.end_date) - GREATEST(conn2.start_date, load1.start_date)) + 1)||' '||(details->'285'->>'unit')::varchar||', '||
        (details->'286'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'286'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(99::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 100 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        1.5 * (details->'288'->>'value')::NUMERIC * (details->'289'->>'value')::NUMERIC * (details->'290'->>'value')::NUMERIC
        AS value,
        conn2.accounting_type_node_id,
        (details->'288'->>'code')::varchar||': '||(details->'288'->>'value')::varchar||' '||(details->'288'->>'unit')::varchar||', '||
        (details->'289'->>'code')::varchar||': '||(details->'289'->>'value')::varchar||' '||(details->'289'->>'unit')::varchar||', '||
        (details->'290'->>'code')::varchar||': '||(details->'290'->>'value')::varchar||' '||(details->'290'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(100::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 154 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'567'->>'value')::NUMERIC *
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'572'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'573'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'574'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'575'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'576'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'577'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'578'->>'value')::NUMERIC
                      ELSE 1 END)
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'579'->>'value')::SMALLINT)
        )
        * (((details->'569'->>'value')::NUMERIC -
        	get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        )/((details->'569'->>'value')::NUMERIC - (details->'571'->>'value')::NUMERIC)) AS value,
        conn2.accounting_type_node_id,
        (details->'567'->>'code')::varchar||': '||(details->'567'->>'value')::varchar||' '||(details->'567'->>'unit')::varchar||', '||
        (details->'568'->>'code')::varchar||': '||
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'572'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'573'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'574'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'575'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'576'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'577'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'578'->>'value')::NUMERIC
                      ELSE 1 END)
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'579'->>'value')::SMALLINT)
        )::varchar||' '||(details->'568'->>'unit')::varchar||', '||
        (details->'569'->>'code')::varchar||': '||(details->'569'->>'value')::varchar||' '||(details->'569'->>'unit')::varchar||', '||
        (details->'570'->>'code')::varchar||': '||
        get_temperature(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),conn2.node_calculate_parameter_id,1,1)
        ||' '||(details->'570'->>'unit')::varchar||', '||
        (details->'571'->>'code')::varchar||': '||(details->'571'->>'value')::varchar||' '||(details->'571'->>'unit')::varchar||', '||
        (details->'572'->>'code')::varchar||': '||(details->'572'->>'value')::varchar||' '||(details->'572'->>'unit')::varchar||', '||
        (details->'573'->>'code')::varchar||': '||(details->'573'->>'value')::varchar||' '||(details->'573'->>'unit')::varchar||', '||
        (details->'574'->>'code')::varchar||': '||(details->'574'->>'value')::varchar||' '||(details->'574'->>'unit')::varchar||', '||
        (details->'575'->>'code')::varchar||': '||(details->'575'->>'value')::varchar||' '||(details->'575'->>'unit')::varchar||', '||
        (details->'576'->>'code')::varchar||': '||(details->'576'->>'value')::varchar||' '||(details->'576'->>'unit')::varchar||', '||
        (details->'577'->>'code')::varchar||': '||(details->'577'->>'value')::varchar||' '||(details->'577'->>'unit')::varchar||', '||
        (details->'578'->>'code')::varchar||': '||(details->'578'->>'value')::varchar||' '||(details->'578'->>'unit')::varchar||', '||
        (details->'579'->>'code')::varchar||': '||(details->'579'->>'value')::varchar||' '||(details->'579'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(154::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
    -- Расчет формулы 155 по нагрузке
    INSERT INTO rul_consumption_load
      (connection_id,connection_name,start_date,end_date,formula_connection_id,version_load_standard_id,value,accounting_type_node_id,note)
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        (details->'556'->>'value')::NUMERIC *
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'559'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'560'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'561'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'562'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'563'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'564'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'565'->>'value')::NUMERIC
                      ELSE 1 END) :: NUMERIC
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'566'->>'value')::SMALLINT)
        )
        /
        (details->'558'->>'value')::NUMERIC
         AS value,
        conn2.accounting_type_node_id,
        (details->'556'->>'code')::varchar||': '||(details->'556'->>'value')::varchar||' '||(details->'556'->>'unit')::varchar||', '||
        (details->'557'->>'code')::varchar||': '||
        (
          SELECT SUM(CASE WHEN day_name LIKE '%Monday%' THEN day_count * (details->'559'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Tuesday%' THEN day_count * (details->'560'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Wednesday%' THEN day_count * (details->'561'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Thursday%' THEN day_count * (details->'562'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Friday%' THEN day_count * (details->'563'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Saturday%' THEN day_count * (details->'564'->>'value')::NUMERIC
                      WHEN day_name LIKE '%Sunday%' THEN day_count * (details->'565'->>'value')::NUMERIC
                      ELSE 1 END) :: NUMERIC
          FROM count_weekdays_for_every_day(GREATEST(conn2.start_date, load1.start_date),LEAST(conn2.end_date, load1.end_date),(details->'566'->>'value')::SMALLINT)
        )::varchar||' '||(details->'557'->>'unit')::varchar||', '||
        (details->'558'->>'code')::varchar||': '||(details->'558'->>'value')::varchar||' '||(details->'558'->>'unit')::varchar||', '||
        (details->'559'->>'code')::varchar||': '||(details->'559'->>'value')::varchar||' '||(details->'559'->>'unit')::varchar||', '||
        (details->'560'->>'code')::varchar||': '||(details->'560'->>'value')::varchar||' '||(details->'560'->>'unit')::varchar||', '||
        (details->'561'->>'code')::varchar||': '||(details->'561'->>'value')::varchar||' '||(details->'561'->>'unit')::varchar||', '||
        (details->'562'->>'code')::varchar||': '||(details->'562'->>'value')::varchar||' '||(details->'562'->>'unit')::varchar||', '||
        (details->'563'->>'code')::varchar||': '||(details->'563'->>'value')::varchar||' '||(details->'563'->>'unit')::varchar||', '||
        (details->'564'->>'code')::varchar||': '||(details->'564'->>'value')::varchar||' '||(details->'564'->>'unit')::varchar||', '||
        (details->'565'->>'code')::varchar||': '||(details->'565'->>'value')::varchar||' '||(details->'565'->>'unit')::varchar||', '||
        (details->'566'->>'code')::varchar||': '||(details->'566'->>'value')::varchar||' '||(details->'566'->>'unit')::varchar
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(155::BIGINT,p_start_date,p_end_date) load1
    ON load1.connection_id = conn2.connection_id
    --Получаем даты пересечения периодов действия способа учета, подключения и заведенных нагрузок.
    --Также все даты уже обрезаны расчетным периодом внутри запросов
    and load1.start_date <= conn2.end_date
    AND load1.end_date >= conn2.start_date
    ;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_formuls_losses(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_mode smallint)
CREATE OR REPLACE PROCEDURE public.process_formuls_losses(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone, IN p_mode smallint DEFAULT 0)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	row_record record;
BEGIN
	IF p_mode = 0 then
        --Расчет потерь по формуле 76
        RAISE NOTICE 'Loses: node_calculate_parameter_id=%, %', p_node_calculate_parameter_id, (select count(*) from temp_results);
        create temp table temp_losses (line_id bigint, section_id BIGINT, path bigint[], v_p numeric, start_date timestamp, end_date timestamp,
        accounting_type_node_id BIGINT, p numeric, g NUMERIC, losses numeric, unit varchar);
        insert into temp_losses
        SELECT
            section.line_id,
            section.section_id,
            section.path,
            section.v_p,
            section.start_date,
            section.end_date,
            section.accounting_type_node_id,
            bal.p,
            count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours AS G,
            0.0025 * bal.p * section.v_p * (
                count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours
            )/count(*) over (partition by section.line_id,section.section_id) AS losses,
            (select unit_name from rul_unit where unit_id =
            	(select unit_id from rul_parameter where parameter_id =
                	(select parameter_id from rul_formula where formula_id = section.formula_id)
                )
             ) as unit
        FROM (
            SELECT
                tr.line_id,
                rs.section_id,
                tr.path,
                get_v_p(rs.section_id, p_start_date) AS v_p,
                GREATEST(tr.start_date, rs.start_date) AS start_date,
                LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date)) AS end_date,
                tr.connection_id,
                tr.accounting_type_node_id,
                tr.formula_id
            FROM temp_results tr
            LEFT JOIN rul_section rs ON tr.line_id = rs.line_id
            WHERE rs.start_date <= tr.end_date
              AND coalesce(rs.end_date,p_end_date) >= tr.start_date
              AND tr.formula_id = 76
        ) AS section
        JOIN (
            SELECT
                mon_work_hours,
                tue_work_hours,
                wed_work_hours,
                thu_work_hours,
                fri_work_hours,
                sat_work_hours,
                sun_work_hours,
                0.99987
                + 1.518 * 10^(-4) * (supply_temperature * 0.75 + return_temperature * 0.25)
                - 7.088 * 10^(-6) * (supply_temperature * 0.75 + return_temperature * 0.25)^2
                - 2.220 * 10^(-8) * (supply_temperature * 0.75 + return_temperature * 0.25)^3
                + 1.310 * 10^(-10) * (supply_temperature * 0.75 + return_temperature * 0.25)^4
                - 1.142 * 10^(-13) * (supply_temperature * 0.75 + return_temperature * 0.25)^5
                + 2.975 * 10^(-17) * (supply_temperature * 0.75 + return_temperature * 0.25)^6 AS P
            FROM rul_losses_params
            WHERE accounting_type_node_id = (
                SELECT accounting_type_node_id
                FROM rul_accounting_type_node
                WHERE node_calculate_parameter_id = find_balance_node(p_node_calculate_parameter_id, p_start_date, p_end_date)
                  AND start_date <= p_end_date
                  AND COALESCE(end_date, p_start_date) >= p_start_date
                  limit 1
            )
        ) AS bal
        ON 1=1;
        --Расчет потерь по формуле 79
        INSERT INTO temp_losses
        --INSERT INTO rul_consumption_losses (line_id, section_id, path, v_p, start_date, end_date, p, g, value)
        SELECT
            section.line_id,
            section.section_id,
            section.path,
            section.v_p,
            section.start_date,
            section.end_date,
            section.accounting_type_node_id,
            bal.p,
            count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours AS G,
            case
                when section.method_tubing_id IN(1,2) AND
                (
                    SELECT target_use_id
                    FROM rul_parameter rp
                    JOIN rul_node_calculate_parameter rncp ON rncp.parameter_id = rp.parameter_id
                    JOIN rul_line_parameter rlp ON rlp.node_calculate_parameter_id = rncp.node_calculate_parameter_id
                                                AND rlp.line_id = section.line_id
                    JOIN rul_accounting_type_node ratn ON ratn.node_calculate_parameter_id = rncp.node_calculate_parameter_id
                    LIMIT 1
                ) IN (1,3)
                THEN
(
    (
        section.value_Q1
        * (
              bal.supply_temperature
            + bal.return_temperature
            - 2 * section.tf
          )
        / (
              section.value_tn1
            + section.value_tn2
            - 2 * section.value_toc
          )
    ) * 10^(-6)
    + 0.0025 * bal.p * section.v_p
      * (
            (bal.supply_temperature * 0.5 + bal.return_temperature * 0.5)
          - bal.recharge_temperature
        ) * 10^(-3)
)
*
(
    count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
    count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
    count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
    count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
    count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
    count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
    count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours
)
/ count(*) over (partition by section.line_id, section.section_id)
                else
                    (
            ((section.value_Q1 * (bal.supply_temperature - section.tf) / (section.value_tn1 - section.value_toc) +
            section.value_Q2 * (bal.return_temperature - section.tf) / (section.value_tn2 - section.value_toc)))* 10^(-6)
            + 0.0025 * bal.p * section.v_p * ((bal.supply_temperature * 0.5 + bal.return_temperature * 0.5) - bal.recharge_temperature) * 10^(-3)
            )
            *
             (
                count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours
            )/count(*) over (partition by section.line_id,section.section_id)
                end AS losses,
            (select unit_name from rul_unit where unit_id =
            	(select unit_id from rul_parameter where parameter_id =
                	(select parameter_id from rul_formula where formula_id = section.formula_id)
                )
             ) as unit
        FROM (
            SELECT
                tr.line_id,
                rs.section_id,
                rs.method_tubing_id,
                tr.path,
                get_v_p(rs.section_id, p_start_date) AS v_p,
                MAX(GREATEST(tr.start_date, rs.start_date)) AS start_date,
                MAX(LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date))) AS end_date,
                MAX(CASE WHEN rasv.attribute_section_id = 20  THEN rasv.value END) AS value_Q1,
                MAX(CASE WHEN rasv.attribute_section_id = 21  THEN rasv.value END) AS value_Q2,
                MAX(CASE WHEN rasv.attribute_section_id = 13  THEN rasv.value END) AS value_tn1,
                MAX(CASE WHEN rasv.attribute_section_id = 14  THEN rasv.value END) AS value_tn2,
                MAX(CASE WHEN rasv.attribute_section_id = 15  THEN rasv.value END) AS value_toc,
                tr.connection_id,
                tr.accounting_type_node_id,
                case
                  when rs.method_tubing_id = 3 then
                	 get_temperature(MAX(GREATEST(tr.start_date, rs.start_date)),MAX(LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date))),tr.node_id,1,1)
                  when rs.method_tubing_id in (1,2) then
                     get_temperature(MAX(GREATEST(tr.start_date, rs.start_date)),MAX(LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date))),tr.node_id,2,1)
                  when rs.method_tubing_id in (4,5,6) then
                  	 MAX(CASE WHEN rasv.attribute_section_id = 15  THEN rasv.value END)
                  end as tf,
                  tr.formula_id
            FROM temp_results tr
            LEFT JOIN rul_section rs ON tr.line_id = rs.line_id
            LEFT JOIN rul_attribute_section_value rasv ON rasv.section_id = rs.section_id
            WHERE rs.start_date <= tr.end_date
              AND coalesce(rs.end_date,p_end_date) >= tr.start_date
              AND tr.formula_id = 79
            GROUP BY  tr.line_id,rs.section_id,tr.connection_id,tr.path,tr.node_id, tr.accounting_type_node_id, tr.start_date, tr.end_date, tr.formula_id
        ) AS section
        JOIN (
            SELECT
                mon_work_hours,
                tue_work_hours,
                wed_work_hours,
                thu_work_hours,
                fri_work_hours,
                sat_work_hours,
                sun_work_hours,
                0.99987
                + 1.518 * 10^(-4) * (supply_temperature * 0.5 + return_temperature * 0.5)
                - 7.088 * 10^(-6) * (supply_temperature * 0.5 + return_temperature * 0.5)^2
                - 2.220 * 10^(-8) * (supply_temperature * 0.5 + return_temperature * 0.5)^3
                + 1.310 * 10^(-10) * (supply_temperature * 0.5 + return_temperature * 0.5)^4
                - 1.142 * 10^(-13) * (supply_temperature * 0.5 + return_temperature * 0.5)^5
                + 2.975 * 10^(-17) * (supply_temperature * 0.5 + return_temperature * 0.5)^6 AS P,
                supply_temperature,
                return_temperature,
                recharge_temperature
            FROM rul_losses_params
            WHERE accounting_type_node_id = (
                SELECT accounting_type_node_id
                FROM rul_accounting_type_node
                WHERE node_calculate_parameter_id =find_balance_node(p_node_calculate_parameter_id, p_start_date, p_end_date)
                  AND start_date <= p_end_date
                  AND COALESCE(end_date, p_start_date) >= p_start_date
                  limit 1
            )
        ) AS bal
        ON 1=1;
            INSERT INTO rul_consumption_losses
            (line_id, section_id, v_p, start_date, end_date, p, g, value, connection_id, theoretical_calculation, accounting_type_node_id, note)
            SELECT tl.line_id, tl.section_id, tl.v_p, tl.start_date, tl.end_date, tl.p, tl.g,
            case when sum(coalesce(tr.val,0)) over (partition by tl.line_id, tr.start_date, tr.end_date) = 0 then tl.losses else
            (coalesce(tr.val,0)/sum(coalesce(tr.val,0)) over (partition by tl.line_id, tr.start_date, tr.end_date)) * tl.losses end
            , tr.connection_id, false , tr.accounting_type_node_id,
            (select line_name from rul_line where line_id = tl.line_id)
            FROM temp_results tr
            JOIN temp_losses tl
                ON tr.path @> tl.path
                AND tl.start_date = tr.start_date
                AND tl.end_date = tr.end_date
                AND tr.connection_id is not null
            ;
	ELSE
		--Расчет потерь по формуле 76
        RAISE NOTICE 'Loses: node_calculate_parameter_id=%, %', p_node_calculate_parameter_id, (select count(*) from temp_results);
        create temp table temp_losses (line_id bigint, section_id BIGINT, path bigint[], v_p numeric, start_date timestamp, end_date timestamp,
        accounting_type_node_id BIGINT, p numeric, g NUMERIC, losses numeric, unit varchar, is_balancing_losses numeric);
        insert into temp_losses
            SELECT
            section.line_id,
            section.section_id,
            section.path,
            section.v_p,
            section.start_date,
            section.end_date,
            section.accounting_type_node_id,
            bal.p,
            count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours AS G,
            0.0025 * bal.p * section.v_p * (
                count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours
            )/count(*) over (partition by section.line_id,section.section_id) AS losses,
            (select unit_name from rul_unit where unit_id =
            	(select unit_id from rul_parameter where parameter_id =
                	(select parameter_id from rul_formula where formula_id = section.formula_id)
                )
             ) as unit,
             1
        FROM (
            SELECT
                tr.line_id,
                rs.section_id,
                tr.path,
                get_v_p(rs.section_id, p_start_date) AS v_p,
                GREATEST(tr.start_date, rs.start_date) AS start_date,
                LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date)) AS end_date,
                --tr.connection_id,
                tr.accounting_type_node_id,
                tr.formula_id
            FROM temp_results tr
            LEFT JOIN rul_section rs ON tr.line_id = rs.line_id
            WHERE rs.start_date <= tr.end_date
              AND coalesce(rs.end_date,p_end_date) >= tr.start_date
              AND tr.formula_id = 76
              AND tr.after_indication_accounting = 0
              AND tr.balancing_line = 1
        ) AS section
        JOIN (
            SELECT
                mon_work_hours,
                tue_work_hours,
                wed_work_hours,
                thu_work_hours,
                fri_work_hours,
                sat_work_hours,
                sun_work_hours,
                0.99987
                + 1.518 * 10^(-4) * (supply_temperature * 0.75 + return_temperature * 0.25)
                - 7.088 * 10^(-6) * (supply_temperature * 0.75 + return_temperature * 0.25)^2
                - 2.220 * 10^(-8) * (supply_temperature * 0.75 + return_temperature * 0.25)^3
                + 1.310 * 10^(-10) * (supply_temperature * 0.75 + return_temperature * 0.25)^4
                - 1.142 * 10^(-13) * (supply_temperature * 0.75 + return_temperature * 0.25)^5
                + 2.975 * 10^(-17) * (supply_temperature * 0.75 + return_temperature * 0.25)^6 AS P
            FROM rul_losses_params
            WHERE accounting_type_node_id = (
                SELECT accounting_type_node_id
                FROM rul_accounting_type_node
                WHERE node_calculate_parameter_id = p_node_calculate_parameter_id
                  AND start_date <= p_end_date
                  AND COALESCE(end_date, p_start_date) >= p_start_date
                  limit 1
            )
        ) AS bal
        ON 1=1;
        --Расчет потерь по формуле 79
        INSERT INTO temp_losses
            SELECT
            section.line_id,
            section.section_id,
            section.path,
            section.v_p,
            section.start_date,
            section.end_date,
            section.accounting_type_node_id,
            bal.p,
            count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
            count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours AS G,
            case
                    when section.method_tubing_id IN(1,2) AND (
                    SELECT target_use_id
                    FROM rul_parameter rp
                    JOIN rul_node_calculate_parameter rncp ON rncp.parameter_id = rp.parameter_id
                    JOIN rul_line_parameter rlp ON rlp.node_calculate_parameter_id = rncp.node_calculate_parameter_id
                                                AND rlp.line_id = section.line_id
                    JOIN rul_accounting_type_node ratn ON ratn.node_calculate_parameter_id = rncp.node_calculate_parameter_id
                    LIMIT 1
                ) IN (1,3)  then
    (
        (
            section.value_Q1
            * (
                  bal.supply_temperature
                + bal.return_temperature
                - 2 * section.tf
              )
            / (
                  section.value_tn1
                + section.value_tn2
                - 2 * section.value_toc
              )
        ) * 10^(-6)
        + 0.0025 * bal.p * section.v_p
          * (
                (bal.supply_temperature * 0.5 + bal.return_temperature * 0.5)
              - bal.recharge_temperature
            ) * 10^(-3)
    )
    *
    (
        count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
        count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
        count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
        count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
        count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
        count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
        count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours
    )
    / count(*) over (partition by section.line_id, section.section_id)
                    else
            (
            ((section.value_Q1 * (bal.supply_temperature - section.tf) / (section.value_tn1 - section.value_toc) +
            section.value_Q2 * (bal.return_temperature - section.tf) / (section.value_tn2 - section.value_toc)))* 10^(-6)
            + 0.0025 * bal.p * section.v_p * ((bal.supply_temperature * 0.5 + bal.return_temperature * 0.5) - bal.recharge_temperature) * 10^(-3)
            )
            *
             (
                count_weekday_in_period(section.start_date, section.end_date, 1) * bal.mon_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 2) * bal.tue_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 3) * bal.wed_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 4) * bal.thu_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 5) * bal.fri_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 6) * bal.sat_work_hours +
                count_weekday_in_period(section.start_date, section.end_date, 7) * bal.sun_work_hours
            )/count(*) over (partition by section.line_id,section.section_id) end AS losses,
            (select unit_name from rul_unit where unit_id =
            	(select unit_id from rul_parameter where parameter_id =
                	(select parameter_id from rul_formula where formula_id = section.formula_id)
                )
             ) as unit,
             1
        FROM (
            SELECT
                tr.line_id,
                rs.section_id,
                rs.method_tubing_id,
                tr.path,
                get_v_p(rs.section_id, p_start_date) AS v_p,
                MAX(GREATEST(tr.start_date, rs.start_date)) AS start_date,
                MAX(LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date))) AS end_date,
                MAX(CASE WHEN rasv.attribute_section_id = 20  THEN rasv.value END) AS value_Q1,
                MAX(CASE WHEN rasv.attribute_section_id = 21  THEN rasv.value END) AS value_Q2,
                MAX(CASE WHEN rasv.attribute_section_id = 13  THEN rasv.value END) AS value_tn1,
                MAX(CASE WHEN rasv.attribute_section_id = 14  THEN rasv.value END) AS value_tn2,
                MAX(CASE WHEN rasv.attribute_section_id = 15  THEN rasv.value END) AS value_toc,
                --tr.connection_id,
                tr.accounting_type_node_id,
                case
                  when rs.method_tubing_id = 3 then
                	 get_temperature(MAX(GREATEST(tr.start_date, rs.start_date)),MAX(LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date))),tr.node_id,1,1)
                  when rs.method_tubing_id in (1,2) then
                     get_temperature(MAX(GREATEST(tr.start_date, rs.start_date)),MAX(LEAST(tr.end_date, COALESCE(rs.end_date, p_end_date))),tr.node_id,2,1)
                  when rs.method_tubing_id in (4,5,6) then
                  	 MAX(CASE WHEN rasv.attribute_section_id = 15  THEN rasv.value END)
                  end as tf,
                tr.formula_id
            FROM temp_results tr
            LEFT JOIN rul_section rs ON tr.line_id = rs.line_id
            LEFT JOIN rul_attribute_section_value rasv ON rasv.section_id = rs.section_id
            WHERE rs.start_date <= tr.end_date
              AND coalesce(rs.end_date,p_end_date) >= tr.start_date
              AND tr.formula_id = 79
              AND tr.after_indication_accounting = 0
              AND tr.balancing_line = 1
            GROUP BY  tr.line_id,rs.section_id--,tr.connection_id
            ,tr.path,tr.node_id, tr.accounting_type_node_id, tr.start_date, tr.end_date, tr.formula_id
        ) AS section
        JOIN (
            SELECT
                mon_work_hours,
                tue_work_hours,
                wed_work_hours,
                thu_work_hours,
                fri_work_hours,
                sat_work_hours,
                sun_work_hours,
                0.99987
                + 1.518 * 10^(-4) * (supply_temperature * 0.5 + return_temperature * 0.5)
                - 7.088 * 10^(-6) * (supply_temperature * 0.5 + return_temperature * 0.5)^2
                - 2.220 * 10^(-8) * (supply_temperature * 0.5 + return_temperature * 0.5)^3
                + 1.310 * 10^(-10) * (supply_temperature * 0.5 + return_temperature * 0.5)^4
                - 1.142 * 10^(-13) * (supply_temperature * 0.5 + return_temperature * 0.5)^5
                + 2.975 * 10^(-17) * (supply_temperature * 0.5 + return_temperature * 0.5)^6 AS P,
                supply_temperature,
                return_temperature,
                recharge_temperature
            FROM rul_losses_params
            WHERE accounting_type_node_id = (
                SELECT accounting_type_node_id
                FROM rul_accounting_type_node
                WHERE node_calculate_parameter_id = p_node_calculate_parameter_id
                  AND start_date <= p_end_date
                  AND COALESCE(end_date, p_start_date) >= p_start_date
                  limit 1
            )
        ) AS bal
        ON 1=1;
        	RAISE NOTICE 'Loses: Losses %', (select count(*) from temp_losses);
            INSERT INTO rul_consumption_losses (line_id, section_id, v_p, start_date, end_date, p, g, value, theoretical_calculation,note,is_balancing_losses,accounting_type_node_id)
            SELECT tl.line_id, tl.section_id, tl.v_p, tl.start_date, tl.end_date, tl.p, tl.g, tl.losses, false,
            (select line_name from rul_line where line_id = tl.line_id),
            1,
            accounting_type_node_id
            FROM temp_losses tl;
    END IF;
    DROP TABLE temp_losses;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_formuls_pipe(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_formuls_pipe(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	-- Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    -- В этом списке будут подключения по договору, которые не подтверждены (т.е. у них на них не сформирован счет)
    CREATE TEMP TABLE temp_connections AS
    SELECT DISTINCT connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL;
    DELETE FROM rul_consumption_pipe
    WHERE connection_id IN (SELECT connection_id FROM temp_connections)
    --WHERE connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
    AND start_date >= p_start_date and end_date <= p_end_date;
    -- Расчет формулы 62 по сечению
    INSERT INTO rul_consumption_pipe
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        conn2.start_date,
        conn2.end_date,
        3.1415 * pipe1.value ^ 2 / 4 * 2.0 * 0,000001 * 86400 * (extract (day from (conn2.end_date - conn2.start_date)) + 1) AS value,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id
    FROM (
    SELECT
        conn.connection_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone),p_end_date) AS end_date,
            c.node_calculate_parameter_id
        FROM rul_connection c
        WHERE
        	--Проверяем действует ли подключение в переданном расчетном периоде
            c.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (c.start_date < p_start_date AND COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id
        FROM rul_accounting_type_node atn
        WHERE
        	--Проверяем действует ли способ учета в переданном расчетном периоде
            atn.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (atn.start_date < p_start_date AND COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    and (acc.start_date >= conn.start_date AND acc.start_date <= conn.end_date
            OR acc.end_date > conn.start_date AND acc.end_date <= conn.end_date
            OR (acc.start_date < conn.start_date AND acc.end_date > conn.end_date))
    ) conn2
    JOIN (
        SELECT
            pv.accounting_type_node_id,
            af.formula_id,
            pv.value
        FROM rul_pipe_value pv
        JOIN rul_argument_formula af
            ON pv.argument_formula_id = af.argument_formula_id
        WHERE af.formula_id = 62
        ) pipe1
    ON pipe1.accounting_type_node_id = conn2.accounting_type_node_id
    ;
    -- Расчет формулы 60 по сечению
    INSERT INTO rul_consumption_pipe
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        conn2.start_date,
        conn2.end_date,
        3.1415 * pipe1.value ^ 2 / 4 * 2.0 * 0,000001 * 86400 * (extract (day from (conn2.end_date - conn2.start_date)) + 1) AS value,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id
    FROM (
    SELECT
        conn.connection_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone),p_end_date) AS end_date,
            c.node_calculate_parameter_id
        FROM rul_connection c
        WHERE
        	--Проверяем действует ли подключение в переданном расчетном периоде
            c.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (c.start_date < p_start_date AND COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id
        FROM rul_accounting_type_node atn
        WHERE
        	--Проверяем действует ли способ учета в переданном расчетном периоде
            atn.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (atn.start_date < p_start_date AND COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    and (acc.start_date >= conn.start_date AND acc.start_date <= conn.end_date
            OR acc.end_date > conn.start_date AND acc.end_date <= conn.end_date
            OR (acc.start_date < conn.start_date AND acc.end_date > conn.end_date))
    ) conn2
    JOIN (
        SELECT
            pv.accounting_type_node_id,
            af.formula_id,
            pv.value
        FROM rul_pipe_value pv
        JOIN rul_argument_formula af
            ON pv.argument_formula_id = af.argument_formula_id
        WHERE af.formula_id = 60
        ) pipe1
    ON pipe1.accounting_type_node_id = conn2.accounting_type_node_id
    ;
    -- Расчет формулы 63 по сечению
    INSERT INTO rul_consumption_pipe
    SELECT
       conn2.connection_id,
       conn2.connection_name,
        conn2.start_date,
        conn2.end_date,
        3.1415 * pipe1.value ^ 2 / 4 * 1.5 * 0,000001 * 86400 * (extract (day from (conn2.end_date - conn2.start_date)) + 1) AS value,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id
    FROM (
    SELECT
        conn.connection_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone),p_end_date) AS end_date,
            c.node_calculate_parameter_id
        FROM rul_connection c
        WHERE
        	--Проверяем действует ли подключение в переданном расчетном периоде
            c.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (c.start_date < p_start_date AND COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id
        FROM rul_accounting_type_node atn
        WHERE
        	--Проверяем действует ли способ учета в переданном расчетном периоде
            atn.start_date BETWEEN p_start_date AND p_end_date
            OR COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) BETWEEN p_start_date AND p_end_date
            OR (atn.start_date < p_start_date AND COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone) > p_end_date)
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    and (acc.start_date >= conn.start_date AND acc.start_date <= conn.end_date
            OR acc.end_date > conn.start_date AND acc.end_date <= conn.end_date
            OR (acc.start_date < conn.start_date AND acc.end_date > conn.end_date))
    ) conn2
    JOIN (
        SELECT
            pv.accounting_type_node_id,
            af.formula_id,
            pv.value
        FROM rul_pipe_value pv
        JOIN rul_argument_formula af
            ON pv.argument_formula_id = af.argument_formula_id
        WHERE af.formula_id = 63
        ) pipe1
    ON pipe1.accounting_type_node_id = conn2.accounting_type_node_id
    ;
    DROP TABLE temp_connections;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_formuls_source_connection(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_formuls_source_connection(IN p_connection_ids bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
	-- Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    -- В этом списке будут подключения по договору, которые не подтверждены (т.е. у них на них не сформирован счет)
    /*CREATE TEMP TABLE temp_connections AS
    SELECT DISTINCT connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL;
    */
    CREATE TEMP TABLE temp_connections AS
    SELECT DISTINCT connection_id FROM rul_connection WHERE connection_id = ANY(p_connection_ids)
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL;
    DELETE FROM rul_consumption_source_connection
    WHERE connection_id IN (SELECT connection_id FROM temp_connections)
    --WHERE connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
    AND start_date >= p_start_date and end_date <= p_end_date;
    -- Расчет формулы по подключению источнику с процентом канализации для формулы 59
    INSERT INTO rul_consumption_source_connection
    	(connection_id,connection_name,start_date,end_date,accounting_type_node_id,node_calculate_parameter_id,accounting_type_id,value,note)
    SELECT
    	source1.connection_id,
    	source1.connection_name,
      	GREATEST(source_con.start_date, source1.start_date) AS start_date,
      	LEAST(source_con.end_date, source1.end_date) AS end_date,
      	source1.accounting_type_node_id,
      	source1.node_calculate_parameter_id,
      	source1.accounting_type_id,
      	(extract (day from (LEAST(source_con.end_date, source1.end_date) - GREATEST(source_con.start_date, source1.start_date))))
      	* source_con.value * (select canalized_part from rul_connection where connection_id =  source1.source_connection_id) / 100 as value,
        round(source_con.month_value,3)::varchar||' '||
        	(SELECT unit_name FROM rul_unit WHERE unit_id =
              (SELECT unit_id FROM rul_parameter WHERE parameter_id =
                  (SELECT parameter_id FROM rul_node_calculate_parameter WHERE node_calculate_parameter_id = source1.node_calculate_parameter_id)
              )
            )
            ||', Kкан='||(select canalized_part from rul_connection where connection_id =  source1.source_connection_id)
            ||'%'
  FROM (
  SELECT
      conn.connection_id,
      rcc.source_connection_id,
      rcc.formula_id,
      conn.connection_name,
      GREATEST(conn.start_date, acc.start_date) AS start_date,
      LEAST(conn.end_date, acc.end_date) AS end_date,
      acc.accounting_type_node_id,
      acc.node_calculate_parameter_id,
      acc.accounting_type_id
  FROM (
      SELECT
          c.connection_id,
          c.connection_name,
          GREATEST(c.start_date, p_start_date) AS start_date,
          LEAST(COALESCE(c.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone),
                p_end_date) AS end_date,
          c.node_calculate_parameter_id,
          c.unaccounted_source_consumption_id
      FROM rul_connection c
      WHERE
          c.start_date < p_end_date
            AND COALESCE(c.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
  ) conn
  JOIN (
      SELECT
          atn.accounting_type_node_id,
          GREATEST(atn.start_date, p_start_date) AS start_date,
          LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone),
                p_end_date) AS end_date,
          atn.node_calculate_parameter_id,
          atn.accounting_type_id
      FROM rul_accounting_type_node atn
      WHERE
          atn.start_date < p_end_date
            AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
  ) acc
  ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
  AND acc.start_date < conn.end_date
  AND acc.end_date >= conn.start_date
  JOIN rul_connection_connection rcc ON rcc.destination_connection_id = conn.connection_id
  WHERE 1=1
  AND acc.accounting_type_id = 17 -- Только Безучетный способ учета
  AND conn.unaccounted_source_consumption_id = 4 -- Только подключение-источник
  AND rcc.formula_id = 59
  ) source1
  JOIN
  (
    select connection_id, start_date, end_date, VALUE / (extract (day from end_date - start_date)) as VALUE, note, VALUE as month_value
    from (
    select connection_id, start_date, end_date, VALUE * coefficient as value, note
    from public.rul_consumption_load
    where theoretical_calculation is false
    union all
    select connection_id, start_date, end_date, VALUE * coefficient as value, note
    FROM public.rul_consumption_standard
    where theoretical_calculation is false
    ) b
    where start_date >= p_start_date
    and end_date <= p_end_date
  ) source_con -- Получили все расходы за выбранный период, мб стоит заранее получить нужные подключения и выбрать только по ним?
  on source1.source_connection_id = source_con.connection_id
  AND source1.start_date < source_con.end_date
  AND source1.end_date >= source_con.start_date
  ;
    --Формула(метод) по теплу
    INSERT INTO rul_consumption_source_connection
    	(connection_id,connection_name,start_date,end_date,accounting_type_node_id,node_calculate_parameter_id,accounting_type_id,value,note)
    SELECT source2.connection_id,
        source2.connection_name,
        GREATEST(standard.start_date, source2.start_date) AS start_date,
        LEAST(standard.end_date, source2.end_date) AS end_date,
        source2.accounting_type_node_id,
        source2.node_calculate_parameter_id,
        source2.accounting_type_id,
        (extract (day from (LEAST(standard.end_date, source2.end_date) - GREATEST(standard.start_date, source2.start_date))))
        * source2.value * standard.value
        as value,
        round(source2.month_value,3)::varchar||' '||
        	(SELECT unit_name FROM rul_unit WHERE unit_id =
              (SELECT unit_id FROM rul_parameter WHERE parameter_id =
                  (SELECT parameter_id FROM rul_node_calculate_parameter WHERE node_calculate_parameter_id = source2.node_calculate_parameter_id)
              )
            )
            --||', Kкан='||(select canalized_part from rul_connection where connection_id =  source1.source_connection_id)
            --||'%'
    FROM (
    SELECT source1.connection_id,
        source1.connection_name,
        GREATEST(source_con.start_date, source1.start_date) AS start_date,
        LEAST(source_con.end_date, source1.end_date) AS end_date,
        source1.accounting_type_node_id,
        source1.node_calculate_parameter_id,
        source1.accounting_type_id,
        source_con.value as value,
        source1.formula_id,
        source_con.note,
        source_con.month_value
    FROM
    (
    SELECT
        conn.connection_id,
        rcc.source_connection_id,
        rcc.formula_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id,
        acc.accounting_type_id,
        conn.unaccounted_source_consumption_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone),
                  p_end_date) AS end_date,
            c.node_calculate_parameter_id,
            c.unaccounted_source_consumption_id
        FROM rul_connection c
        WHERE
            c.start_date < p_end_date
            AND COALESCE(c.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
            AND c.connection_id IN (SELECT connection_id FROM temp_connections)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone),
                  p_end_date) AS end_date,
            atn.node_calculate_parameter_id,
            atn.accounting_type_id
        FROM rul_accounting_type_node atn
        WHERE
        	atn.start_date < p_end_date
            AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    AND acc.start_date < conn.end_date
    AND acc.end_date >= conn.start_date
    JOIN rul_connection_connection rcc ON rcc.destination_connection_id = conn.connection_id
    WHERE 1=1
    AND acc.accounting_type_id = 17
    AND conn.unaccounted_source_consumption_id = 4
    AND rcc.formula_id in (select formula_id from rul_formula where method_id = 3)
    ) source1
    JOIN
    (
      select connection_id, start_date, end_date, VALUE / (extract (day from end_date - start_date)) as VALUE, note, VALUE as month_value
      from (
      select connection_id, start_date, end_date, VALUE * coefficient as value, note
      from public.rul_consumption_load
      where theoretical_calculation is false
      union all
      select connection_id, start_date, end_date, VALUE * coefficient as value, note
      FROM public.rul_consumption_standard
      where theoretical_calculation is false
      ) b
      where start_date >= p_start_date
      and end_date <= p_end_date
    ) source_con
    on source1.source_connection_id = source_con.connection_id
    AND source1.start_date < source_con.end_date
    AND source1.end_date >= source_con.start_date
    ) source2 JOIN
    (
    select value, vc.formula_id,
        GREATEST(vc.start_date, p_start_date) as start_date,
        LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date) as end_date
        from rul_version_constant vc
        JOIN rul_constant_value cv
            ON cv.version_constant_id = vc.version_constant_id
        JOIN rul_formula f
            ON vc.formula_id = f.formula_id
        JOIN rul_argument_formula af
            ON af.argument_formula_id = cv.argument_formula_id
        WHERE f.method_id = 3
        AND vc.start_date < p_end_date
        AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    ) standard
    ON source2.formula_id = standard.formula_id
    AND source2.start_date < standard.end_date
    AND source2.end_date >= standard.start_date;
    DROP TABLE temp_connections;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_formuls_standard(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_formuls_standard(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_connection_ids BIGINT[];
BEGIN
	-- Получение списка подключений, чтобы использовать его в удалениях и расчетах.
    -- В этом списке будут подключения по договору, которые не подтверждены (т.е. у них на них не сформирован счет)
    SELECT array_agg(connection_id) INTO v_connection_ids FROM (
    SELECT DISTINCT connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
      AND connection_id NOT IN (SELECT connection_id FROM rul_charge WHERE invoice_id IS NOT NULL
      							AND billing_start_date >= p_start_date
								AND billing_end_date <= p_end_date)
      AND invoice_group_index IS NOT NULL) conn;
    DELETE FROM rul_consumption_standard
    WHERE connection_id = ANY (v_connection_ids)
    --WHERE connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
    AND start_date >= p_start_date and end_date <= p_end_date;
    -- Расчет формулы 61 по нагрузке, надо переделать на расчет по методу, а не формуле.
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        (details->'K'->>'value')::NUMERIC * standard.value * (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1) AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'K'->>'code')::varchar||': '||(details->'K'->>'value')::varchar||' '||(details->'K'->>'unit')::varchar||', '||
        (details->'Vуд'->>'code')::varchar||': '||standard.value||' '||(details->'Vуд'->>'unit')::varchar||', '||
        (details->'Д<авто>'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Д<авто>'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,1::BIGINT) load1
    ON load1.connection_id = conn2.connection_id
      AND load1.start_date <= conn2.end_date
      AND load1.end_date >= conn2.start_date
    ) conn3
    join
    (select value, vc.formula_id,
    GREATEST(vc.start_date, p_start_date) as start_date,
    LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date) as end_date
    from rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 1
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    ) standard
    ON standard.start_date <= conn3.end_date
    	AND standard.end_date >= conn3.start_date
    	AND standard.formula_id = conn3.formula_id
    ;
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        standard.value_148 * (details->'F'->>'value')::NUMERIC * (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)
        * ((standard.value_150 -
        get_temperature(GREATEST(conn3.start_date, standard.start_date),LEAST(conn3.end_date, standard.end_date),conn3.node_calculate_parameter_id,1,1)
        )/(standard.value_150 - standard.value_152))
         AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'F'->>'code')::varchar||': '||(details->'F'->>'value')::varchar||' '||(details->'F'->>'unit')::varchar||', '||
        (details->'Qуд'->>'code')::varchar||': '||standard.value_148||' '||(details->'Qуд'->>'unit')::varchar||', '||
        (details->'tвн'->>'code')::varchar||': '||standard.value_150||' '||(details->'tвн'->>'unit')::varchar||', '||
        (details->'tнар.баз'->>'code')::varchar||': '||standard.value_152||' '||(details->'tнар.баз'->>'unit')::varchar||', '||
        (details->'tнар.ф<авто>'->>'code')::varchar||': '||get_temperature(GREATEST(conn3.start_date, standard.start_date),LEAST(conn3.end_date, standard.end_date),conn3.node_calculate_parameter_id,1,1)||' '||(details->'tнар.ф<авто>'->>'unit')::varchar||', '||
        (details->'Дфакт<авто>'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Дфакт<авто>'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,2::BIGINT) load1
    ON load1.connection_id = conn2.connection_id
    	AND load1.start_date <= conn2.end_date
    	AND load1.end_date >= conn2.start_date
    ) conn3
    JOIN
    (
    SELECT vc.formula_id,
        MAX(CASE WHEN af.argument_formula_code = 'Qуд' THEN cv.value END) AS value_148,
        MAX(CASE WHEN af.argument_formula_code = 'tвн' THEN cv.value END) AS value_150,
        MAX(CASE WHEN af.argument_formula_code = 'tнар.баз' THEN cv.value END) AS value_152,
        MAX(GREATEST(vc.start_date, p_start_date)) as start_date,
        MAX(LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date)) as end_date
    FROM rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 2
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    GROUP BY vc.formula_id,vc.version_constant_id
    ) standard
    ON  standard.start_date <= conn3.end_date
    	AND standard.end_date >= conn3.start_date
    	AND standard.formula_id = conn3.formula_id
    ;
    -- По методу 4 для формулы шаблона 101
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        standard.value_292 * (details->'Vф'->>'value')::NUMERIC
        *
        (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)
        /
        (extract (day from p_end_date - p_start_date) + 1)
         AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'Vф'->>'code')::varchar||': '||(details->'Vф'->>'value')::varchar||' '||(details->'Vф'->>'unit')::varchar||', '||
        (details->'Дм'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'Дм'->>'unit')::varchar||', '||
        (details->'Др'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Др'->>'unit')::varchar||', '||
        (details->'Qуд'->>'code')::varchar||': '||standard.value_292||' '||(details->'Qуд'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,4::BIGINT) load1
    	ON load1.connection_id = conn2.connection_id
    		AND load1.start_date <= conn2.end_date
    		AND load1.end_date >= conn2.start_date
    ) conn3
    JOIN
    (
    SELECT vc.formula_id,
        MAX(CASE WHEN af.argument_formula_code = 'Qуд' THEN cv.value END) AS value_292,
        MAX(GREATEST(vc.start_date, p_start_date)) as start_date,
        MAX(LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date)) as end_date
    FROM rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 4
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    GROUP BY vc.formula_id,vc.version_constant_id
    ) standard
    ON standard.start_date <= conn3.end_date
    AND standard.end_date >= conn3.start_date
    AND standard.formula_id = conn3.formula_id
    ;
    -- По методу 5 для формулы шаблона 153
    INSERT INTO rul_consumption_standard
        (
          connection_id,
          connection_name,
          start_date,
          end_date,
          value,
          formula_connection_id,
          version_load_standard_id,
          accounting_type_node_id,
          node_calculate_parameter_id,
          note
        )
    SELECT
        conn3.connection_id,
        conn3.connection_name,
        GREATEST(conn3.start_date, standard.start_date) AS start_date,
        LEAST(conn3.end_date, standard.end_date) AS end_date,
        standard.value_292 * (details->'F'->>'value')::NUMERIC
        *
        (extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)
        /
        (extract (day from p_end_date - p_start_date) + 1)
         AS value,
        conn3.formula_connection_id,
        conn3.version_load_standard_id,
        conn3.accounting_type_node_id,
        conn3.node_calculate_parameter_id,
        (details->'F'->>'code')::varchar||': '||(details->'F'->>'value')::varchar||' '||(details->'F'->>'unit')::varchar||', '||
        (details->'Дм'->>'code')::varchar||': '||(extract (day from p_end_date - p_start_date) + 1)||' '||(details->'Дм'->>'unit')::varchar||', '||
        (details->'Др'->>'code')::varchar||': '||(extract (day from (LEAST(conn3.end_date, standard.end_date) - GREATEST(conn3.start_date, standard.start_date))) + 1)||' '||(details->'Др'->>'unit')::varchar||', '||
        (details->'Qуд'->>'code')::varchar||': '||standard.value_292||' '||(details->'Qуд'->>'unit')::varchar
    FROM
    (
    SELECT
        conn2.connection_id,
        conn2.connection_name,
        GREATEST(conn2.start_date, load1.start_date) AS start_date,
        LEAST(conn2.end_date, load1.end_date) AS end_date,
        load1.formula_connection_id,
        load1.version_load_standard_id,
        load1.details,
        conn2.accounting_type_node_id,
        conn2.node_calculate_parameter_id,
        load1.formula_id
    FROM get_connection(p_start_date,p_end_date,v_connection_ids) conn2
    JOIN get_formula_details(NULL::BIGINT,p_start_date,p_end_date,5::BIGINT) load1
    	ON load1.connection_id = conn2.connection_id
    		AND load1.start_date <= conn2.end_date
    		AND load1.end_date >= conn2.start_date
    ) conn3
    JOIN
    (
    SELECT vc.formula_id,
        MAX(CASE WHEN af.argument_formula_code = 'Qуд' THEN cv.value END) AS value_292,
        MAX(GREATEST(vc.start_date, p_start_date)) as start_date,
        MAX(LEAST(COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone),p_end_date)) as end_date
    FROM rul_version_constant vc
    JOIN rul_constant_value cv
        ON cv.version_constant_id = vc.version_constant_id
    JOIN rul_formula f
        ON vc.formula_id = f.formula_id
    JOIN rul_argument_formula af
        ON af.argument_formula_id = cv.argument_formula_id
    WHERE f.method_id = 5
    	AND vc.start_date <= p_end_date
    	AND COALESCE(vc.end_date, '2100-01-31 23:59:59+03'::timestamp without time zone) >= p_start_date
    GROUP BY vc.formula_id,vc.version_constant_id
    ) standard
    ON standard.start_date <= conn3.end_date
    AND standard.end_date >= conn3.start_date
    AND standard.formula_id = conn3.formula_id
    ;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_group_accounting3(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_group_accounting3(IN p_node_calculate_parameter_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    row_record record;
    row_record2 record;
    coef numeric;
    PY_SUM numeric;
    Losses numeric;
    Charges numeric;
    v_connection_ids BIGINT[];
BEGIN
	RAISE NOTICE 'Yra, %, %, %',p_start_date,p_end_date,p_node_calculate_parameter_id;
    FOR row_record IN
    SELECT
        acc.start_date AS start_date
        ,acc.end_date AS end_date
        ,acc.accounting_type_node_id as accounting_type_node_id
        ,acc.node_calculate_parameter_id as node_calculate_parameter_id
        ,acc.accounting_type_id as accounting_type_id
    FROM
    (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id,
            atn.accounting_type_id
        FROM rul_accounting_type_node atn
        WHERE atn.start_date <= p_end_date
        	AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) > p_start_date
        	AND atn.node_calculate_parameter_id = p_node_calculate_parameter_id
    ) acc
    where acc.accounting_type_id in (2,5,17,19)
    LOOP
        -- Обработка каждой строки
        RAISE NOTICE 'Processing row: node_calculate_parameter_id=%, accounting_type_node_id=%', row_record.node_calculate_parameter_id, row_record.accounting_type_node_id;
       CREATE TEMP TABLE temp_results
    	as
        WITH RECURSIVE tree_cte AS (
            -- Базовый случай: выбираем корневые элементы
            SELECT
                zero_level.name as line_name,
                null::bigint as line_id,
                zero_level.node_id AS node_id,
                zero_level.node_id AS child_id,
                0 AS level,
                ARRAY[zero_level.node_id] AS path,
                zero_level.node_id::TEXT AS path_str,
                -- Добавляем даты для корневого элемента
                GREATEST(conn3.start_date, row_record.start_date) AS start_date,
                LEAST(conn3.end_date, row_record.end_date) AS end_date,
                conn3.accounting_type_id as accounting_type_id,
        		conn3.accounting_type_node_id as accounting_type_node_id,
                null::bigint as formula_id
    		FROM (select 'zero_level' as name, row_record.node_calculate_parameter_id as node_id) zero_level
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, row_record.start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                    	atn.start_date < row_record.end_date::timestamp without time zone
        				AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') > row_record.start_date::timestamp without time zone
            ) conn3 ON conn3.node_calculate_parameter_id = zero_level.node_id
            UNION ALL
            -- Рекурсивный случай: присоединяем детей с учетом дат родителя
            SELECT
                rl.line_name,
                rl.line_id,
                rlp.node_calculate_parameter_id AS node_id,
                rlpc.node_calculate_parameter_id AS child_id,
                t.level + 1,
                t.path || rlpc.node_calculate_parameter_id,
                t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                -- Вычисляем даты дочернего элемента на основе родительского
                GREATEST(conn3.start_date, t.start_date) AS start_date,
                LEAST(conn3.end_date, t.end_date) AS end_date,
                conn3.accounting_type_id,
                conn3.accounting_type_node_id as accounting_type_node_id,
                rlp.formula_id
            FROM tree_cte t
            JOIN public.rul_line_parameter rlp
                ON t.child_id = rlp.node_calculate_parameter_id
            JOIN public.rul_line_parameter_child rlpc
                ON rlpc.line_parameter_id = rlp.line_parameter_id
            JOIN public.rul_line rl
                ON rl.line_id = rlp.line_id
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, row_record.start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                        atn.start_date < row_record.end_date
                        AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') > row_record.start_date
            ) conn3 ON conn3.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
            WHERE rl.client_id IS NOT NULL
              -- Ограничиваем даты дочерних элементов датами родителя
              AND conn3.start_date <= t.end_date
              AND conn3.end_date >= t.start_date
              AND (t.accounting_type_id = 17 OR t.child_id = row_record.node_calculate_parameter_id) -- Дерево строиться только при безучетном расходе
        )
        SELECT
        	tree_cte.line_id,
            tree_cte.line_name,
            tree_cte.node_id,
            tree_cte.child_id,
            tree_cte.level,
            tree_cte.path,
            tree_cte.path_str,
            --GREATEST(cons.start_date, tree_cte.start_date,conn.start_date) as start_date,
            --LEAST(cons.end_date,tree_cte.end_date,conn.end_date) as end_date,
            case when tree_cte.accounting_type_id = 2 then  GREATEST(tree_cte.start_date,conn.start_date) else GREATEST(cons.start_date, tree_cte.start_date,conn.start_date) end as start_date,
            case when tree_cte.accounting_type_id = 2 then LEAST(tree_cte.end_date,conn.end_date) else LEAST(cons.end_date,tree_cte.end_date,conn.end_date) end as end_date,
            tree_cte.accounting_type_id,
            tree_cte.accounting_type_node_id,
            conn.connection_id,
            -- Добавление подтвеждений влияет на то, что подключение должно счиаться непересчитываемым по ГПУ и не подлежащим балансировке
            -- Также не должно сформироваться в новые начисления и расходы.
            -- если подтверждено подключение, мы его считает не перерасчитываемым по ГПУ
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM rul_charge rc
                    WHERE rc.connection_id = conn.connection_id
                      AND rc.charge_checked = 1
                      AND rc.billing_start_date <= p_end_date
                      AND rc.billing_end_date >= p_start_date
                ) THEN 4
                ELSE conn.group_recalculation_attitude_id
            END AS group_recalculation_attitude_id,
            conn.allocation_source_consumption_id,
            cons.value,
            cons.connection_name,
            case when (extract (day from (date_trunc('day',LEAST(cons.end_date,tree_cte.end_date,conn.end_date))
            - GREATEST(cons.start_date, tree_cte.start_date,conn.start_date)))) + 1 = 0
                    then 1
                 else (extract (day from (date_trunc('day',LEAST(cons.end_date,tree_cte.end_date,conn.end_date))
            - GREATEST(cons.start_date, tree_cte.start_date,conn.start_date)))) + 1
            end
              *
            cons.value /
                case when (extract (day from (date_trunc('day',cons.end_date) - cons.start_date))) + 1 = 0
                then 1
                else (extract (day from (date_trunc('day',cons.end_date) - cons.start_date))) + 1
                end
            / case when tree_cte.accounting_type_id <> 17 and tree_cte.level != 0
            then count(*) over (partition by tree_cte.child_id,GREATEST(cons.start_date, tree_cte.start_date,conn.start_date)
            ,LEAST(cons.end_date,tree_cte.end_date,conn.end_date))
            else 1 end
                as val,
            cons.source_consumption_id,
            tree_cte.formula_id,
            cons.note
        FROM tree_cte
        left join  (
            SELECT
                c.connection_id,
                c.connection_name,
                GREATEST(c.start_date, row_record.start_date) AS start_date,
                LEAST(COALESCE(c.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                c.node_calculate_parameter_id,
                c.unaccounted_source_consumption_id,
                c.allocation_source_consumption_id,
                c.group_recalculation_attitude_id
            FROM rul_connection c
            WHERE
                c.start_date <= row_record.end_date
                AND COALESCE(c.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
        ) conn ON tree_cte.child_id = conn.node_calculate_parameter_id
            AND tree_cte.start_date <= conn.end_date
            AND tree_cte.end_date >= conn.start_date
        LEFT JOIN
        (
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id, note
              , 1 as source_consumption_id
            FROM
              public.rul_consumption_load
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id, note
              , 2 as source_consumption_id
            FROM
              public.rul_consumption_standard
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id, note
              , 4 as source_consumption_id
            FROM
              public.rul_consumption_source_connection
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  '-----', start_date,  end_date,  value, accounting_type_node_id, 2 as accounting_type_id, null, null
            FROM
              public.rul_consumption
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
                AND value != 0
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 19 as accounting_type_id, null, null
            FROM
              public.rul_consumption_pipe
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 5 as accounting_type_id, null, null
            FROM
              public.rul_consumption_average
            WHERE 1=1
                AND start_date <= row_record.end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
        ) cons ON (cons.connection_id = conn.connection_id or cons.accounting_type_id in (2,5,19))
                AND cons.accounting_type_node_id = tree_cte.accounting_type_node_id
                AND (
                    (conn.allocation_source_consumption_id = cons.source_consumption_id
                    and (tree_cte.accounting_type_id = 17 or tree_cte.child_id = row_record.node_calculate_parameter_id))
                    or (tree_cte.accounting_type_id = 2 and cons.accounting_type_id = 2 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 5 and cons.accounting_type_id = 5 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 19 and cons.accounting_type_id = 19 and tree_cte.child_id != row_record.node_calculate_parameter_id)
                    )
                AND GREATEST(tree_cte.start_date,conn.start_date) <= cons.end_date
                AND LEAST(tree_cte.end_date,conn.end_date) >= cons.start_date
        ORDER BY path, node_id, child_id, tree_cte.start_date;
        --call public.process_formuls_losses(row_record.node_calculate_parameter_id,row_record.start_date,row_record.end_date);
        call public.process_formuls_losses(row_record.node_calculate_parameter_id,p_start_date,p_end_date);
        IF row_record.accounting_type_id = 17 THEN
        	coef := 1;
        ELSE
            IF row_record.accounting_type_id = 2 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
                  from public.rul_consumption rcons
                  --left join public.rul_connection rconn
                    --on rcons.connection_id = rconn.connection_id
                  left join public.rul_accounting_type_node ratn
                    on rcons.accounting_type_node_id = ratn.accounting_type_node_id
                  where ratn.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                  	and rcons.start_date < p_end_date
                  	and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') > p_start_date
                ) py;
            ELSIF row_record.accounting_type_id = 5 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct ratn.node_calculate_parameter_id,
                      rcona.start_date,
                      rcona.end_date,
                      rcona.value
                  from public.rul_consumption_average rcona
                  left join public.rul_accounting_type_node ratn
                    on rcona.accounting_type_node_id = ratn.accounting_type_node_id
                  --left join public.rul_connection rconn
                    --on rcona.connection_id = rconn.connection_id
                  where ratn.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                  	and rcona.start_date < p_end_date
                  	and COALESCE(rcona.end_date, '2100-04-30 23:59:59+03') > p_start_date
                ) py;
            ELSIF row_record.accounting_type_id = 19 THEN
                select sum(VALUE) into PY_SUM
                from (
                  select distinct ratn.node_calculate_parameter_id,rconp.start_date,rconp.end_date,rconp.value
                  from public.rul_consumption_pipe rconp
                  left join public.rul_accounting_type_node ratn
                    on rconp.accounting_type_node_id = ratn.accounting_type_node_id
                  --left join public.rul_connection rconn
                    --on rconp.connection_id = rconn.connection_id
                  where rconp.node_calculate_parameter_id = row_record.node_calculate_parameter_id
                  	and rconp.start_date < p_end_date
                  	and COALESCE(rconp.end_date, '2100-04-30 23:59:59+03') > p_start_date
                ) py;
            END IF;
            select sum(rcl.value) into Losses
            from temp_results tr
            join rul_consumption_losses rcl
            	on tr.line_id = rcl.line_id
            	and tr.start_date <= rcl.start_date
            	and tr.end_date >= rcl.end_date
            ;
            -- Выбираем ручные начисления которые возьмем в формулу расчета коэффициента
            select sum(rc.sum_consumption) into Charges
            from temp_results tr
            join rul_charge rc
            	on tr.connection_id = rc.connection_id
            	and tr.start_date <= rc.billing_start_date
            	and tr.end_date >= rc.billing_start_date
            	and (rc.charge_type_id = 2 -- Ручные
            	or  tr.group_recalculation_attitude_id = 4)
            ;
            with result_for_coefficient as (
            select
            Losses as poteri,
            Charges as charges_nepodl,
            PY_SUM as PY,
            SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END) as indication,
            SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END) as nepodl,
            SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END) as podl_vniz,
            SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN coalesce(val,0) END) as podl,
            case when
            coalesce(PY_SUM,0) - coalesce(Losses,0) - coalesce(Charges,0)
            - coalesce(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),0)
            - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END),0)
            - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0)
            - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN coalesce(val,0) END),0)
            > 0 then 1 else 0 end as X
            from temp_results)
            select (coalesce(PY,0) - coalesce(indication,0) - coalesce(nepodl,0) - coalesce(charges_nepodl,0) - ( X * coalesce(podl_vniz,0) ))
                    /
                    ( case when ( ( 1 - X ) * coalesce(podl_vniz,0) + coalesce(podl,0) + coalesce(poteri,0)) = 0 then 1 else ( ( 1 - X ) * coalesce(podl_vniz,0) + coalesce(podl,0) + coalesce(poteri,0)) end )
                    into coef
            from result_for_coefficient;
        END IF;
        RAISE NOTICE 'COEFICIENT %',coef;
        INSERT INTO public.rul_consumption_load
      	(
        connection_id, start_date, end_date, value, accounting_type_node_id, coefficient, theoretical_calculation, note
      	)
      	SELECT connection_id, start_date, end_date, val, accounting_type_node_id,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1)) THEN coef
        ELSE 1 END
        , false ,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1))
        THEN get_notes(row_record.accounting_type_id,temp_results.val::numeric,coef,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        ELSE get_notes(row_record.accounting_type_id,temp_results.val::numeric,1::bigint,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        END
        FROM temp_results WHERE source_consumption_id = 1
        AND NOT EXISTS (
            SELECT 1
            FROM rul_charge rc
            WHERE rc.connection_id = temp_results.connection_id
              AND rc.charge_checked = 1
              AND rc.billing_start_date <= p_end_date
              AND rc.billing_end_date >= p_start_date
        );
        INSERT INTO public.rul_consumption_standard
      	(
        connection_id, start_date, end_date, value, accounting_type_node_id, coefficient, theoretical_calculation, note
      	)
      	SELECT connection_id, start_date, end_date, val, accounting_type_node_id,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1)) THEN coef
        ELSE 1 END, FALSE ,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1))
        THEN get_notes(row_record.accounting_type_id,temp_results.val::numeric,coef,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        ELSE get_notes(row_record.accounting_type_id,temp_results.val::numeric,1::bigint,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        END
        FROM temp_results where source_consumption_id = 2
        AND NOT EXISTS (
            SELECT 1
            FROM rul_charge rc
            WHERE rc.connection_id = temp_results.connection_id
              AND rc.charge_checked = 1
              AND rc.billing_start_date <= p_end_date
              AND rc.billing_end_date >= p_start_date
        );
        INSERT INTO public.rul_consumption_source_connection
      	(
        connection_id, connection_name, start_date, end_date, value, accounting_type_node_id, coefficient, theoretical_calculation, note
      	)
      	SELECT connection_id, (select connection_name from rul_connection where connection_id = temp_results.connection_id), start_date, end_date, val, accounting_type_node_id,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1)) THEN coef
        ELSE 1 END, FALSE ,
        CASE WHEN (accounting_type_id = 17 or level = 0)
        	AND (group_recalculation_attitude_id = 3 OR (group_recalculation_attitude_id = 2 and coef < 1))
        THEN get_notes(row_record.accounting_type_id,temp_results.val::numeric,coef,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        ELSE get_notes(row_record.accounting_type_id,temp_results.val::numeric,1::bigint,row_record.node_calculate_parameter_id,p_start_date,p_end_date,start_date,end_date,note,row_record.accounting_type_node_id)
        END
        FROM temp_results  where source_consumption_id = 4
        AND NOT EXISTS (
            SELECT 1
            FROM rul_charge rc
            WHERE rc.connection_id = temp_results.connection_id
              AND rc.charge_checked = 1
              AND rc.billing_start_date <= p_end_date
              AND rc.billing_end_date >= p_start_date
        );
        UPDATE public.rul_consumption_losses
        SET coefficient = coef
        WHERE line_id in (select distinct line_id from temp_results)
        and start_date >= p_start_date
        and end_date <= p_end_date;
        SELECT array_agg(rcc.destination_connection_id)
        INTO v_connection_ids
        FROM rul_connection_connection rcc
        JOIN temp_results tr
        	ON rcc.source_connection_id = tr.connection_id;
        CALL process_formuls_source_connection(v_connection_ids,p_start_date,p_end_date);
        drop table temp_results;
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_group_accounting_new(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_group_accounting_new(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    row_record record;
    row_record2 record;
    coef numeric;
    v_conn_conn_ids bigint[];
BEGIN
	CREATE TEMP TABLE IF NOT EXISTS processed_child_ids (
    child_id INT PRIMARY KEY
	) ON COMMIT DROP;
    select array_agg(node_calculate_parameter_id) into v_conn_conn_ids
    from rul_connection rc
    join rul_connection_connection rcc
    on rc.connection_id = rcc.source_connection_id;
	-- В цикле получаем Верхние узлы деревьев из договора
    FOR row_record IN
    SELECT
        acc.start_date AS start_date
        ,acc.end_date AS end_date
        ,acc.accounting_type_node_id as accounting_type_node_id
        ,acc.node_calculate_parameter_id as node_calculate_parameter_id
        ,case when node_calculate_parameter_id = ANY (v_conn_conn_ids) then 1 else 0 end as sort
    FROM
    (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id,
            atn.accounting_type_id
        FROM rul_accounting_type_node atn
        WHERE atn.start_date <= p_end_date
        	AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) > p_start_date
        	--AND atn.node_calculate_parameter_id IN (select node_calculate_parameter_id FROM rul_connection WHERE agreement_id = p_agreement_id)
    ) acc
    where acc.accounting_type_id in (2,5,17,19)
    --where acc.accounting_type_id in (2)
    AND acc.node_calculate_parameter_id IN
    (
        SELECT DISTINCT rncp.commercial_node_calculate_parameter_id
        FROM rul_node_calculate_parameter rncp
        JOIN rul_connection rc
            ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
            AND rc.agreement_id = p_agreement_id
        WHERE rncp.commercial_node_calculate_parameter_id IS NOT NULL
    )
	ORDER BY sort DESC
    LOOP
        -- Обработка каждой строки
        RAISE NOTICE 'Обработка дерева по договору %: node_calculate_parameter_id=%, accounting_type_node_id=%',p_agreement_id, row_record.node_calculate_parameter_id, row_record.accounting_type_node_id;
        --Создаем temp дерево для цикла расчета по нему.
        FOR row_record2 IN
        WITH RECURSIVE tree_cte AS (
              -- Базовый случай: выбираем корневые элементы
              SELECT
                  zero_level.name as line_name,
                  zero_level.node_id AS node_id,
                  zero_level.node_id AS child_id,
                  0 AS level,
                  ARRAY[zero_level.node_id] AS path,
                  zero_level.node_id::TEXT AS path_str,
                  -- Добавляем даты для корневого элемента
                  GREATEST(conn3.start_date, row_record.start_date) AS start_date,
                  LEAST(conn3.end_date, row_record.end_date) AS end_date,
                  conn3.accounting_type_id as accounting_type_id,
                  conn3.accounting_type_node_id as accounting_type_node_id
              FROM (select 'zero_level' as name, row_record.node_calculate_parameter_id as node_id) zero_level
              JOIN (
                      SELECT
                          atn.accounting_type_node_id,
                          GREATEST(atn.start_date, row_record.start_date) AS start_date,
                          LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                          atn.node_calculate_parameter_id,
                          atn.accounting_type_id
                      FROM rul_accounting_type_node atn
                      WHERE
                          atn.start_date < row_record.end_date
                          AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') > row_record.start_date
              ) conn3 ON conn3.node_calculate_parameter_id = zero_level.node_id
              UNION ALL
              -- Рекурсивный случай: присоединяем детей с учетом дат родителя
              SELECT
                  rl.line_name,
                  rlp.node_calculate_parameter_id AS node_id,
                  rlpc.node_calculate_parameter_id AS child_id,
                  t.level + 1,
                  t.path || rlpc.node_calculate_parameter_id,
                  t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                  -- Вычисляем даты дочернего элемента на основе родительского
                  GREATEST(conn3.start_date, t.start_date) AS start_date,
                  LEAST(conn3.end_date, t.end_date) AS end_date,
                  conn3.accounting_type_id,
                  conn3.accounting_type_node_id as accounting_type_node_id
              FROM tree_cte t
              JOIN public.rul_line_parameter rlp
                  ON t.child_id = rlp.node_calculate_parameter_id
              JOIN public.rul_line_parameter_child rlpc
                  ON rlpc.line_parameter_id = rlp.line_parameter_id
              JOIN public.rul_line rl
                  ON rl.line_id = rlp.line_id
              JOIN (
                      SELECT
                          atn.accounting_type_node_id,
                          GREATEST(atn.start_date, row_record.start_date) AS start_date,
                          LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                          atn.node_calculate_parameter_id,
                          atn.accounting_type_id
                      FROM rul_accounting_type_node atn
                      WHERE
                          atn.start_date < row_record.end_date
                          AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') > row_record.start_date
              ) conn3 ON conn3.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
              WHERE rl.client_id IS NOT NULL
                -- Ограничиваем даты дочерних элементов датами родителя
                AND conn3.start_date <= t.end_date
                AND conn3.end_date >= t.start_date
                --AND (t.accounting_type_id = 17 OR t.child_id = row_record.node_calculate_parameter_id) -- Дерево строиться только при безучетном расходе
          )
          SELECT
              tree_cte.child_id,
              tree_cte.accounting_type_id,
              tree_cte.accounting_type_node_id
          FROM tree_cte
          left join  (
              SELECT
                  c.connection_id,
                  c.connection_name,
                  GREATEST(c.start_date, row_record.start_date) AS start_date,
                  LEAST(COALESCE(c.end_date, '2100-04-30 23:59:59+03'), row_record.end_date) AS end_date,
                  c.node_calculate_parameter_id,
                  c.unaccounted_source_consumption_id,
                  c.allocation_source_consumption_id,
                  c.group_recalculation_attitude_id
              FROM rul_connection c
              WHERE
                  c.start_date <= row_record.end_date
                  AND COALESCE(c.end_date, '2100-04-30 23:59:59+03') >= row_record.start_date
          ) conn ON tree_cte.child_id = conn.node_calculate_parameter_id
              AND tree_cte.start_date <= conn.end_date
              AND tree_cte.end_date >= conn.start_date
          WHERE
              (tree_cte.accounting_type_id <> 17 OR tree_cte.level = 0)
          GROUP BY level, child_id, accounting_type_id, tree_cte.accounting_type_node_id
          ORDER BY level desc, child_id, accounting_type_id, tree_cte.accounting_type_node_id
          LOOP
          	-- Нужно сохранять передаваемые в ГПУ параметры и не считать их повторно, кроме узла level=0. Не сделано!
			IF NOT EXISTS (SELECT 1 FROM processed_child_ids WHERE child_id = row_record2.child_id) THEN
        	-- Добавляем child_id в список обработанных
        	INSERT INTO processed_child_ids (child_id) VALUES (row_record2.child_id) ON CONFLICT (child_id) DO NOTHING;
              IF row_record2.accounting_type_id = 5 THEN
                  RAISE NOTICE 'Расчет по среднему для: node_calculate_parameter_id=%, accounting_type_node_id=%, accounting_type_node_id=%',
                      row_record2.child_id, row_record2.accounting_type_id, row_record2.accounting_type_node_id;
                  --Расчет расхода для среднего
                  CALL public.process_formuls_average(p_start_date,p_end_date,row_record2.child_id);
                  --Расчет ГПУ для среднего
                  IF row_record2.child_id = row_record.node_calculate_parameter_id THEN
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,TRUE);
                  ELSE
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,FALSE);
                  --CALL public.process_group_accounting3(row_record2.child_id,row_record.start_date,row_record.end_date,row_record2.accounting_type_node_id);
              -- Безучетный считаем только если он в верхнеуровневом коммерческом узле, иначен е трогаем
                  END IF;
              ELSIF row_record2.accounting_type_id in (2,19,17) THEN
                  RAISE NOTICE 'Расчет приборного или по сечению для: node_calculate_parameter_id=%, accounting_type_id=%, accounting_type_node_id=%',
                      row_record2.child_id, row_record2.accounting_type_id, row_record2.accounting_type_node_id;
                  --Расчет ГПУ для приборного и по сечению
                  CALL public.process_formuls_average(p_start_date,p_end_date,row_record2.child_id);
                  IF row_record2.child_id = row_record.node_calculate_parameter_id THEN
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,TRUE);
                  ELSE
                  	CALL public.new_process_group_accounting3(row_record2.child_id,p_start_date,p_end_date,FALSE);
                  --CALL public.process_group_accounting3(row_record2.child_id,row_record.start_date,row_record.end_date,row_record2.accounting_type_node_id);
              -- Безучетный считаем только если он в верхнеуровневом коммерческом узле, иначен е трогаем
                  END IF;
              END IF;
            ELSE
                RAISE NOTICE 'Пропуск повторного расчета для child_id=%', row_record2.child_id;
            END IF;
          END LOOP;
          RAISE NOTICE 'Дерево посчитано: node_calculate_parameter_id=%, accounting_type_node_id=%',
                      row_record.node_calculate_parameter_id, row_record.accounting_type_node_id;
        -- Ваша логика обработки здесь
    END LOOP;
    DROP TABLE processed_child_ids;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_invoice(IN p_invoice_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_invoice(IN p_invoice_id bigint[], IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_invoice_id bigint;
BEGIN
	-- Цикл по всем переданным счетам для рассчетов
    FOREACH v_invoice_id IN ARRAY p_invoice_id
    LOOP
    -- Проверяем, есть ли счета в болле позднем периоде по такому же коду АУ и вызываем ошибку
      IF (SELECT count(*) FROM rul_invoice
          WHERE agreement_id in (SELECT agreement_id FROM rul_agreement
          							WHERE code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = v_invoice_id)))
            AND billing_start_date >= p_end_date) != 0 THEN
          RAISE EXCEPTION '%', get_message('ERR_PROCESS_INVOICE')
          --'[[Перерасчёт невозможен: в будущих периодах уже созданы счета с этим Кодом АУ.]]'
          USING ERRCODE = '25003';
      END IF;
    -- Для пересчета надо продумать удаление.
    -- Самое неприятное будет удалять версии проводок, чтобы получить новые актуальные.
    -- Для этого нужно будет удалить версии за расчетный месяц по нужным счетам??
    -- Для отмены сторнирования надо удалить все версии у которых за текущий месяц проставлено айди сторнирования
    -- upd Сделал в следующем методе. Не тестировалось нормально
      call public.clear_all_for_recalculate_invoice(v_invoice_id,p_start_date,p_end_date);
      -- Создаем версии проводок у которых нет на данный момент ни одной.
      -- Убрал из метода удаления, т.к. тот метод используется при удалении проводок и начислений
      INSERT INTO rul_transaction_version (payment_percent,create_date,month,transaction_id)
        SELECT 0,
               CURRENT_TIMESTAMP::timestamp(0),
               date_trunc('month',p_start_date),
               rt.transaction_id
        FROM rul_transaction rt
        LEFT JOIN rul_transaction_version rtv
        ON rt.transaction_id = rtv.transaction_id
        WHERE rtv.transaction_id is null
        AND rt.operation_date < p_end_date
        AND rt.code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = v_invoice_id))
        ;
      -- Сторнирование проводок, текущего месяца
      call public.create_storn_pay(v_invoice_id,p_start_date,p_end_date);
      -- Не сделано.
      -- После сторнирования по идее должны пересчитываться пени и индексации за прошлые месяцы и
      -- создаваться их корректировки и куда-то выставляться. Возможно они не должны попасть в стандартные рассчеты,
      -- а должны сформироваться как-то отдельно. Возможно это надо запихнуть в сам метод сторнирования (Это даже более вероятно,
      -- т.к. там цикл по сторниующим проводкам)
      -- Формирование проводок из начислений по счету
      call public.create_transaction(v_invoice_id,p_start_date,p_end_date);
      -- Погашение проводок
      -- Счет не используется и скорее всего не будет, т.к. будет какой-то код АУ
      -- Который вроде как будет приходить из импорта, и также где-то вестись, чтобы можно было ограничить расчеты
      call public.create_transaction_pay(v_invoice_id,p_start_date,p_end_date);
      -- Создание индексации по погашенным проводкам
      call public.create_indexing(v_invoice_id,p_start_date,p_end_date);
      -- Создание проводок по этим индексациям
      call public.create_indexing_transaction(v_invoice_id,p_start_date,p_end_date);
      -- Создание пени
      call public.create_penalty(v_invoice_id,p_start_date,p_end_date);
      -- Создание проводок по пене
      call public.create_penalty_transaction(v_invoice_id,p_start_date,p_end_date);
      -- Обновление данных по счету.
      -- Пока выбираются не совсем верные суммы, т.к. нужно после доделки расчета индексации после сторнирования
      -- нужно проверить учитываются ли они в эти суммы. Т.е. к примеру сумма индексации на текущий период включает в себя
      -- помимо своей индексации еще и пересчитанную разницу из-за сторнирования и т.д.
      UPDATE rul_invoice SET
        indexing_amount = (select round(sum(ri.index_amount),2)
                          from rul_indexing ri
                          join rul_charge rc
                          on rc.charge_id = ri.charge_id
                          where rc.nds_percent is not null
                          and ri.invoice_id = v_invoice_id)
      , indexing_amount_unnds = (select round(sum(ri.index_amount),2)
                                  from rul_indexing ri
                                  join rul_charge rc
                                  on rc.charge_id = ri.charge_id
                                  where rc.nds_percent is null
                                  and ri.invoice_id = v_invoice_id)
      , indexing_nds = (select round(sum(ri.index_nds),2)
                          from rul_indexing ri
                          join rul_charge rc
                          on rc.charge_id = ri.charge_id
                          where ri.invoice_id = v_invoice_id)
      -- Пеню распределяем в пропорции по начислениям облагаемых ндс и необалагемых, т.к. прямой связи как в индексации нету
      , penalty_amount = round((select sum(rp.penalty_value)
                          from rul_penalty rp
                          where rp.invoice_id = v_invoice_id) * coalesce(sum_amount,0) / (coalesce(sum_amount,0) + coalesce(sum_amount_unnds,0)),2)
      , penalty_amount_unnds = round((select sum(rp.penalty_value)
                          from rul_penalty rp
                          where rp.invoice_id = v_invoice_id) * coalesce(sum_amount_unnds,0) / (coalesce(sum_amount,0) + coalesce(sum_amount_unnds,0)),2)
      , penalty_nds = (select round(sum(rp.penalty_nds_value),2)
                          from rul_penalty rp
                          where rp.invoice_id = v_invoice_id)
      WHERE invoice_id = v_invoice_id;
      UPDATE rul_invoice SET
      -- Для расчета сальдо берутся все заведенные отслеживаемые счета
      -- и по ним из таблицы проводок собирается все, что пришло на дебет и на кредит
      balance = (
      select
          round((
          select coalesce(sum(amount),0) from rul_transaction rt
            where rt.debit_subinvoice in
            (
            select main_subinvoice from (
                -- Дебетовые счета по начислениям
                select rot.main_subinvoice
                from rul_charge rc
                join rul_connection rcc
                    on rc.connection_id = rcc.connection_id
                join rul_operation_template rot
                    on rot.operation_template_id = rcc.service_operation_template_id
                where rc.invoice_id = v_invoice_id
                union all
                -- Дебетовые счета по индексации
                select rot.main_subinvoice
                from rul_charge rc
                join rul_connection rcc
                    on rc.connection_id = rcc.connection_id
                join rul_operation_template rot
                    on rot.operation_template_id = rcc.indexing_operation_template_id
                where rc.invoice_id = v_invoice_id
                union all
                -- Дебетовые счета по пене
                select rot.main_subinvoice
                from rul_agreement ra
                join rul_invoice ri
                    on ri.agreement_id = ra.agreement_id
                join rul_operation_template rot
                    on rot.operation_template_id = ra.penalty_operation_template_id
                where ri.invoice_id = v_invoice_id
            ) debit
            group by main_subinvoice
            )
            and rt.operation_date <= p_start_date
            --and rt.operation_date <= p_end_date
            and rt.code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = v_invoice_id))
         ),2)
         -
          round((
          select coalesce(sum(amount),0) from rul_transaction rt
            where rt.credit_subinvoice in
            (
              select main_subinvoice from (
                  -- Дебетовые счета по начислениям
                  select rot.main_subinvoice
                  from rul_charge rc
                  join rul_connection rcc
                      on rc.connection_id = rcc.connection_id
                  join rul_operation_template rot
                      on rot.operation_template_id = rcc.service_operation_template_id
                  where rc.invoice_id = v_invoice_id
                  union all
                  -- Дебетовые счета по индексации
                  select rot.main_subinvoice
                  from rul_charge rc
                  join rul_connection rcc
                      on rc.connection_id = rcc.connection_id
                  join rul_operation_template rot
                      on rot.operation_template_id = rcc.indexing_operation_template_id
                  where rc.invoice_id = v_invoice_id
                  union all
                  -- Дебетовые счета по пене
                  select rot.main_subinvoice
                  from rul_agreement ra
                  join rul_invoice ri
                      on ri.agreement_id = ra.agreement_id
                  join rul_operation_template rot
                      on rot.operation_template_id = ra.penalty_operation_template_id
                  where ri.invoice_id = v_invoice_id
              ) debit
              group by main_subinvoice
            )
            and rt.operation_date <= p_start_date
            --and rt.operation_date <= p_end_date
            and rt.code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = v_invoice_id))
          ) ,2)
      ),
      pay_value = round((
          select coalesce(sum(amount),0) from rul_transaction rt
            where rt.credit_subinvoice in
            (
              select main_subinvoice from (
                  -- Дебетовые счета по начислениям
                  select rot.main_subinvoice
                  from rul_charge rc
                  join rul_connection rcc
                      on rc.connection_id = rcc.connection_id
                  join rul_operation_template rot
                      on rot.operation_template_id = rcc.service_operation_template_id
                  where rc.invoice_id = v_invoice_id
                  union all
                  -- Дебетовые счета по индексации
                  select rot.main_subinvoice
                  from rul_charge rc
                  join rul_connection rcc
                      on rc.connection_id = rcc.connection_id
                  join rul_operation_template rot
                      on rot.operation_template_id = rcc.indexing_operation_template_id
                  where rc.invoice_id = v_invoice_id
                  union all
                  -- Дебетовые счета по пене
                  select rot.main_subinvoice
                  from rul_agreement ra
                  join rul_invoice ri
                      on ri.agreement_id = ra.agreement_id
                  join rul_operation_template rot
                      on rot.operation_template_id = ra.penalty_operation_template_id
                  where ri.invoice_id = v_invoice_id
              ) debit
              group by main_subinvoice
            )
            and rt.operation_date >= p_start_date
            and rt.operation_date <= p_end_date
            and rt.code_ay = (SELECT code_ay FROM rul_agreement WHERE agreement_id =
                                      (SELECT agreement_id FROM rul_invoice WHERE invoice_id = v_invoice_id))
          ) ,2)
      WHERE invoice_id = v_invoice_id
      -- Считаем баланс и оплачено за период только для ИГС 1
      AND invoice_group_index = 1;
      UPDATE rul_invoice
      SET total_amount = coalesce(sum_amount,0) + coalesce(sum_amount_unnds,0) + coalesce(sum_nds,0) +
      					 coalesce(indexing_amount,0) + coalesce(indexing_amount_unnds,0) + coalesce(indexing_nds,0) +
          				 coalesce(penalty_amount,0) + coalesce(penalty_amount_unnds,0) + coalesce(penalty_nds,0)
      WHERE invoice_id = v_invoice_id;
    END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.process_zero_charges(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.process_zero_charges(IN p_agreement_id bigint, IN p_start_date timestamp without time zone, IN p_end_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
        -- Неправильно! надо source_id вытягивать из расчетного расхода подключения
        INSERT INTO
              public.rul_charge
            (
              connection_id,
              sum_consumption,
              amount,
              nds_percent,
              note,
              start_date,
              end_date,
              base_value,
              billing_start_date,
              billing_end_date,
              charge_type_id,
              nds_rub,
              amount_nds,
              cost_factor,
        	  currency_rate,
              comitet_resolution,
              source_id,
              invoice_group_index
            )
        SELECT
            connection_id,
            ROUND(consumption,3),
            ROUND(ROUND(consumption,3) * rrv.base_value,2),
            rrv.nds,
            note,
            rcd.start_date,
            rcd.end_date,
            rrv.base_value,
            p_start_date,
            p_end_date,
            1,
            ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            ROUND(consumption * rrv.base_value,2) + ROUND(ROUND(consumption * rrv.base_value,2) * rrv.nds / 100,2),
            rrv.cost_factor,
        	rrv.currency_rate,
            rrv.comitet_resolution,
            1,
            rcd.invoice_group_index
        FROM (
              select
                  connection_id,
                  sum(consumption) as consumption,
                  string_agg(note, ' - ') as note,
                  rate_value_id,
                  min(start_date) as start_date,
                  max(end_date) as end_date,
                  invoice_group_index
              from (
                  	WITH calc_charges AS (
                    SELECT
                        rc.connection_id,
                        0 AS consumption,
                        GREATEST(MAX(rc.start_date),p_start_date) AS start_date,
                        LEAST(MAX(COALESCE(rc.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone)),p_end_date) AS end_date,
                        rc.rate_id,
                        'Отсутствуют данные по расходам' as description,
                        rc.invoice_group_index
                    FROM rul_connection rc
                    LEFT JOIN rul_charge rch
                    	ON rch.connection_id = rc.connection_id
                        and rch.billing_start_date >= p_start_date
                        and rch.billing_end_date <= p_end_date
                    WHERE 1=1 -- Надо поставить false, пока заглушка
                    AND rc.connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id
                    						 AND invoice_group_index IS NOT NULL)
                    and rc.start_date <= p_end_date
                    and COALESCE(rc.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
                    and rch.charge_id is null
                    GROUP BY rc.connection_id, rc.rate_id, rc.invoice_group_index
                    ),
                    time_calcs AS (
                        SELECT
                            charges.connection_id,
                            charges.consumption,
                            rrv.start_date AS rate_start,
                            rrv.end_date AS rate_end,
                            LEAST(COALESCE(rrv.end_date, charges.end_date),
                                         charges.end_date) AS period_end,
                            GREATEST(rrv.start_date, charges.start_date) AS period_start,
                            EXTRACT(day FROM charges.end_date - charges.start_date) + 1 AS total_days,
                            rrv.rate_value_id, -- Добавляем rate_value_id сюда
                            charges.description,
                            charges.invoice_group_index
                        FROM calc_charges charges
                        JOIN rul_rate_value rrv
                            ON charges.rate_id = rrv.rate_id
                            AND ((charges.start_date < COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                            AND charges.end_date > rrv.start_date) OR
                             (rrv.start_date BETWEEN charges.start_date AND charges.end_date
                                OR COALESCE(rrv.end_date, '2100-01-01 00:00:00+03'::timestamp)
                                    BETWEEN charges.start_date AND charges.end_date))
                    )
                    SELECT
                        t.period_start as start_date,
                        t.period_end as end_date,
                        ROUND(CASE
                            WHEN t.total_days = 0 THEN 0
                            ELSE (t.consumption * (EXTRACT(day FROM t.period_end - t.period_start) + 1)  / t.total_days):: numeric
                        END,3) AS consumption,
                        t.rate_value_id,
                        t.connection_id,
                        t.description as note,
                        t.invoice_group_index
                    FROM time_calcs t
                    ) as rul_charge_detail
            where 1=1
            and start_date >= p_start_date
            and end_date <= p_end_date
            AND connection_id IN (select connection_id FROM rul_connection WHERE agreement_id = p_agreement_id)
            group by connection_id,rate_value_id,invoice_group_index
            ) rcd
        JOIN
            rul_rate_value rrv
        ON
            rcd.rate_value_id = rrv.rate_value_id;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.update_currency_rate()
CREATE OR REPLACE PROCEDURE public.update_currency_rate()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
ref_json json;
i record;
l_usd numeric;
BEGIN
ref_json := get_json_from_url('https://api.nbrb.by/exrates/rates?ondate='|| TO_CHAR(current_date, 'yyyy-mm-dd') ||'&periodicity=0');
FOR i IN
(
  SELECT * FROM
      (
            SELECT
          elem->>'Cur_Abbreviation' AS currency_code,
          (elem->>'Date')::timestamp AS currency_rate_date,
          (elem->>'Cur_OfficialRate')::numeric AS currency_rate,
          (elem->>'Cur_Scale')::numeric AS currency_scale
        FROM json_array_elements(ref_json) AS elem
      ) ref_table
    WHERE EXISTS (SELECT * FROM rul_currency bc WHERE bc.currency_code = ref_table.currency_code)
)
 LOOP
   IF i.currency_code NOT IN (SELECT currency_code FROM rul_currency_rate) THEN
   	INSERT INTO rul_currency_rate
				(currency_code,
				currency_rate_date,
				currency_rate)
    VALUES
		(i.currency_code,
		 i.currency_rate_date,
		 round(i.currency_rate,4));
	ELSEIF i.currency_rate_date NOT IN (SELECT currency_rate_date FROM rul_currency_rate bcr WHERE i.currency_code = bcr.currency_code) THEN
	 INSERT INTO rul_currency_rate
				(currency_code,
				currency_rate_date,
				currency_rate)
    VALUES
		(i.currency_code,
		 i.currency_rate_date,
		 round(i.currency_rate,4));
	END IF;
END LOOP;
END;
$procedure$

-- ======================================================================

-- PROCEDURE: public.update_transaction(IN p_source_correlation_transaction_id bigint, IN p_storn_correlation_transaction_id bigint, IN p_calculate_date timestamp without time zone)
CREATE OR REPLACE PROCEDURE public.update_transaction(IN p_source_correlation_transaction_id bigint, IN p_storn_correlation_transaction_id bigint, IN p_calculate_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	rec record;
	old_storn_id bigint;
    old_calculate_date timestamp;
    v_amount numeric;
    /*v_start_date timestamp:= (SELECT date_trunc('month',MAX(operation_date)) FROM rul_transaction
    							WHERE correlation_transaction_id = p_source_correlation_transaction_id);
    v_end_date timestamp:= (SELECT date_trunc('month',MAX(operation_date)) + interval '1 month' - interval '1 second' FROM rul_transaction
    							WHERE correlation_transaction_id = p_source_correlation_transaction_id);*/
    v_start_date timestamp:= (SELECT date_trunc('month',MAX(operation_date)) FROM rul_transaction
    							WHERE correlation_transaction_id = p_storn_correlation_transaction_id);
    v_end_date timestamp:= (SELECT date_trunc('month',MAX(operation_date)) + interval '1 month' - interval '1 second' FROM rul_transaction
    							WHERE correlation_transaction_id = p_storn_correlation_transaction_id);
    v_storn_id bigint := p_source_correlation_transaction_id;
    v_source_id bigint := p_storn_correlation_transaction_id;
BEGIN
	--p_storn_correlation_transaction_id := p_source_correlation_transaction_id;
	--p_source_correlation_transaction_id := v_storn_id;
	-- Надо понять какая проводка, сторнирующая или обычная и понять изменилась ли дата и что изменилось
    -- Затем менять
    SELECT coalesce(storn_correlation_transaction_id,0) INTO old_storn_id FROM rul_transaction_reversal
    			WHERE source_correlation_transaction_id = v_source_id
                AND deleted = 0::smallint;
    SELECT MAX(operation_date),MAX(amount) INTO old_calculate_date,v_amount FROM rul_transaction
    			WHERE correlation_transaction_id = v_source_id;
    -- Если расчетная дата поменялась, то теперь это коррекстирующая проводка
   IF old_storn_id IS NULL AND v_storn_id IS NOT NULL
       THEN -- Заведение сторнирования, дата в данном случае не выбираемая (т.к. используется как фильтр)
         INSERT INTO rul_transaction_reversal (source_correlation_transaction_id,storn_correlation_transaction_id,amount)
         SELECT v_source_id,
                v_storn_id,
                v_amount;
   ELSEIF old_storn_id IS NULL AND v_storn_id IS NULL
       THEN -- Исправление обычной проводки (если дата не менялась, то и менять нечего)
         IF coalesce(p_calculate_date,'2000-01-01 00:00:00+03') != coalesce(old_calculate_date,'2000-01-01 00:00:00+03')
         THEN
         	-- Обновляем рассчетную дату и создаем корректирующую проводку
         	UPDATE rul_transaction SET calculated_date = p_calculate_date
        	WHERE correlation_transaction_id = v_source_id;
            INSERT INTO rul_transaction_reversal (source_correlation_transaction_id,storn_correlation_transaction_id,amount)
            SELECT v_source_id,
                   null,
                   v_amount;
         END IF;
   ELSEIF old_storn_id IS NOT NULL AND v_storn_id IS NULL
       THEN -- Удаление именно сторнирования
         UPDATE rul_transaction_reversal SET deleted = 1::smallint where source_correlation_transaction_id = v_source_id;
         IF coalesce(p_calculate_date,'2000-01-01 00:00:00+03') != coalesce(old_calculate_date,'2000-01-01 00:00:00+03')
         THEN
         	-- Обновляем рассчетную дату и создаем корректирующую проводку
         	UPDATE rul_transaction SET calculated_date = p_calculate_date
        	WHERE correlation_transaction_id = v_source_id;
            INSERT INTO rul_transaction_reversal (source_correlation_transaction_id,storn_correlation_transaction_id,amount)
            SELECT v_source_id,
                   null,
                   v_amount;
         END IF;
   ELSEIF old_storn_id IS NOT NULL AND v_storn_id IS NOT NULL
       THEN -- Перепривязывание проводки
         UPDATE rul_transaction_reversal SET deleted = 1::smallint where source_correlation_transaction_id = v_source_id;
         INSERT INTO rul_transaction_reversal (source_correlation_transaction_id,storn_correlation_transaction_id,amount)
         SELECT v_source_id,
                v_storn_id,
                v_amount;
   END IF;
   FOR rec IN
    	SELECT invoice_id FROM rul_invoice WHERE agreement_id IN
        	(SELECT agreement_id FROM rul_agreement WHERE code_ay IN (SELECT code_ay FROM rul_transaction
               WHERE correlation_transaction_id IN (p_source_correlation_transaction_id,p_storn_correlation_transaction_id))
        	)
        AND billing_start_date = v_start_date
        AND billing_end_date = v_end_date
    LOOP
    	RAISE NOTICE 'Запуск пересчета по %, %, %', rec.invoice_id, v_start_date, v_end_date;
    	CALL public.process_invoice(ARRAY[rec.invoice_id], v_start_date, v_end_date);
    END LOOP;
END;
$procedure$

-- ======================================================================

-- FUNCTION: public.count_weekday_in_period(start_date timestamp without time zone, end_date timestamp without time zone, target_weekday integer)
CREATE OR REPLACE FUNCTION public.count_weekday_in_period(start_date timestamp without time zone, end_date timestamp without time zone, target_weekday integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    result_count INT;
BEGIN
    SELECT COUNT(*) INTO result_count
    FROM generate_series(
        date_trunc('day', start_date),
        date_trunc('day', end_date),
        INTERVAL '1 day'
    ) AS d
    WHERE EXTRACT(ISODOW FROM d) = target_weekday;
    RETURN result_count;
END;
$function$

-- ======================================================================

-- FUNCTION: public.count_weekdays(start_ts timestamp without time zone, end_ts timestamp without time zone)
CREATE OR REPLACE FUNCTION public.count_weekdays(start_ts timestamp without time zone, end_ts timestamp without time zone)
 RETURNS TABLE(weekdays bigint, weekends bigint)
 LANGUAGE plpgsql
AS $function$
/*DECLARE
    start_date date := start_ts::date;
    end_date   date := end_ts::date;
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM day) NOT IN (6, 7)) AS weekdays,
        COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM day) IN (6, 7))     AS weekends
    FROM generate_series(start_date, end_date, '1 day') AS day;
END;*/
-- Метод возвращает количество дней выходных и будних с учетом празников за запрошенный период
DECLARE
    start_date date := start_ts::date;
    end_date   date := end_ts::date;
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*) FILTER (WHERE is_weekend = FALSE) AS weekdays,
        COUNT(*) FILTER (WHERE is_weekend = TRUE) AS weekends
    FROM (
        SELECT
            day,
            CASE
                WHEN EXTRACT(ISODOW FROM day) IN (6, 7) THEN TRUE
                WHEN h.holiday_date IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_weekend
        FROM generate_series(start_date, end_date, '1 day'::interval) AS day
        LEFT JOIN rul_holiday h ON h.holiday_date::date = day
        WHERE (h.deleted is null or h.deleted =  0)  -- если есть флаг удаления
    ) AS days_with_flags;
END;
$function$

-- ======================================================================

-- FUNCTION: public.count_weekdays_for_every_day(start_date timestamp without time zone, end_date timestamp without time zone, work_holidays smallint)
CREATE OR REPLACE FUNCTION public.count_weekdays_for_every_day(start_date timestamp without time zone, end_date timestamp without time zone, work_holidays smallint DEFAULT 1)
 RETURNS TABLE(day_name text, day_count bigint)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        to_char(d, 'Day'),
        count(*) AS day_count
    FROM generate_series(
        date_trunc('day', start_date),
        date_trunc('day', end_date),
        interval '1 day'
    ) AS d
    WHERE
        (work_holidays = 1)  -- Если не нужно исключать — пропускаем проверку
        OR (
            work_holidays = 0
            AND NOT EXISTS (
                SELECT 1
                FROM rul_holiday h
                WHERE h.holiday_date = d
                  AND h.deleted = 0  -- Только активные праздники
            )
        )
    GROUP BY to_char(d, 'IDay'), to_char(d, 'Day')
    ORDER BY to_char(d, 'IDay');
END;
$function$

-- ======================================================================

-- FUNCTION: public.find_balance_node(node_calculate_parameter_id bigint, p_start_date timestamp without time zone, p_end_date timestamp without time zone)
CREATE OR REPLACE FUNCTION public.find_balance_node(node_calculate_parameter_id bigint, p_start_date timestamp without time zone, p_end_date timestamp without time zone)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    result bigint;
BEGIN
    WITH RECURSIVE tree_cte AS (
        SELECT
            zero_level.node_id AS node_id,
            zero_level.node_id AS child_id,
            GREATEST(conn3.start_date, p_start_date) AS start_date,
            LEAST(conn3.end_date, p_end_date) AS end_date,
            null::bigint as node_type_id
        FROM (select 'zero_level' as name, node_calculate_parameter_id as node_id) zero_level
        JOIN (
                SELECT
                    atn.accounting_type_node_id,
                    GREATEST(atn.start_date, p_start_date) AS start_date,
                    LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), p_end_date) AS end_date,
                    atn.node_calculate_parameter_id,
                    atn.accounting_type_id
                FROM rul_accounting_type_node atn
                WHERE
                    atn.start_date < p_end_date
                    AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= p_start_date
        ) conn3 ON conn3.node_calculate_parameter_id = zero_level.node_id
        UNION ALL
        -- Рекурсивный случай: присоединяем детей с учетом дат родителя
        SELECT
            rlpc.node_calculate_parameter_id AS node_id,
            rlp.node_calculate_parameter_id AS child_id,
            GREATEST(conn3.start_date, t.start_date) AS start_date,
            LEAST(conn3.end_date, t.end_date) AS end_date,
            rn.node_type_id
        FROM tree_cte t
        JOIN public.rul_line_parameter_child rlpc
            ON t.child_id = rlpc.node_calculate_parameter_id
        JOIN public.rul_line_parameter rlp
            ON rlpc.line_parameter_id = rlp.line_parameter_id
        JOIN public.rul_line rl
            ON rl.line_id = rlp.line_id
        JOIN (
                SELECT
                    atn.accounting_type_node_id,
                    GREATEST(atn.start_date, p_start_date) AS start_date,
                    LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), p_end_date) AS end_date,
                    atn.node_calculate_parameter_id,
                    atn.accounting_type_id
                FROM rul_accounting_type_node atn
                WHERE
                    atn.start_date <= p_end_date
                    AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') >= p_start_date
        ) conn3 ON conn3.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
        JOIN public.rul_node_calculate_parameter rncp
            ON rncp.node_calculate_parameter_id = rlp.node_calculate_parameter_id
        JOIN public.rul_node rn
            ON rn.node_id = rncp.node_id
        JOIN public.rul_node_calculate_parameter rncp2
            ON rncp2.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
        JOIN public.rul_node rn2
            ON rn2.node_id = rncp2.node_id
        WHERE rl.client_id IS NOT NULL
          AND conn3.start_date <= t.end_date
          AND conn3.end_date >= t.start_date
          AND rn2.node_type_id != 3 --Завершаем на первом балансном узле
        )
    SELECT
        child_id into result
    FROM tree_cte
    WHERE
        node_type_id = 3;
    RETURN result;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_average_value(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_formula_id bigint)
CREATE OR REPLACE FUNCTION public.get_average_value(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_formula_id bigint)
 RETURNS TABLE(argument_id character varying, argument_value character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	v_cnt bigint:=0;
    v_formula_id bigint;
    v_result numeric;
    v_start_date timestamp;
    v_end_date timestamp;
    v_err bigint:=0;
    v_temperature numeric;
    v_err_name varchar(1024);
    v_count bigint;
    v_locality_id bigint;
    v_avg numeric;
BEGIN
    SELECT COUNT(*)
    INTO v_cnt
    FROM rul_accounting_type_node
    WHERE node_calculate_parameter_id = p_node_calculate_parameter_id
    	AND start_date <= p_end_date
    	AND COALESCE(end_date,'2100-10-31 23:59:59+03'::timestamp) >= p_start_date
        AND accounting_type_id = 2;
	IF v_cnt != 1 THEN
    	RAISE EXCEPTION '%', get_message('ERR_GET_AVERAGE_VALUE');
    ELSE
    -- Поулчаем показания в рамках РП
        CREATE TEMP TABLE indication AS
        SELECT node_panel_id,
               /*CASE WHEN start_date < p_start_date
                    THEN p_start_date
                    ELSE start_date END as*/
               start_date,
               end_date, consumption, value_number, argument_formula_id, formula_id,
               row_number() OVER(partition by node_panel_id order by end_date) as rn,
               CASE WHEN start_date = MIN(start_date) OVER (PARTITION BY node_panel_id) THEN 1
                    WHEN end_date = MAX(end_date) OVER (PARTITION BY node_panel_id) THEN 1
               ELSE 0 END first_last_value
        FROM (
            SELECT nd.node_panel_id,
                   LAG(check_date) OVER (PARTITION BY nd.node_panel_id ORDER BY nd.node_panel_id, nd.check_date)
                            AS start_date,
                   check_date AS end_date,
                   CASE
                       WHEN p.indication_type_id = 2 THEN (COALESCE(value_number, 0) +
                            LAG(COALESCE(value_number, 0)) OVER (PARTITION BY nd.node_panel_id ORDER BY nd.node_panel_id, nd.check_date))/2
                       WHEN p.indication_type_id = 1 THEN COALESCE(value_number, 0) -
                            LAG(COALESCE(value_number, 0)) OVER (PARTITION BY nd.node_panel_id ORDER BY nd.node_panel_id, nd.check_date)
                   END AS consumption,
                   COALESCE(value_number, 0) AS value_number,
                   rnpa.argument_formula_id,
                   raf.formula_id
                FROM rul_node_calculate_parameter rncp
                JOIN (
                    SELECT node_calculate_parameter_id, start_date, accounting_type_id, accounting_type_node_id,
                           COALESCE(end_date, p_end_date) AS end_date
                    FROM rul_accounting_type_node
                ) ratn
                    ON ratn.node_calculate_parameter_id = rncp.node_calculate_parameter_id
                    AND ratn.start_date <= p_end_date
                    AND ratn.end_date >= p_start_date
                    AND ratn.accounting_type_id = 2
                JOIN rul_node_meter rnm
                    ON rnm.node_id = rncp.node_id AND ratn.start_date >= rnm.start_date
                JOIN rul_node_panel rnp
                    ON rnp.node_meter_id = rnm.node_meter_id
                JOIN rul_node_panel_argument rnpa
                    ON ratn.accounting_type_node_id = rnpa.accounting_type_node_id
                    AND rnpa.node_panel_id = rnp.node_panel_id
                JOIN rul_node_panel_value nd
                    ON nd.node_panel_id = rnp.node_panel_id
                JOIN rul_panel p
                    ON rnp.panel_id = p.panel_id
                JOIN rul_argument_formula raf
                    ON raf.argument_formula_id = rnpa.argument_formula_id
                WHERE 1=1
                    AND nd.is_correct = 1
                    AND nd.deleted = 0
                    AND rncp.node_calculate_parameter_id = p_node_calculate_parameter_id
                    AND nd.check_date <= p_end_date
            ORDER BY nd.node_panel_id, nd.check_date
        ) a
        WHERE end_date > p_start_date;
        -- Дальше можно через формулу
        SELECT MAX(formula_id) INTO v_formula_id
		FROM indication WHERE first_last_value = 1;
       	IF v_formula_id = 1
        	THEN
              SELECT sum(consumption)/(EXTRACT (DAY FROM MAX(end_date) - MIN(start_date)) + 1),
              MIN(start_date),
              MAX(end_date)
              INTO v_result, v_start_date, v_end_date
              FROM indication;
        ELSIF v_formula_id = 2
        	THEN
            	SELECT sum(result_consumption)/(EXTRACT (DAY FROM MAX(end_date) - MIN(start_date)) + 1),
                MIN(start_date),
                MAX(end_date),
                MAX(err)
                INTO v_result, v_start_date, v_end_date, v_err
                FROM
                (
                  SELECT i1.consumption - i2.consumption as result_consumption,i1.start_date,i1.end_date,
                      CASE WHEN i1.start_date IS NULL OR i2.start_date IS NULL THEN 1 ELSE 0 END as err
                  FROM indication i1
                  FULL JOIN indication i2
                      ON COALESCE(i1.start_date,p_start_date) = COALESCE(i2.start_date,p_start_date)
                      AND i1.end_date = i2.end_date
                  WHERE i1.argument_formula_id = 2
                  AND i2.argument_formula_id = 3
                ) temp_consumption;
        ELSIF v_formula_id = 15
        	THEN
            	SELECT sum(result_consumption)/(EXTRACT (DAY FROM MAX(end_date) - MIN(start_date)) + 1),
                MIN(start_date),
                MAX(end_date),
                MAX(err)
                INTO v_result, v_start_date, v_end_date, v_err
                FROM
                (
                  SELECT i1.consumption + i2.consumption as result_consumption,i1.start_date,i1.end_date,
                      CASE WHEN i1.start_date IS NULL OR i2.start_date IS NULL THEN 1 ELSE 0 END as err
                  FROM indication i1
                  FULL JOIN indication i2
                      ON COALESCE(i1.start_date,p_start_date) = COALESCE(i2.start_date,p_start_date)
                      AND i1.end_date = i2.end_date
                  WHERE i1.argument_formula_id = 16
                  AND i2.argument_formula_id = 17
                ) temp_consumption;
        ELSIF v_formula_id = 16
        	THEN
              SELECT sum(consumption)/(EXTRACT (DAY FROM MAX(end_date) - MIN(start_date)) + 1),
              MIN(start_date),
              MAX(end_date)
              INTO v_result, v_start_date, v_end_date
              FROM indication;
        ELSIF v_formula_id = 17
        	THEN
              SELECT sum(consumption)/(EXTRACT (DAY FROM MAX(end_date) - MIN(start_date)) + 1),
              MIN(start_date),
              MAX(end_date)
              INTO v_result, v_start_date, v_end_date
              FROM indication;
        ELSIF v_formula_id = 18
        	THEN
            	SELECT sum(result_consumption)/(EXTRACT (DAY FROM MAX(end_date) - MIN(start_date)) + 1),
                MIN(start_date),
                MAX(end_date),
                MAX(err)
                INTO v_result, v_start_date, v_end_date, v_err
                FROM
                (
                  SELECT i1.consumption - i2.consumption as result_consumption,i1.start_date,i1.end_date,
                      CASE WHEN i1.start_date IS NULL OR i2.start_date IS NULL THEN 1 ELSE 0 END as err
                  FROM indication i1
                  FULL JOIN indication i2
                      ON COALESCE(i1.start_date,p_start_date) = COALESCE(i2.start_date,p_start_date)
                      AND i1.end_date = i2.end_date
                  WHERE i1.argument_formula_id = 19
                  AND i2.argument_formula_id = 20
                ) temp_consumption;
        ELSIF v_formula_id = 80
        	THEN
              SELECT sum(consumption)/(EXTRACT (DAY FROM MAX(end_date) - MIN(start_date)) + 1),
              MIN(start_date),
              MAX(end_date)
              INTO v_result, v_start_date, v_end_date
              FROM indication;
        END IF;
        IF v_formula_id IS NULL
        THEN
        	RAISE EXCEPTION '%', get_message('ERR_AVERAGE_INDICATION');
        END IF;
        IF v_result IS NULL
        THEN
        	RAISE EXCEPTION '%', get_message('ERR_AVERAGE_INDICATION');
        END IF;
        IF v_err = 1 OR v_err IS NULL
        THEN
            RAISE EXCEPTION '%', get_message('ERR_AVERAGE_INDICATION');
        END IF;
        IF p_formula_id != 176 THEN
        RETURN QUERY
          SELECT 'indication_start_date'::varchar,to_char(v_start_date,'DD.MM.YYYY')::varchar
          UNION ALL
          SELECT 'indication_end_date'::varchar,to_char(v_end_date,'DD.MM.YYYY')::varchar
          UNION ALL
          SELECT (SELECT argument_formula_id FROM rul_argument_formula WHERE formula_id = p_formula_id and argument_formula_code like '%ср.сут%')::varchar,v_result::varchar
          ;
        ELSE
          -- Рассчитать температуру и выдать ошибку
          select count(*),obs.locality_id, sum(obs.temperature)/count(*)
          into v_count, v_locality_id, v_avg
          from rul_observation obs
          join rul_object obj on obs.locality_id = obj.locality_id
          join rul_node n on n.object_id = obj.object_id
          join rul_node_calculate_parameter ncp on ncp.node_id = n.node_id
          where observation_type_id = 1 --Воздух
          and observation_period_id = 1 --Среднесуточная
          and node_calculate_parameter_id = p_node_calculate_parameter_id
          and observation_date >= v_start_date
          and observation_date <= v_end_date
          group by obs.locality_id;
          IF COALESCE(v_count,0) != EXTRACT('day' FROM v_end_date - v_start_date) + 1
          THEN
            SELECT rl.locality_name
            INTO v_err_name
            FROM rul_locality rl
            JOIN rul_object obj ON rl.locality_id = obj.locality_id
            JOIN rul_node n ON n.object_id = obj.object_id
            JOIN rul_node_calculate_parameter ncp ON ncp.node_id = n.node_id
            WHERE p_node_calculate_parameter_id = ncp.node_calculate_parameter_id;
            RAISE EXCEPTION '%', get_message('ERR_TEMPERATURE',v_err_name);
          END IF;
          RETURN QUERY
            SELECT 'indication_start_date'::varchar,to_char(v_start_date,'DD.MM.YYYY')::varchar
            UNION ALL
            SELECT 'indication_end_date'::varchar,to_char(v_end_date,'DD.MM.YYYY')::varchar
            UNION ALL
            SELECT (SELECT argument_formula_id FROM rul_argument_formula WHERE formula_id = p_formula_id and argument_formula_code like '%ср.сут%')::varchar,v_result::varchar
            UNION ALL
            SELECT (SELECT argument_formula_id FROM rul_argument_formula WHERE formula_id = p_formula_id and argument_formula_code like '%оп%')::varchar,1::varchar
            ;
        END IF;
    END IF;
    DROP TABLE indication;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_average_value_argument(p_average_value_id bigint, p_argument_formula_id bigint)
CREATE OR REPLACE FUNCTION public.get_average_value_argument(p_average_value_id bigint, p_argument_formula_id bigint)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_value numeric;
BEGIN
    SELECT value
    INTO v_value
    FROM rul_average_value_argument
    WHERE average_value_id = p_average_value_id
      AND argument_formula_id = p_argument_formula_id;
    RETURN v_value;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_average_value_old(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_formula_id bigint)
CREATE OR REPLACE FUNCTION public.get_average_value_old(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_formula_id bigint)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
koef numeric;
avg_value numeric;
-- v.0.1
begin
    -- Предполагается, что в данной функции будет происходить вычисление коэффициента,
    -- который позволит рассчитать расход (не уверен) путем умножения расхода за выбранный промежуток
    -- на этот коэффициент. Сохраняться будет интерфейсом, эта функция будет только отдавать значения
    -- upd. Помимо кэфа будет возможность расчитывать среднесуточный расход
    -- В первом этапе выбирается приборный расход по выбранной дате.
    -- Т.е. по этому же подключению должен быть сформирован приборный расход за переданный период.
    /*IF p_average_type_id = 1::BIGINT THEN
    select
        (
        	(select sum(VALUE)
                from (
                  select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
                  from public.rul_consumption rcons
                  --left join public.rul_connection rconn
                    --on rcons.connection_id = rconn.connection_id
                  left join public.rul_accounting_type_node ratn
                    on rcons.accounting_type_node_id = ratn.accounting_type_node_id
                  where ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
                  and rcons.start_date < p_end_date
                  and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') > p_start_date) py
            )
        	/*(select coalesce(sum(VALUE),0) from public.rul_consumption where connection_id in (select connection_id from rul_connection where  node_calculate_parameter_id = p_node_calculate_parameter_id)
            	AND start_date >= p_start_date
        		AND end_date <= p_end_date
            ) */
            - coalesce(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),0)
            - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END),0)
            - (
            	(case when
                    /*(select coalesce(sum(VALUE),0) from public.rul_consumption where connection_id in
                            (select connection_id from rul_connection where  node_calculate_parameter_id = p_node_calculate_parameter_id)
                        AND start_date >= start_date AND end_date <= end_date) */
                    (select sum(VALUE)
                        from (
                          select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
                          from public.rul_consumption rcons
                          --left join public.rul_connection rconn
                            --on rcons.connection_id = rconn.connection_id
                          left join public.rul_accounting_type_node ratn
                            on rcons.accounting_type_node_id = ratn.accounting_type_node_id
                          where ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
                          and rcons.start_date < p_end_date
                          and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') > p_start_date) py
                    )
                    - 0
                    - coalesce(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN coalesce(val,0) END),0)
                    > 0 then 1 else 0 end)
           	  *
            	coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0)
              )
        )
        /
        case when
        (
        coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN val END),0)
        + (
        	(1 - case when
            		/*(select coalesce(sum(VALUE),0) from public.rul_consumption where connection_id in
                            (select connection_id from rul_connection where  node_calculate_parameter_id = p_node_calculate_parameter_id)
                        AND start_date >= start_date AND end_date <= end_date) */
                    (select sum(VALUE)
                        from (
                          select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
                          from public.rul_consumption rcons
                          --left join public.rul_connection rconn
                            --on rcons.connection_id = rconn.connection_id
                          left join public.rul_accounting_type_node ratn
                            on rcons.accounting_type_node_id = ratn.accounting_type_node_id
                          where ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
                          and rcons.start_date < p_end_date
                          and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') > p_start_date) py
                    )
                    - 0
                    - coalesce(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN coalesce(val,0) END),0)
                    > 0 then 1 else 0 end)
            *
            coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0))
        + 0
        ) = 0 then 1 else
        (
        coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN val END),0)
        + (
        	(1 - case when
                    /*(select coalesce(sum(VALUE),0) from public.rul_consumption where connection_id in
                            (select connection_id from rul_connection where  node_calculate_parameter_id = p_node_calculate_parameter_id)
                        AND start_date >= start_date AND end_date <= end_date) */
                    (select sum(VALUE)
                        from (
                          select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
                          from public.rul_consumption rcons
                          --left join public.rul_connection rconn
                            --on rcons.connection_id = rconn.connection_id
                          left join public.rul_accounting_type_node ratn
                            on rcons.accounting_type_node_id = ratn.accounting_type_node_id
                          where ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
                          and rcons.start_date < p_end_date
                          and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') > p_start_date) py
                    )
                    - 0
                    - coalesce(SUM(CASE WHEN (accounting_type_id != 17 and level!=0) THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 1 THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0)
                    - coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 3 THEN coalesce(val,0) END),0)
                    > 0 then 1 else 0 end)
            *
            coalesce(SUM(CASE WHEN (accounting_type_id = 17 or level = 0) and group_recalculation_attitude_id = 2 THEN coalesce(val,0) END),0))
        + 0
        )
        end
        as K
        into koef
from (
WITH RECURSIVE tree_cte AS (
            -- Базовый случай: выбираем корневые элементы
            SELECT
                zero_level.name as line_name,
                null::bigint as line_id,
                zero_level.node_id AS node_id,
                zero_level.node_id AS child_id,
                0 AS level,
                ARRAY[zero_level.node_id] AS path,
                zero_level.node_id::TEXT AS path_str,
                -- Добавляем даты для корневого элемента
                GREATEST(conn3.start_date, p_start_date) AS start_date,
                LEAST(conn3.end_date, p_end_date) AS end_date,
                conn3.accounting_type_id as accounting_type_id,
        		conn3.accounting_type_node_id as accounting_type_node_id,
                null::bigint as formula_id
    		FROM (select 'zero_level' as name, p_node_calculate_parameter_id::BIGINT as node_id) zero_level
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, p_start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), p_end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                    	atn.start_date < p_end_date::timestamp without time zone
        				AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') > p_start_date::timestamp without time zone
            ) conn3 ON conn3.node_calculate_parameter_id = zero_level.node_id
            UNION ALL
            -- Рекурсивный случай: присоединяем детей с учетом дат родителя
            SELECT
                rl.line_name,
                rl.line_id,
                rlp.node_calculate_parameter_id AS node_id,
                rlpc.node_calculate_parameter_id AS child_id,
                t.level + 1,
                t.path || rlpc.node_calculate_parameter_id,
                t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                -- Вычисляем даты дочернего элемента на основе родительского
                GREATEST(conn3.start_date, t.start_date) AS start_date,
                LEAST(conn3.end_date, t.end_date) AS end_date,
                conn3.accounting_type_id,
                conn3.accounting_type_node_id as accounting_type_node_id,
                rlp.formula_id
            FROM tree_cte t
            JOIN public.rul_line_parameter rlp
                ON t.child_id = rlp.node_calculate_parameter_id
            JOIN public.rul_line_parameter_child rlpc
                ON rlpc.line_parameter_id = rlp.line_parameter_id
            JOIN public.rul_line rl
                ON rl.line_id = rlp.line_id
            JOIN (
                    SELECT
                        atn.accounting_type_node_id,
                        GREATEST(atn.start_date, p_start_date) AS start_date,
                        LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'), p_end_date) AS end_date,
                        atn.node_calculate_parameter_id,
                        atn.accounting_type_id
                    FROM rul_accounting_type_node atn
                    WHERE
                        atn.start_date < p_end_date
                        AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03') > p_start_date
            ) conn3 ON conn3.node_calculate_parameter_id = rlpc.node_calculate_parameter_id
            WHERE rl.client_id IS NOT NULL
              -- Ограничиваем даты дочерних элементов датами родителя
              AND conn3.start_date <= t.end_date
              AND conn3.end_date >= t.start_date
              AND (t.accounting_type_id = 17 OR t.child_id = p_node_calculate_parameter_id) -- Дерево строиться только при безучетном расходе
        )
        SELECT
        	tree_cte.line_id,
            tree_cte.line_name,
            tree_cte.node_id,
            tree_cte.child_id,
            tree_cte.level,
            tree_cte.path,
            tree_cte.path_str,
            --GREATEST(cons.start_date, tree_cte.start_date,conn.start_date) as start_date,
            --LEAST(cons.end_date,tree_cte.end_date,conn.end_date) as end_date,
            case when tree_cte.accounting_type_id = 2 then  GREATEST(tree_cte.start_date,conn.start_date) else GREATEST(cons.start_date, tree_cte.start_date,conn.start_date) end as start_date,
            case when tree_cte.accounting_type_id = 2 then LEAST(tree_cte.end_date,conn.end_date) else LEAST(cons.end_date,tree_cte.end_date,conn.end_date) end as end_date,
            tree_cte.accounting_type_id,
            tree_cte.accounting_type_node_id,
            conn.connection_id,
            -- Добавление подтвеждений влияет на то, что подключение должно счиаться непересчитываемым по ГПУ и не подлежащим балансировке
            -- Также не должно сформироваться в новые начисления и расходы.
            -- если подтверждено подключение, мы его считает не перерасчитываемым по ГПУ
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM rul_charge rc
                    WHERE rc.connection_id = conn.connection_id
                      AND rc.invoice_id IS NOT NULL
                      AND rc.billing_start_date <= p_end_date
                      AND rc.billing_end_date >= p_start_date
                ) THEN 1
                ELSE conn.group_recalculation_attitude_id
            END AS group_recalculation_attitude_id,
            conn.allocation_source_consumption_id,
            cons.value,
            cons.connection_name,
            (extract (day from (date_trunc('day',LEAST(cons.end_date,tree_cte.end_date,conn.end_date)+ interval '1 second')
            - GREATEST(cons.start_date, tree_cte.start_date,conn.start_date))))  *
            cons.value /
            case when (extract (day from (date_trunc('day',cons.end_date + interval '1 second') - cons.start_date))) = 0
            	then 1
                else (extract (day from (date_trunc('day',cons.end_date + interval '1 second') - cons.start_date)))
                end
            / case when tree_cte.accounting_type_id <> 17 and tree_cte.level != 0
    		then count(*) over (partition by tree_cte.child_id,GREATEST(cons.start_date, tree_cte.start_date,conn.start_date)
            ,LEAST(cons.end_date,tree_cte.end_date,conn.end_date))
    		else 1 end
                as val,
            cons.source_consumption_id,
            tree_cte.formula_id
        FROM tree_cte
        left join  (
            SELECT
                c.connection_id,
                c.connection_name,
                GREATEST(c.start_date, p_start_date) AS start_date,
                LEAST(COALESCE(c.end_date, '2100-04-30 23:59:59+03'), p_end_date) AS end_date,
                c.node_calculate_parameter_id,
                c.unaccounted_source_consumption_id,
                c.allocation_source_consumption_id,
                c.group_recalculation_attitude_id
            FROM rul_connection c
            WHERE
                c.start_date <= p_end_date
                AND COALESCE(c.end_date, '2100-04-30 23:59:59+03') >= p_start_date
        ) conn ON tree_cte.child_id = conn.node_calculate_parameter_id
            AND tree_cte.start_date <= conn.end_date
            AND tree_cte.end_date >= conn.start_date
        LEFT JOIN
        (
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id
              , 1 as source_consumption_id
            FROM
              public.rul_consumption_load
            WHERE 1=1
                AND start_date <= p_end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= p_start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id
              , 2 as source_consumption_id
            FROM
              public.rul_consumption_standard
            WHERE 1=1
                AND start_date <= p_end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= p_start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  connection_name,  start_date,  end_date,  value, accounting_type_node_id, 17 as accounting_type_id
              , 4 as source_consumption_id
            FROM
              public.rul_consumption_source_connection
            WHERE 1=1
                AND start_date <= p_end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= p_start_date
                AND theoretical_calculation = true
            UNION ALL
            SELECT
              connection_id,  '-----', start_date,  end_date,  value, accounting_type_node_id, 2 as accounting_type_id, null
            FROM
              public.rul_consumption
            WHERE 1=1
                AND start_date <= p_end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= p_start_date
                AND value != 0
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 19 as accounting_type_id, null
            FROM
              public.rul_consumption_pipe
            WHERE 1=1
                AND start_date <= p_end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= p_start_date
            UNION ALL
            SELECT
              connection_id,  connection_name, start_date,  end_date,  value, accounting_type_node_id, 5 as accounting_type_id, null
            FROM
              public.rul_consumption_average
            WHERE 1=1
                AND start_date <= p_end_date
                AND COALESCE(end_date, '2100-04-30 23:59:59+03') >= p_start_date
        ) cons ON (cons.connection_id = conn.connection_id or cons.accounting_type_id = 2)
                AND cons.accounting_type_node_id = tree_cte.accounting_type_node_id
                AND (
                    (conn.allocation_source_consumption_id = cons.source_consumption_id
                    and (tree_cte.accounting_type_id = 17 or tree_cte.child_id = p_node_calculate_parameter_id))
                    or (tree_cte.accounting_type_id = 2 and cons.accounting_type_id = 2 and tree_cte.child_id != p_node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 5 and cons.accounting_type_id = 5 and tree_cte.child_id != p_node_calculate_parameter_id)
                    or (tree_cte.accounting_type_id = 19 and cons.accounting_type_id = 19 and tree_cte.child_id != p_node_calculate_parameter_id)
                    )
                AND GREATEST(tree_cte.start_date,conn.start_date) <= cons.end_date
                AND LEAST(tree_cte.end_date,conn.end_date) >= cons.start_date
        ORDER BY path, node_id, child_id, tree_cte.start_date
              )
              a;
RETURN coalesce(koef,0);
ELSIF p_average_type_id = 2::BIGINT THEN
	select sum(VALUE)/ (extract (day from p_end_date - p_start_date) + 1) into avg_value
    from (
      select distinct ratn.node_calculate_parameter_id,rcons.start_date,rcons.end_date,rcons.value
      from public.rul_consumption rcons
      left join public.rul_accounting_type_node ratn
        on rcons.accounting_type_node_id = ratn.accounting_type_node_id
      where ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
      and rcons.start_date < p_end_date
      and COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') > p_start_date) py;
    RETURN coalesce(avg_value,0);
END IF;*/
end;
$function$

-- ======================================================================

-- FUNCTION: public.get_connection(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_connection_ids bigint[])
CREATE OR REPLACE FUNCTION public.get_connection(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_connection_ids bigint[] DEFAULT NULL::bigint[])
 RETURNS TABLE(connection_id bigint, connection_name character varying, start_date timestamp without time zone, end_date timestamp without time zone, accounting_type_node_id bigint, node_calculate_parameter_id bigint)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        conn.connection_id,
        conn.connection_name,
        GREATEST(conn.start_date, acc.start_date) AS start_date,
        LEAST(conn.end_date, acc.end_date) AS end_date,
        acc.accounting_type_node_id,
        acc.node_calculate_parameter_id
    FROM (
        SELECT
            c.connection_id,
            c.connection_name,
            GREATEST(c.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(c.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone),p_end_date) AS end_date,
            c.node_calculate_parameter_id
        FROM rul_connection c
        WHERE
        	c.start_date <= p_end_date
            AND COALESCE(c.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
            --AND c.connection_id in (select connection_id from rul_connection where agreement_id = p_agreement_id)
            AND (c.connection_id = ANY (p_connection_ids) OR p_connection_ids IS NULL)
    ) conn
    JOIN (
        SELECT
            atn.accounting_type_node_id,
            GREATEST(atn.start_date, p_start_date) AS start_date,
            LEAST(COALESCE(atn.end_date, '2100-03-31 23:59:59+03'::timestamp without time zone), p_end_date) AS end_date,
            atn.node_calculate_parameter_id
        FROM rul_accounting_type_node atn
        WHERE
        	--Проверяем действует ли способ учета в переданном расчетном периоде
            atn.start_date <= p_end_date
            AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
    ) acc
    ON acc.node_calculate_parameter_id = conn.node_calculate_parameter_id
    --Получаем даты пересечения периодов действия способа учета и подключения
    and acc.start_date <= conn.end_date
    AND acc.end_date >= conn.start_date;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_data_history_agreement(p_json json)
CREATE OR REPLACE FUNCTION public.get_data_history_agreement(p_json json)
 RETURNS TABLE(log_time timestamp without time zone, log_action character varying, log_user_email character varying, log_user_ip character varying, agreement_id bigint, parent_agreement_id bigint, supplier_client_id bigint, customer_client_id bigint, billing_period_id bigint, start_date timestamp without time zone, end_date timestamp without time zone, agreement_code character varying, agreement_name character varying, description character varying, supplier_user_id bigint, customer_user_id bigint, supplier_document_name character varying, supplier_document_info character varying, customer_document_name character varying, customer_document_info character varying, supplier_description character varying, customer_description character varying, is_active numeric, op_user_id bigint, op_date timestamp without time zone, deleted smallint, owner_client_id bigint, owner_user_id bigint, owner_document_name character varying, owner_document_info character varying, pay_day_count bigint, penalty numeric, supplier_responsible_user_id bigint, customer_responsible_user_id bigint, owner_agreement_id bigint, agreement_type_id bigint, penalty_operation_template_id bigint, code_ay character varying, payment_mechanism_id bigint, second_supplier_user_id bigint, signing_date timestamp without time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
	client_id int := coalesce(p_json->>'client_id','0')::int;
	date_start timestamp := coalesce(p_json->>'need_detalization_2','0')::timestamp;
	date_end timestamp := coalesce(p_json->>'need_detalization_3','0')::timestamp;
BEGIN
    RETURN QUERY
    SELECT 'Время действия','Действие','Пользователь','Ip пользователя','Id договора','Указывает на родительский договор к которому создано доп соглашение.', 'Ссылка на поставщика по  договору (клиент)', 'Ссылка на плательщика(абонент/субабонент)', 'Периодичность расчетов', 'Начало даты действия', 'Конец даты действия', 'Номер договора', 'Название договора', 'Описание договора', 'Подписант со стороны поставщика', 'Подписант со стороны плательщика(абонент/субабонент)', 'Действует на основании(Поставщик)', 'Описание документа', 'Действует на основании(Абонент)', 'Описание документа', 'Описание для поставщика', 'Описание для плательщика(абонент/субабонент)', 'Ссылка на владельца объекта', 'Подписант со стороны владельца объекта', 'Действует на основании(Субабонент)', 'Описание документа', 'Количество дней на оплату', 'Пеня, % годовых', 'Ответственное за расчеты лицо поставщика', 'Ответственное за расчеты лицо плательщика(абонент/субабонент)', 'Ссылка на договор владельца объекта.(при трехстороннем)', 'Ссылка на тип договора', 'Ссылка на операцию по пене', 'Код АУ', 'Ссылка на механизм оплаты', 'Подписант поставщика №2', 'Дата подписания'
    UNION ALL
SELECT
    l.log_time,
    CASE max(l.log_action)
        WHEN 'ui' THEN 'Обновление'
        WHEN 'd'  THEN 'Удаление'
        WHEN 'i'  THEN 'Добавление'
        ELSE 'Неизвестное действие'
    END,
    max(u.email),
    max(l.log_user_ip),
max(l.agreement_id),
    CASE WHEN array_length(array_agg( l.parent_agreement_id ORDER BY l.parent_agreement_id),1) > 1
         AND (array_agg( l.parent_agreement_id ORDER BY l.parent_agreement_id))[2] = (array_agg( l.parent_agreement_id ORDER BY l.parent_agreement_id))[1]
         THEN NULL
         ELSE (array_agg( l.parent_agreement_id ORDER BY l.parent_agreement_id))[1]::text
              || COALESCE(' (' || (array_agg( l.parent_agreement_id ORDER BY l.parent_agreement_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.supplier_client_id ORDER BY l.supplier_client_id),1) > 1
         AND (array_agg( l.supplier_client_id ORDER BY l.supplier_client_id))[2] = (array_agg( l.supplier_client_id ORDER BY l.supplier_client_id))[1]
         THEN NULL
         ELSE (array_agg( l.supplier_client_id ORDER BY l.supplier_client_id))[1]::text
              || COALESCE(' (' || (array_agg( l.supplier_client_id ORDER BY l.supplier_client_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.customer_client_id ORDER BY l.customer_client_id),1) > 1
         AND (array_agg( l.customer_client_id ORDER BY l.customer_client_id))[2] = (array_agg( l.customer_client_id ORDER BY l.customer_client_id))[1]
         THEN NULL
         ELSE (array_agg( l.customer_client_id ORDER BY l.customer_client_id))[1]::text
              || COALESCE(' (' || (array_agg( l.customer_client_id ORDER BY l.customer_client_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.billing_period_id ORDER BY l.billing_period_id),1) > 1
         AND (array_agg( l.billing_period_id ORDER BY l.billing_period_id))[2] = (array_agg( l.billing_period_id ORDER BY l.billing_period_id))[1]
         THEN NULL
         ELSE (array_agg( l.billing_period_id ORDER BY l.billing_period_id))[1]::text
              || COALESCE(' (' || (array_agg( l.billing_period_id ORDER BY l.billing_period_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.start_date ORDER BY l.start_date),1) > 1
         AND (array_agg( l.start_date ORDER BY l.start_date))[2] = (array_agg( l.start_date ORDER BY l.start_date))[1]
         THEN NULL
         ELSE (array_agg( l.start_date ORDER BY l.start_date))[1]::text
              || COALESCE(' (' || (array_agg( l.start_date ORDER BY l.start_date))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.end_date ORDER BY l.end_date),1) > 1
         AND (array_agg( l.end_date ORDER BY l.end_date))[2] = (array_agg( l.end_date ORDER BY l.end_date))[1]
         THEN NULL
         ELSE (array_agg( l.end_date ORDER BY l.end_date))[1]::text
              || COALESCE(' (' || (array_agg( l.end_date ORDER BY l.end_date))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.agreement_code ORDER BY l.agreement_code),1) > 1
         AND (array_agg( l.agreement_code ORDER BY l.agreement_code))[2] = (array_agg( l.agreement_code ORDER BY l.agreement_code))[1]
         THEN NULL
         ELSE (array_agg( l.agreement_code ORDER BY l.agreement_code))[1]
              || COALESCE(' (' || (array_agg( l.agreement_code ORDER BY l.agreement_code))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.agreement_name ORDER BY l.agreement_name),1) > 1
         AND (array_agg( l.agreement_name ORDER BY l.agreement_name))[2] = (array_agg( l.agreement_name ORDER BY l.agreement_name))[1]
         THEN NULL
         ELSE (array_agg( l.agreement_name ORDER BY l.agreement_name))[1]
              || COALESCE(' (' || (array_agg( l.agreement_name ORDER BY l.agreement_name))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.description ORDER BY l.description),1) > 1
         AND (array_agg( l.description ORDER BY l.description))[2] = (array_agg( l.description ORDER BY l.description))[1]
         THEN NULL
         ELSE (array_agg( l.description ORDER BY l.description))[1]
              || COALESCE(' (' || (array_agg( l.description ORDER BY l.description))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.supplier_user_id ORDER BY l.supplier_user_id),1) > 1
         AND (array_agg( l.supplier_user_id ORDER BY l.supplier_user_id))[2] = (array_agg( l.supplier_user_id ORDER BY l.supplier_user_id))[1]
         THEN NULL
         ELSE (array_agg( l.supplier_user_id ORDER BY l.supplier_user_id))[1]::text
              || COALESCE(' (' || (array_agg( l.supplier_user_id ORDER BY l.supplier_user_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.customer_user_id ORDER BY l.customer_user_id),1) > 1
         AND (array_agg( l.customer_user_id ORDER BY l.customer_user_id))[2] = (array_agg( l.customer_user_id ORDER BY l.customer_user_id))[1]
         THEN NULL
         ELSE (array_agg( l.customer_user_id ORDER BY l.customer_user_id))[1]::text
              || COALESCE(' (' || (array_agg( l.customer_user_id ORDER BY l.customer_user_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.supplier_document_name ORDER BY l.supplier_document_name),1) > 1
         AND (array_agg( l.supplier_document_name ORDER BY l.supplier_document_name))[2] = (array_agg( l.supplier_document_name ORDER BY l.supplier_document_name))[1]
         THEN NULL
         ELSE (array_agg( l.supplier_document_name ORDER BY l.supplier_document_name))[1]
              || COALESCE(' (' || (array_agg( l.supplier_document_name ORDER BY l.supplier_document_name))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.supplier_document_info ORDER BY l.supplier_document_info),1) > 1
         AND (array_agg( l.supplier_document_info ORDER BY l.supplier_document_info))[2] = (array_agg( l.supplier_document_info ORDER BY l.supplier_document_info))[1]
         THEN NULL
         ELSE (array_agg( l.supplier_document_info ORDER BY l.supplier_document_info))[1]
              || COALESCE(' (' || (array_agg( l.supplier_document_info ORDER BY l.supplier_document_info))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.customer_document_name ORDER BY l.customer_document_name),1) > 1
         AND (array_agg( l.customer_document_name ORDER BY l.customer_document_name))[2] = (array_agg( l.customer_document_name ORDER BY l.customer_document_name))[1]
         THEN NULL
         ELSE (array_agg( l.customer_document_name ORDER BY l.customer_document_name))[1]
              || COALESCE(' (' || (array_agg( l.customer_document_name ORDER BY l.customer_document_name))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.customer_document_info ORDER BY l.customer_document_info),1) > 1
         AND (array_agg( l.customer_document_info ORDER BY l.customer_document_info))[2] = (array_agg( l.customer_document_info ORDER BY l.customer_document_info))[1]
         THEN NULL
         ELSE (array_agg( l.customer_document_info ORDER BY l.customer_document_info))[1]
              || COALESCE(' (' || (array_agg( l.customer_document_info ORDER BY l.customer_document_info))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.supplier_description ORDER BY l.supplier_description),1) > 1
         AND (array_agg( l.supplier_description ORDER BY l.supplier_description))[2] = (array_agg( l.supplier_description ORDER BY l.supplier_description))[1]
         THEN NULL
         ELSE (array_agg( l.supplier_description ORDER BY l.supplier_description))[1]
              || COALESCE(' (' || (array_agg( l.supplier_description ORDER BY l.supplier_description))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.customer_description ORDER BY l.customer_description),1) > 1
         AND (array_agg( l.customer_description ORDER BY l.customer_description))[2] = (array_agg( l.customer_description ORDER BY l.customer_description))[1]
         THEN NULL
         ELSE (array_agg( l.customer_description ORDER BY l.customer_description))[1]
              || COALESCE(' (' || (array_agg( l.customer_description ORDER BY l.customer_description))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.owner_client_id ORDER BY l.owner_client_id),1) > 1
         AND (array_agg( l.owner_client_id ORDER BY l.owner_client_id))[2] = (array_agg( l.owner_client_id ORDER BY l.owner_client_id))[1]
         THEN NULL
         ELSE (array_agg( l.owner_client_id ORDER BY l.owner_client_id))[1]::text
              || COALESCE(' (' || (array_agg( l.owner_client_id ORDER BY l.owner_client_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.owner_user_id ORDER BY l.owner_user_id),1) > 1
         AND (array_agg( l.owner_user_id ORDER BY l.owner_user_id))[2] = (array_agg( l.owner_user_id ORDER BY l.owner_user_id))[1]
         THEN NULL
         ELSE (array_agg( l.owner_user_id ORDER BY l.owner_user_id))[1]::text
              || COALESCE(' (' || (array_agg( l.owner_user_id ORDER BY l.owner_user_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.owner_document_name ORDER BY l.owner_document_name),1) > 1
         AND (array_agg( l.owner_document_name ORDER BY l.owner_document_name))[2] = (array_agg( l.owner_document_name ORDER BY l.owner_document_name))[1]
         THEN NULL
         ELSE (array_agg( l.owner_document_name ORDER BY l.owner_document_name))[1]
              || COALESCE(' (' || (array_agg( l.owner_document_name ORDER BY l.owner_document_name))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.owner_document_info ORDER BY l.owner_document_info),1) > 1
         AND (array_agg( l.owner_document_info ORDER BY l.owner_document_info))[2] = (array_agg( l.owner_document_info ORDER BY l.owner_document_info))[1]
         THEN NULL
         ELSE (array_agg( l.owner_document_info ORDER BY l.owner_document_info))[1]
              || COALESCE(' (' || (array_agg( l.owner_document_info ORDER BY l.owner_document_info))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.pay_day_count ORDER BY l.pay_day_count),1) > 1
         AND (array_agg( l.pay_day_count ORDER BY l.pay_day_count))[2] = (array_agg( l.pay_day_count ORDER BY l.pay_day_count))[1]
         THEN NULL
         ELSE (array_agg( l.pay_day_count ORDER BY l.pay_day_count))[1]::text
              || COALESCE(' (' || (array_agg( l.pay_day_count ORDER BY l.pay_day_count))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.penalty ORDER BY l.penalty),1) > 1
         AND (array_agg( l.penalty ORDER BY l.penalty))[2] = (array_agg( l.penalty ORDER BY l.penalty))[1]
         THEN NULL
         ELSE (array_agg( l.penalty ORDER BY l.penalty))[1]::text
              || COALESCE(' (' || (array_agg( l.penalty ORDER BY l.penalty))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.supplier_responsible_user_id ORDER BY l.supplier_responsible_user_id),1) > 1
         AND (array_agg( l.supplier_responsible_user_id ORDER BY l.supplier_responsible_user_id))[2] = (array_agg( l.supplier_responsible_user_id ORDER BY l.supplier_responsible_user_id))[1]
         THEN NULL
         ELSE (array_agg( l.supplier_responsible_user_id ORDER BY l.supplier_responsible_user_id))[1]::text
              || COALESCE(' (' || (array_agg( l.supplier_responsible_user_id ORDER BY l.supplier_responsible_user_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.customer_responsible_user_id ORDER BY l.customer_responsible_user_id),1) > 1
         AND (array_agg( l.customer_responsible_user_id ORDER BY l.customer_responsible_user_id))[2] = (array_agg( l.customer_responsible_user_id ORDER BY l.customer_responsible_user_id))[1]
         THEN NULL
         ELSE (array_agg( l.customer_responsible_user_id ORDER BY l.customer_responsible_user_id))[1]::text
              || COALESCE(' (' || (array_agg( l.customer_responsible_user_id ORDER BY l.customer_responsible_user_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.owner_agreement_id ORDER BY l.owner_agreement_id),1) > 1
         AND (array_agg( l.owner_agreement_id ORDER BY l.owner_agreement_id))[2] = (array_agg( l.owner_agreement_id ORDER BY l.owner_agreement_id))[1]
         THEN NULL
         ELSE (array_agg( l.owner_agreement_id ORDER BY l.owner_agreement_id))[1]::text
              || COALESCE(' (' || (array_agg( l.owner_agreement_id ORDER BY l.owner_agreement_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.agreement_type_id ORDER BY l.agreement_type_id),1) > 1
         AND (array_agg( l.agreement_type_id ORDER BY l.agreement_type_id))[2] = (array_agg( l.agreement_type_id ORDER BY l.agreement_type_id))[1]
         THEN NULL
         ELSE (array_agg( l.agreement_type_id ORDER BY l.agreement_type_id))[1]::text
              || COALESCE(' (' || (array_agg( l.agreement_type_id ORDER BY l.agreement_type_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.penalty_operation_template_id ORDER BY l.penalty_operation_template_id),1) > 1
         AND (array_agg( l.penalty_operation_template_id ORDER BY l.penalty_operation_template_id))[2] = (array_agg( l.penalty_operation_template_id ORDER BY l.penalty_operation_template_id))[1]
         THEN NULL
         ELSE (array_agg( l.penalty_operation_template_id ORDER BY l.penalty_operation_template_id))[1]::text
              || COALESCE(' (' || (array_agg( l.penalty_operation_template_id ORDER BY l.penalty_operation_template_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.code_ay ORDER BY l.code_ay),1) > 1
         AND (array_agg( l.code_ay ORDER BY l.code_ay))[2] = (array_agg( l.code_ay ORDER BY l.code_ay))[1]
         THEN NULL
         ELSE (array_agg( l.code_ay ORDER BY l.code_ay))[1]
              || COALESCE(' (' || (array_agg( l.code_ay ORDER BY l.code_ay))[2] || ')','')
    END,
    CASE WHEN array_length(array_agg( l.payment_mechanism_id ORDER BY l.payment_mechanism_id),1) > 1
         AND (array_agg( l.payment_mechanism_id ORDER BY l.payment_mechanism_id))[2] = (array_agg( l.payment_mechanism_id ORDER BY l.payment_mechanism_id))[1]
         THEN NULL
         ELSE (array_agg( l.payment_mechanism_id ORDER BY l.payment_mechanism_id))[1]::text
              || COALESCE(' (' || (array_agg( l.payment_mechanism_id ORDER BY l.payment_mechanism_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.second_supplier_user_id ORDER BY l.second_supplier_user_id),1) > 1
         AND (array_agg( l.second_supplier_user_id ORDER BY l.second_supplier_user_id))[2] = (array_agg( l.second_supplier_user_id ORDER BY l.second_supplier_user_id))[1]
         THEN NULL
         ELSE (array_agg( l.second_supplier_user_id ORDER BY l.second_supplier_user_id))[1]::text
              || COALESCE(' (' || (array_agg( l.second_supplier_user_id ORDER BY l.second_supplier_user_id))[2]::text || ')','')
    END,
    CASE WHEN array_length(array_agg( l.signing_date ORDER BY l.signing_date),1) > 1
         AND (array_agg( l.signing_date ORDER BY l.signing_date))[2] = (array_agg( l.signing_date ORDER BY l.signing_date))[1]
         THEN NULL
         ELSE (array_agg( l.signing_date ORDER BY l.signing_date))[1]::text
              || COALESCE(' (' || (array_agg( l.signing_date ORDER BY l.signing_date))[2]::text || ')','')
    END
FROM log.log_rul_agreement l
LEFT JOIN public.rul_user u
       ON u.user_id = l.log_user_id
WHERE l.log_user_id IS NOT NULL and l.supplier_client_id=client_id and l.log_time BETWEEN date_start and date_end
GROUP BY l.log_time
ORDER BY l.log_time DESC;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_formula_details(p_formula_id bigint, p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_method_id bigint)
CREATE OR REPLACE FUNCTION public.get_formula_details(p_formula_id bigint, p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_method_id bigint DEFAULT NULL::bigint)
 RETURNS TABLE(formula_connection_id bigint, version_load_standard_id bigint, connection_id bigint, details jsonb, start_date timestamp without time zone, end_date timestamp without time zone, formula_id bigint)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        fc.formula_connection_id,
        vls.version_load_standard_id,
        fc.connection_id,
        CASE WHEN p_method_id IS NOT NULL
        	THEN jsonb_object_agg(raf.argument_formula_code::TEXT,
                  jsonb_build_object('value', lsv.value,'code', raf.argument_formula_code,'unit', ru.unit_name)
        					)
            ELSE jsonb_object_agg(raf.argument_formula_id::TEXT,
                  jsonb_build_object('value', lsv.value,'code', raf.argument_formula_code,'unit', ru.unit_name)
        					)
        END
         AS details,
        GREATEST(vls.start_date, p_start_date) AS start_date,
        LEAST(COALESCE(vls.end_date, '2100-03-31 23:59:59'::TIMESTAMP), p_end_date) AS end_date,
        rf.formula_id
    FROM rul_formula rf
    JOIN rul_formula_connection fc ON rf.formula_id = fc.formula_id
    JOIN rul_argument_formula raf ON raf.formula_id = rf.formula_id
    JOIN rul_version_load_standard vls ON fc.formula_connection_id = vls.formula_connection_id
    LEFT JOIN rul_load_standard_value lsv
        ON lsv.version_load_standard_id = vls.version_load_standard_id
        AND lsv.argument_formula_id = raf.argument_formula_id
    JOIN rul_unit ru ON ru.unit_id = raf.unit_id
    WHERE
    	CASE WHEN p_method_id IS NOT NULL
        	THEN rf.method_id = p_method_id
            ELSE rf.formula_id = p_formula_id
        END
        AND vls.start_date <= p_end_date
        AND COALESCE(vls.end_date, '2100-03-31 23:59:59'::TIMESTAMP) >= p_start_date
    GROUP BY
        fc.formula_connection_id,
        vls.version_load_standard_id,
        fc.connection_id,
        vls.start_date,
        vls.end_date,
        rf.formula_id;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_indication_consumption(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_formula_id bigint, p_argument_formula_code character varying)
CREATE OR REPLACE FUNCTION public.get_indication_consumption(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_formula_id bigint, p_argument_formula_code character varying)
 RETURNS TABLE(start_date timestamp without time zone, end_date timestamp without time zone, value numeric, accounting_type_node_id bigint, node_panel_argument_id bigint, node_panel_id bigint, value_number numeric, argument_formula_code character varying, unit_name character varying, consumption numeric)
 LANGUAGE plpgsql
AS $function$
BEGIN
	-- Получаем показания всех расходиков за период по нужной формуле и по нужному аргументу
    RETURN QUERY
    SELECT
            rc1.start_date,
            rc1.end_date,
            rc1.consumption * rnpa1.conversion_factor AS value,
            rc1.accounting_type_node_id,
            rnpa1.node_panel_argument_id,
            rnpa1.node_panel_id,
            rc1.value_number,
            pa1.panel_name,
            u1.unit_name,
            rc1.consumption
        FROM rul_node_panel_argument rnpa1
        JOIN rul_argument_formula raf1
            ON rnpa1.argument_formula_id = raf1.argument_formula_id
        JOIN rul_node_panel np1
            ON rnpa1.node_panel_id = np1.node_panel_id
        JOIN rul_panel pa1
            ON np1.panel_id = pa1.panel_id
        JOIN rul_parameter p1
            ON np1.parameter_id = p1.parameter_id
        JOIN rul_unit u1
            on p1.unit_id = u1.unit_id
        JOIN rul_formula rf1
            ON rf1.formula_id = raf1.formula_id
        JOIN rul_preconsumption rc1
            ON rc1.node_panel_argument_id = rnpa1.node_panel_argument_id
        JOIN (
                  SELECT
                      atn.accounting_type_node_id,
                      GREATEST(atn.start_date, p_start_date) AS start_date,
                      LEAST(COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone),
                            p_end_date) AS end_date,
                      atn.node_calculate_parameter_id
                  FROM rul_accounting_type_node atn
                  WHERE
                      atn.start_date < p_end_date
                      AND COALESCE(atn.end_date, '2100-04-30 23:59:59+03'::timestamp without time zone) >= p_start_date
              ) conn
        --JOIN get_connection(p_start_date,p_end_date) conn
              ON conn.accounting_type_node_id = rc1.accounting_type_node_id
        WHERE rf1.formula_id = p_formula_id
            AND raf1.argument_formula_code = p_argument_formula_code
            AND rc1.start_date >= p_start_date
            AND rc1.end_date <= p_end_date
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_json_from_url(url text)
CREATE OR REPLACE FUNCTION public.get_json_from_url(url text)
 RETURNS json
 LANGUAGE plpython3u
AS $function$
from urllib.request import urlopen
with urlopen(url) as response:
	json = response.read().decode("utf-8")
return json
$function$

-- ======================================================================

-- FUNCTION: public.get_math(primer text)
CREATE OR REPLACE FUNCTION public.get_math(primer text)
 RETURNS TABLE(value double precision)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY EXECUTE FORMAT('SELECT '||primer);
END
$function$

-- ======================================================================

-- FUNCTION: public.get_message(p_message_code character varying, p_message character varying, p_lang character varying)
CREATE OR REPLACE FUNCTION public.get_message(p_message_code character varying, p_message character varying DEFAULT ''::character varying, p_lang character varying DEFAULT 'RUS'::character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
begin
    return left(
        '[[' ||
            coalesce(
                (
                    select system_message_text
                    from rul_system_message
                    where system_message_lang = p_lang
                      and system_message_code = p_message_code
                ),
                ''
            ) || ' ' || coalesce(p_message, ''),
        512
    )
        || ']]';
end;
$function$

-- ======================================================================

-- FUNCTION: public.get_notes(p_accounting_type_id bigint, p_val numeric, p_coef numeric, p_node_calculate_parameter_id bigint, p_start_date timestamp without time zone, p_end_date timestamp without time zone, start_date timestamp without time zone, end_date timestamp without time zone, note character varying, p_accounting_type_node_id bigint)
CREATE OR REPLACE FUNCTION public.get_notes(p_accounting_type_id bigint, p_val numeric, p_coef numeric, p_node_calculate_parameter_id bigint, p_start_date timestamp without time zone, p_end_date timestamp without time zone, start_date timestamp without time zone, end_date timestamp without time zone, note character varying, p_accounting_type_node_id bigint)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN CASE WHEN p_accounting_type_id = 17 THEN  to_char(start_date,'DD')||'-'||to_char(end_date,'DD')||' (БУ): '||note
        	      WHEN p_accounting_type_id = 2 THEN
                   COALESCE((SELECT to_char(consumption.start_date, 'DD') || '-' || to_char(consumption.end_date, 'DD') ||' (ПУ): ' || consumption.note ||' R:' ||
                   case when  COALESCE(consumption.value,0) = 0 then 0 else  round((COALESCE(p_val,0)*p_coef/consumption.value)*100::numeric,2) end || ' %'
                   from	(SELECT min(rcons.start_date) as start_date, max(rcons.end_date) as end_date, sum(rcons.value) as value,
                              string_agg(rcons.note, ' ') as note
                              FROM public.rul_consumption rcons
                              LEFT JOIN public.rul_accounting_type_node ratn
                                  ON rcons.accounting_type_node_id = ratn.accounting_type_node_id
                              WHERE ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
                                AND rcons.start_date < p_end_date
                                AND COALESCE(rcons.end_date, '2100-04-30 23:59:59+03') >= p_start_date
                                AND rcons.accounting_type_node_id = p_accounting_type_node_id
                          ) consumption
                   ),'Не заведены показания для приборного учета')
                  WHEN p_accounting_type_id = 5 THEN
                  COALESCE((SELECT to_char(consumption.start_date, 'DD') || '-' || to_char(consumption.end_date, 'DD') ||' (СУ): ' || consumption.note
                   from	(SELECT min(rcona.start_date) as start_date, max(rcona.end_date) as end_date, sum(rcona.value) as value,
                              round(sum(rcona.value),3)::varchar ||' ' ||(SELECT unit_name FROM rul_unit WHERE unit_id =
                                (SELECT unit_id FROM rul_parameter WHERE parameter_id =
                                    (SELECT parameter_id FROM rul_node_calculate_parameter WHERE node_calculate_parameter_id = p_node_calculate_parameter_id)
                                )
                              ) as note
                              FROM public.rul_consumption_average rcona
                              LEFT JOIN public.rul_accounting_type_node ratn
                                  ON rcona.accounting_type_node_id = ratn.accounting_type_node_id
                              WHERE ratn.node_calculate_parameter_id = p_node_calculate_parameter_id
                                AND rcona.start_date < p_end_date
                                AND COALESCE(rcona.end_date, '2100-04-30 23:59:59+03') >= p_start_date
                                AND rcona.accounting_type_node_id = p_accounting_type_node_id
                          ) consumption
                   ),'Не заведены показания для учета по среднему')
                  END;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_precipitation(p_start_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_precipitation_type_id bigint, p_precipitation_period_id bigint)
CREATE OR REPLACE FUNCTION public.get_precipitation(p_start_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_precipitation_type_id bigint, p_precipitation_period_id bigint)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
	v_precipitation numeric;
BEGIN
    select rp.level_precipitation into v_precipitation
    from rul_precipitation rp
      join rul_object obj on rp.locality_id = obj.locality_id
      join rul_node n on n.object_id = obj.object_id
      join rul_node_calculate_parameter ncp on ncp.node_id = n.node_id
    where precipitation_type_id = p_precipitation_type_id
      and precipitation_period_id = p_precipitation_period_id
      and node_calculate_parameter_id = p_node_calculate_parameter_id
      and precipitation_date = p_start_date;
    RETURN v_precipitation;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_final_registry(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_final_registry(p_json json)
 RETURNS TABLE(code_ay character varying, purpose_consumption character varying, param_water character varying, param_water_rub character varying, param_canalization character varying, param_canalization_rub character varying, sum_consumption character varying, sum_rub character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::int;
	l_need_detalization_2 int := coalesce(p_json->>'need_detalization_2','0')::int;
	l_need_detalization_3 int := coalesce(p_json->>'need_detalization_3','0')::int;
    l_group_1 BIGINT := coalesce(p_json->>'group_1','0')::BIGINT;
    l_group_2 BIGINT := coalesce(p_json->>'group_2','0')::BIGINT;
    l_group_3 BIGINT := coalesce(p_json->>'group_3','0')::BIGINT;
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
    l_client_group BIGINT := p_json->>'client_group';
BEGIN
    RETURN QUERY
    SELECT 'Код АУ','Назначение','Вода, м3','Вода, руб','Стоки, м3','Стоки, руб','Всего, м3','Всего, руб'
    UNION ALL
    SELECT
    	 filter.code_ay, filter.purpose_consumption_name, filter.consumption_param_1, filter.rub_param_1,
         filter.consumption_param_6,filter.rub_param_6,filter.sum_consumption,filter.sum_rub
    FROM (
    SELECT rgc.group_consumption_name,
    CASE
        WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
            THEN ra.code_ay
        ELSE null
    END AS code_ay,
    CASE
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
        	 AND GROUPING(ra.code_ay) = 1 AND GROUPING(rjc.shortname) = 1
            THEN 'Итого'
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
            THEN coalesce(rcl.filial,' ') ||' '|| coalesce(rjc.shortname,' ')
        WHEN GROUPING(rpc.purpose_consumption_name) = 1
            THEN ('    в т.ч. ' ||rgc.group_consumption_name)::varchar
        ELSE ('        в т.ч. ' || rpc.purpose_consumption_name)::varchar
    END AS purpose_consumption_name,
    round(SUM(CASE WHEN rncp.parameter_id = 1 THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 1 THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 6 THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_6,
    round(SUM(CASE WHEN rncp.parameter_id = 6 THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_6,
    round(SUM(CASE WHEN rncp.parameter_id in (1,6) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS sum_consumption,
    round(SUM(CASE WHEN rncp.parameter_id in (1,6) THEN round(rch.amount,2) END),2)::varchar(256) AS sum_rub
    FROM rul_charge rch
    JOIN rul_connection rc
        ON rch.connection_id = rc.connection_id
    JOIN rul_node_calculate_parameter rncp
        ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
        AND rncp.parameter_id IN (1, 6)
    JOIN rul_purpose_consumption rpc
        ON rpc.purpose_consumption_id = rc.purpose_consumption_id
    JOIN rul_group_purpose_consumption rgpc
        ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
    JOIN rul_group_consumption rgc
        ON rgc.group_consumption_id = rgpc.group_consumption_id
    JOIN rul_agreement ra
        ON ra.agreement_id = rc.agreement_id
        AND ra.supplier_client_id = ANY (l_client)
    JOIN rul_client rcl
        ON ra.customer_client_id = rcl.client_id
        AND rcl.client_group_id = l_client_group
    JOIN rul_juristic_company rjc
        ON rjc.client_id = ra.customer_client_id
        AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
        AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
/*
FROM rul_group_consumption rgc
JOIN rul_group_purpose_consumption rgpc
	ON rgc.group_consumption_id = rgpc.group_consumption_id
	AND rgc.group_consumption_id IN (l_group_1, l_group_2,l_group_3)
JOIN rul_purpose_consumption rpc
    ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
LEFT JOIN rul_connection rc
	ON rpc.purpose_consumption_id = rc.purpose_consumption_id
LEFT JOIN rul_charge rch
    ON rch.connection_id = rc.connection_id
LEFT JOIN rul_node_calculate_parameter rncp
    ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    AND rncp.parameter_id IN (1, 6)
LEFT JOIN rul_agreement ra
	ON ra.agreement_id = rc.agreement_id
    AND ra.supplier_client_id = ANY (l_client)
LEFT JOIN rul_client rcl
	ON ra.customer_client_id = rcl.client_id
    AND rcl.client_group_id = l_client_group
LEFT JOIN rul_juristic_company rjc
	ON rjc.client_id = ra.customer_client_id
	AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
    AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
*/
    WHERE rgc.group_consumption_id IN (l_group_1, l_group_2,l_group_3)
    AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
    GROUP BY GROUPING SETS (
        (rjc.shortname, rcl.filial, ra.code_ay ,rgc.group_consumption_name, rpc.purpose_consumption_name),  -- детальные строки
        (ra.code_ay,rjc.shortname,rcl.filial,rgc.group_consumption_name),                                -- итог по группе
        (ra.code_ay,rjc.shortname,rcl.filial),
        ()                                                           -- общий итог
    )
    ORDER BY
        rcl.filial || ' ' || rjc.shortname,
        ra.code_ay,
        GROUPING(rgc.group_consumption_name) desc,
        rgc.group_consumption_name NULLS LAST,
        GROUPING(rpc.purpose_consumption_name) desc,
        rpc.purpose_consumption_name NULLS LAST
        ) filter
    WHERE (l_need_detalization_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose_consumption_name = '    в т.ч. ' ||filter.group_consumption_name
                 OR
                 filter.code_ay is not null
                 ))
    AND (l_need_detalization_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose_consumption_name = '    в т.ч. ' ||filter.group_consumption_name
                 OR
                 filter.code_ay is not null
                 ))
    AND (l_need_detalization_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose_consumption_name = '    в т.ч. ' ||filter.group_consumption_name
                 OR
                 filter.code_ay is not null
                 ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_final_registry_heat_network(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_final_registry_heat_network(p_json json)
 RETURNS TABLE(code_ay character varying, purpose_consumption character varying, param_ov character varying, param_ov_rub character varying, param_gvs character varying, param_gvs_rub character varying, sum_consumption character varying, sum_rub character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::int;
	l_need_detalization_2 int := coalesce(p_json->>'need_detalization_2','0')::int;
	l_need_detalization_3 int := coalesce(p_json->>'need_detalization_3','0')::int;
    l_group_1 BIGINT := coalesce(p_json->>'group_1','0')::BIGINT;
    l_group_2 BIGINT := coalesce(p_json->>'group_2','0')::BIGINT;
    l_group_3 BIGINT := coalesce(p_json->>'group_3','0')::BIGINT;
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
    l_client_group BIGINT := p_json->>'client_group';
BEGIN
    RETURN QUERY
    SELECT 'Код АУ','Назначение','ОВ, Гкал','ОВ, руб','ГВС, Гкал','ГВС, руб','Всего, Гкал','Всего, руб'
    UNION ALL
    SELECT
    	 filter.code_ay, filter.purpose_consumption_name, filter.consumption_param_1, filter.rub_param_1,
         filter.consumption_param_6,filter.rub_param_6,filter.sum_consumption,filter.sum_rub
    FROM (
    SELECT rgc.group_consumption_name,
    CASE
        WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
            THEN ra.code_ay
        ELSE null
    END AS code_ay,
    CASE
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
        	 AND GROUPING(ra.code_ay) = 1 AND GROUPING(rjc.shortname) = 1
            THEN 'Итого'
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
            THEN coalesce(rcl.filial,' ') ||' '|| coalesce(rjc.shortname, ' ')
        WHEN GROUPING(rpc.purpose_consumption_name) = 1
            THEN ('    в т.ч. ' ||rgc.group_consumption_name)::varchar
        ELSE ('        в т.ч. ' || rpc.purpose_consumption_name)::varchar
    END AS purpose_consumption_name,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_6,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_6,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS sum_consumption,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS sum_rub
    FROM rul_charge rch
    JOIN rul_connection rc
        ON rch.connection_id = rc.connection_id
    JOIN rul_node_calculate_parameter rncp
        ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
        AND rncp.parameter_id IN (7)
        AND rncp.target_use_id in (2,3,4)
    JOIN rul_purpose_consumption rpc
        ON rpc.purpose_consumption_id = rc.purpose_consumption_id
    JOIN rul_group_purpose_consumption rgpc
        ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
    JOIN rul_group_consumption rgc
        ON rgc.group_consumption_id = rgpc.group_consumption_id
    JOIN rul_agreement ra
        ON ra.agreement_id = rc.agreement_id
        AND ra.supplier_client_id = ANY (l_client)
    JOIN rul_client rcl
        ON ra.customer_client_id = rcl.client_id
        AND rcl.client_group_id = l_client_group
    JOIN rul_juristic_company rjc
        ON rjc.client_id = ra.customer_client_id
        AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
        AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
/*
FROM rul_group_consumption rgc
JOIN rul_group_purpose_consumption rgpc
	ON rgc.group_consumption_id = rgpc.group_consumption_id
	AND rgc.group_consumption_id IN (l_group_1, l_group_2,l_group_3)
JOIN rul_purpose_consumption rpc
    ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
LEFT JOIN rul_connection rc
	ON rpc.purpose_consumption_id = rc.purpose_consumption_id
LEFT JOIN rul_charge rch
    ON rch.connection_id = rc.connection_id
LEFT JOIN rul_node_calculate_parameter rncp
    ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    AND rncp.parameter_id IN (1, 6)
LEFT JOIN rul_agreement ra
	ON ra.agreement_id = rc.agreement_id
    AND ra.supplier_client_id = ANY (l_client)
LEFT JOIN rul_client rcl
	ON ra.customer_client_id = rcl.client_id
    AND rcl.client_group_id = l_client_group
LEFT JOIN rul_juristic_company rjc
	ON rjc.client_id = ra.customer_client_id
	AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
    AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
*/
    WHERE rgc.group_consumption_id IN (l_group_1, l_group_2,l_group_3)
    AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
    GROUP BY GROUPING SETS (
        (rjc.shortname, rcl.filial, ra.code_ay ,rgc.group_consumption_name, rpc.purpose_consumption_name),  -- детальные строки
        (ra.code_ay,rjc.shortname,rcl.filial,rgc.group_consumption_name),                                -- итог по группе
        (ra.code_ay,rjc.shortname,rcl.filial),
        ()                                                           -- общий итог
    )
    ORDER BY
        rcl.filial || ' ' || rjc.shortname,
        ra.code_ay,
        GROUPING(rgc.group_consumption_name) desc,
        rgc.group_consumption_name NULLS LAST,
        GROUPING(rpc.purpose_consumption_name) desc,
        rpc.purpose_consumption_name NULLS LAST
        ) filter
    WHERE (l_need_detalization_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose_consumption_name = '    в т.ч. ' ||filter.group_consumption_name
                 OR
                 filter.purpose_consumption_name = 'Итого'
                 OR
                 filter.code_ay is not null
                 ))
    AND (l_need_detalization_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose_consumption_name = '    в т.ч. ' ||filter.group_consumption_name
                 OR
                 filter.purpose_consumption_name = 'Итого'
                 OR
                 filter.code_ay is not null
                 ))
    AND (l_need_detalization_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose_consumption_name = '    в т.ч. ' ||filter.group_consumption_name
                 OR
                 filter.purpose_consumption_name = 'Итого'
                 OR
                 filter.code_ay is not null
                 ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_heat_network_heating_house(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_heat_network_heating_house(p_json json)
 RETURNS TABLE(object_id character varying, consumption_name character varying, consumption_1 character varying, consumption_2 character varying, result character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
BEGIN
    RETURN QUERY
    SELECT 'ID ЖД','Жилые дома','Qтэ, Гкал','Fтэ, м2','qтэ, Гкал/м2'
    UNION ALL
    SELECT
    	 filter.object_id, filter.consumption_name, filter.consumption_1, filter.consumption_2,filter.result
    FROM (
    SELECT
    	ro.object_id::varchar,
        rf.formula_name,
        CASE
            WHEN GROUPING(ro.object_id) = 1
                THEN rf.formula_name
            ELSE ('        ' || ro.object_name)::varchar
        END AS consumption_name,
    round(SUM(rch.sum_consumption),2)::varchar(256) AS consumption_1,
    round(SUM(cv2.value),2)::varchar(256) AS consumption_2,
    round(round(SUM(rch.sum_consumption),2)/round(SUM(cv2.value),2),2)::varchar(256) AS result
    FROM rul_charge rch
    JOIN rul_connection rc
        ON rch.connection_id = rc.connection_id
        AND rch.source_id = 1
    JOIN rul_node_calculate_parameter rncp
        ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    JOIN rul_node rn
    	ON rn.node_id = rncp.node_id
    JOIN rul_object ro
    	ON rn.object_id = ro.object_id
    JOIN rul_accounting_type_node ratn
    	ON ratn.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    	AND ratn.start_date < to_timestamp(l_date_report,'YYYY-MM-DD') + interval '1 month' - interval '1 second'
        AND coalesce(ratn.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
        AND ratn.accounting_type_id = 2
    JOIN rul_agreement ra
        ON ra.agreement_id = rc.agreement_id
        AND ra.supplier_client_id = ANY (l_client)
    JOIN rul_client rcl
        ON ra.customer_client_id = rcl.client_id
   	JOIN rul_formula_connection rfc
    	ON rc.connection_id = rfc.connection_id
    JOIN rul_formula rf
    	ON rf.formula_id = rfc.formula_id
        AND rf.method_id = 2
    JOIN rul_version_load_standard vls
    	ON rfc.formula_connection_id = vls.formula_connection_id
    JOIN rul_load_standard_value lsv
    	ON lsv.version_load_standard_id = vls.version_load_standard_id
    JOIN rul_version_constant vc
    	ON vc.formula_id = rf.formula_id
        AND vc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD') + interval '1 month' - interval '1 second'
        AND coalesce(vc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
    JOIN rul_constant_value cv1
        ON cv1.version_constant_id = vc.version_constant_id
    JOIN rul_argument_formula af1
        ON af1.argument_formula_id = cv1.argument_formula_id
        AND af1.argument_formula_code in ('Эталон')
        AND cv1.value = 1
    JOIN rul_constant_value cv2
    	ON cv2.version_constant_id = vc.version_constant_id
    JOIN rul_argument_formula af2
        ON af2.argument_formula_id = lsv.argument_formula_id
        AND af2.argument_formula_code in ('F')
        -------------------------------------------------------------------------------
    WHERE 1=1
    AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
    GROUP BY GROUPING SETS (
        (ro.object_id, rf.formula_name, ro.object_name),  -- детальные строки
        (rf.formula_name)  -- итог по нормативу
    )
    ORDER BY
        GROUPING(rf.formula_name) desc,
        rf.formula_name NULLS LAST,
        GROUPING(ro.object_id) desc,
        ro.object_id NULLS LAST
        ) filter
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_heat_network_heating_house_avg(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_heat_network_heating_house_avg(p_json json)
 RETURNS TABLE(formula_name character varying, consumption_1 character varying, consumption_2 character varying, result character varying, result2 character varying, consumption_2_2 character varying, consumption_1_2 character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
BEGIN
    RETURN QUERY
    SELECT 'Норматив','Qтэ, Гкал','Fтэ, м2','qтэ, Гкал/м2','qт_б/у, Гкал/м2','Fт_б/у, м2','Qт_б/у, Гкал'
    UNION ALL
    SELECT
    	 coalesce(filter.formula_name,filter2.formula_name), filter.consumption_1, filter.consumption_2,filter.result,
         filter2.result,filter2.consumption_2,filter2.consumption_1
    FROM (
    SELECT
        rf.formula_name,
    round(SUM(rch.sum_consumption),2)::varchar(256) AS consumption_1,
    round(SUM(cv2.value),2)::varchar(256) AS consumption_2,
    round(round(SUM(rch.sum_consumption),2)/round(SUM(cv2.value),2),2)::varchar(256) AS result
    FROM rul_charge rch
    JOIN rul_connection rc
        ON rch.connection_id = rc.connection_id
        AND rch.source_id = 1
    JOIN rul_node_calculate_parameter rncp
        ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    JOIN rul_node rn
    	ON rn.node_id = rncp.node_id
    JOIN rul_object ro
    	ON rn.object_id = ro.object_id
    JOIN rul_accounting_type_node ratn
    	ON ratn.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    	AND ratn.start_date < to_timestamp(l_date_report,'YYYY-MM-DD') + interval '1 month' - interval '1 second'
        AND coalesce(ratn.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
        AND ratn.accounting_type_id = 2
    JOIN rul_agreement ra
        ON ra.agreement_id = rc.agreement_id
        AND ra.supplier_client_id = ANY (l_client)
    JOIN rul_client rcl
        ON ra.customer_client_id = rcl.client_id
   	JOIN rul_formula_connection rfc
    	ON rc.connection_id = rfc.connection_id
    JOIN rul_formula rf
    	ON rf.formula_id = rfc.formula_id
        AND rf.method_id = 2
    JOIN rul_version_load_standard vls
    	ON rfc.formula_connection_id = vls.formula_connection_id
    JOIN rul_load_standard_value lsv
    	ON lsv.version_load_standard_id = vls.version_load_standard_id
    JOIN rul_version_constant vc
    	ON vc.formula_id = rf.formula_id
        AND vc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD') + interval '1 month' - interval '1 second'
        AND coalesce(vc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
    JOIN rul_constant_value cv1
        ON cv1.version_constant_id = vc.version_constant_id
    JOIN rul_argument_formula af1
        ON af1.argument_formula_id = cv1.argument_formula_id
        AND af1.argument_formula_code in ('Эталон')
        AND cv1.value = 1
    JOIN rul_constant_value cv2
    	ON cv2.version_constant_id = vc.version_constant_id
    JOIN rul_argument_formula af2
        ON af2.argument_formula_id = lsv.argument_formula_id
        AND af2.argument_formula_code in ('F')
        -------------------------------------------------------------------------------
    WHERE 1=1
    AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
    GROUP BY rf.formula_name
    ORDER BY rf.formula_name NULLS LAST
        ) filter
    -- Отчет из двух частей. Собирает дома по приборному учету и безучетные.
    -- Объединяются по названию норматива
    FULL JOIN
    (
    SELECT
        rf.formula_name,
    round(SUM(rch.sum_consumption),2)::varchar(256) AS consumption_1,
    round(SUM(cv2.value),2)::varchar(256) AS consumption_2,
    round(round(SUM(rch.sum_consumption),2)/round(SUM(cv2.value),2),2)::varchar(256) AS result
    FROM rul_charge rch
    JOIN rul_connection rc
        ON rch.connection_id = rc.connection_id
        AND rch.source_id = 1
    JOIN rul_node_calculate_parameter rncp
        ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    JOIN rul_node rn
    	ON rn.node_id = rncp.node_id
    JOIN rul_object ro
    	ON rn.object_id = ro.object_id
    JOIN rul_accounting_type_node ratn
    	ON ratn.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    	AND ratn.start_date < to_timestamp(l_date_report,'YYYY-MM-DD') + interval '1 month' - interval '1 second'
        AND coalesce(ratn.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
        AND ratn.accounting_type_id = 17
    JOIN rul_agreement ra
        ON ra.agreement_id = rc.agreement_id
        AND ra.supplier_client_id = ANY (l_client)
    JOIN rul_client rcl
        ON ra.customer_client_id = rcl.client_id
   	JOIN rul_formula_connection rfc
    	ON rc.connection_id = rfc.connection_id
    JOIN rul_formula rf
    	ON rf.formula_id = rfc.formula_id
        AND rf.method_id = 2
    JOIN rul_version_load_standard vls
    	ON rfc.formula_connection_id = vls.formula_connection_id
    JOIN rul_load_standard_value lsv
    	ON lsv.version_load_standard_id = vls.version_load_standard_id
    JOIN rul_version_constant vc
    	ON vc.formula_id = rf.formula_id
        AND vc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD') + interval '1 month' - interval '1 second'
        AND coalesce(vc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
    --JOIN rul_constant_value cv1
        --ON cv1.version_constant_id = vc.version_constant_id
    --JOIN rul_argument_formula af1
        --ON af1.argument_formula_id = cv1.argument_formula_id
        --AND af1.argument_formula_code in ('Эталон')
        --AND cv1.value = 1
    JOIN rul_constant_value cv2
    	ON cv2.version_constant_id = vc.version_constant_id
    JOIN rul_argument_formula af2
        ON af2.argument_formula_id = lsv.argument_formula_id
        AND af2.argument_formula_code in ('F')
        -------------------------------------------------------------------------------
    WHERE 1=1
    AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
    GROUP BY rf.formula_name
    ORDER BY rf.formula_name NULLS LAST
        ) filter2
	ON filter.formula_name = filter2.formula_name
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_heat_network_realization(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_heat_network_realization(p_json json)
 RETURNS TABLE(purpose_consumption character varying, param_heat character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::int;
	l_need_detalization_2 int := coalesce(p_json->>'need_detalization_2','0')::int;
    l_group_1 BIGINT := coalesce(p_json->>'group_1','0')::BIGINT;
    l_group_2 BIGINT := coalesce(p_json->>'group_2','0')::BIGINT;
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
BEGIN
    RETURN QUERY
    SELECT 'Группа/Назначение','Количество, Гкал'
    UNION ALL
    SELECT
    	purpose_consumption_name, consumption_param_7
    FROM (
    SELECT
    rgc.group_consumption_name,
    CASE
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
            THEN 'Итого реализовано'
        WHEN GROUPING(rpc.purpose_consumption_name) = 1
            THEN upper(rgc.group_consumption_name)
        ELSE ('    в т.ч. ' || rpc.purpose_consumption_name) ::varchar
    END AS purpose_consumption_name,
    round(SUM(CASE WHEN rncp.parameter_id = 7 THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_7
FROM rul_charge rch
JOIN rul_connection rc
    ON rch.connection_id = rc.connection_id
JOIN rul_node_calculate_parameter rncp
    ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    AND rncp.parameter_id IN (7)
JOIN rul_purpose_consumption rpc
    ON rpc.purpose_consumption_id = rc.purpose_consumption_id
JOIN rul_group_purpose_consumption rgpc
    ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
JOIN rul_group_consumption rgc
    ON rgc.group_consumption_id = rgpc.group_consumption_id
JOIN rul_agreement ra
	ON ra.agreement_id = rc.agreement_id
    AND ra.supplier_client_id = ANY (l_client)
WHERE rgc.group_consumption_id IN (l_group_1, l_group_2)
AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
GROUP BY GROUPING SETS (
    (rgc.group_consumption_name, rpc.purpose_consumption_name),  -- детальные строки
    (rgc.group_consumption_name),                                -- итог по группе
    ()                                                           -- общий итог
)
ORDER BY
	CASE
        WHEN GROUPING(rgc.group_consumption_name) = 1
             AND GROUPING(rpc.purpose_consumption_name) = 1
    THEN 4
    ELSE 1  END,
    GROUPING(rgc.group_consumption_name) desc,
    rgc.group_consumption_name NULLS LAST,
    GROUPING(rpc.purpose_consumption_name) desc,
    rpc.purpose_consumption_name NULLS LAST
    ) filter
WHERE (l_need_detalization_1 = 1 OR
			(filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
             OR
             filter.purpose_consumption_name = upper(filter.group_consumption_name)
             OR
             filter.purpose_consumption_name = 'Итого реализовано'
             ))
AND (l_need_detalization_2 = 1 OR
			(filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
             OR
             filter.purpose_consumption_name = upper(filter.group_consumption_name)
             OR
             filter.purpose_consumption_name = 'Итого реализовано'
             ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_heat_network_svod(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_heat_network_svod(p_json json)
 RETURNS TABLE(purpose character varying, param_ov character varying, param_ov_rub character varying, param_gvs character varying, param_gvs_rub character varying, sum_consumption character varying, sum_rub character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::int;
	l_need_detalization_2 int := coalesce(p_json->>'need_detalization_2','0')::int;
	l_need_detalization_3 int := coalesce(p_json->>'need_detalization_3','0')::int;
    l_need_detalization_4 int := coalesce(p_json->>'need_detalization_4','0')::int;
    l_need_detalization_5 int := coalesce(p_json->>'need_detalization_5','0')::int;
    l_need_detalization_rate_1 int := coalesce(p_json->>'need_detalization_rate_1','0')::int;
	l_need_detalization_rate_2 int := coalesce(p_json->>'need_detalization_rate_2','0')::int;
	l_need_detalization_rate_3 int := coalesce(p_json->>'need_detalization_rate_3','0')::int;
    l_need_detalization_rate_4 int := coalesce(p_json->>'need_detalization_rate_4','0')::int;
    l_need_detalization_rate_5 int := coalesce(p_json->>'need_detalization_rate_5','0')::int;
    l_group_1 BIGINT := coalesce(p_json->>'group_1','0')::BIGINT;
    l_group_2 BIGINT := coalesce(p_json->>'group_2','0')::BIGINT;
    l_group_3 BIGINT := coalesce(p_json->>'group_3','0')::BIGINT;
    l_group_4 BIGINT := coalesce(p_json->>'group_4','0')::BIGINT;
    l_group_5 BIGINT := coalesce(p_json->>'group_5','0')::BIGINT;
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
    l_client_group BIGINT := p_json->>'client_group';
BEGIN
    RETURN QUERY
    SELECT 'Назначение/Тариф','ОВ, Гкал','ОВ, руб','ГВС, Гкал','ГВС, руб','Всего, Гкал','Всего, руб'
    UNION ALL
    SELECT
    	 filter.purpose, filter.consumption_param_1, filter.rub_param_1,
         filter.consumption_param_6,filter.rub_param_6,filter.sum_consumption,filter.sum_rub
    FROM (
    SELECT rgc.group_consumption_name,
	rpc.purpose_consumption_name,
    CASE
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1 AND GROUPING(rr.description) = 1
            THEN 'Итого'
        WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rr.description) = 1
            THEN upper(rgc.group_consumption_name)::varchar
        WHEN GROUPING(rr.description) = 1
            THEN ('    ' || rpc.purpose_consumption_name)::varchar
        ELSE ('        ' || rr.description)::varchar
    END AS purpose,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_6,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_6,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS sum_consumption,
    round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS sum_rub
    FROM rul_charge rch
    JOIN rul_connection rc
        ON rch.connection_id = rc.connection_id
    JOIN rul_rate rr
    	ON
          CASE WHEN rch.source_id = 2
          		 THEN rc.losses_rate_id
               ELSE
               	  rc.rate_id
          END = rr.rate_id
    JOIN rul_node_calculate_parameter rncp
        ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
        AND rncp.parameter_id IN (7)
        AND rncp.target_use_id in (2,3,4)
    JOIN rul_purpose_consumption rpc
        ON rpc.purpose_consumption_id = rc.purpose_consumption_id
    JOIN rul_group_purpose_consumption rgpc
        ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
    JOIN rul_group_consumption rgc
        ON rgc.group_consumption_id = rgpc.group_consumption_id
    JOIN rul_agreement ra
        ON ra.agreement_id = rc.agreement_id
        AND ra.supplier_client_id = ANY (l_client)
    JOIN rul_client rcl
        ON ra.customer_client_id = rcl.client_id
        AND rcl.client_group_id = 1
    JOIN rul_juristic_company rjc
        ON rjc.client_id = ra.customer_client_id
        AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
        AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
    WHERE rgc.group_consumption_id IN (l_group_1, l_group_2,l_group_3,l_group_4,l_group_5)
    AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
    GROUP BY GROUPING SETS (
        (rr.description,rgc.group_consumption_name, rpc.purpose_consumption_name),
        (rgc.group_consumption_name, rpc.purpose_consumption_name),
        (rgc.group_consumption_name),
        ()
    )
    ORDER BY
    	CASE
        WHEN GROUPING(rgc.group_consumption_name) = 1
             AND GROUPING(rpc.purpose_consumption_name) = 1
             AND GROUPING(rr.description) = 1
        THEN 4
        ELSE 1
    END,
        GROUPING(rgc.group_consumption_name) desc,
        rgc.group_consumption_name NULLS LAST,
        GROUPING(rpc.purpose_consumption_name) desc,
        rpc.purpose_consumption_name NULLS LAST,
        GROUPING(rr.description) desc,
        rr.description NULLS LAST
        ) filter
    WHERE (l_need_detalization_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
	AND (l_need_detalization_4 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_4),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_5 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_5),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
	AND (l_need_detalization_rate_4 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_4),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_5 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_5),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_heat_network_svod_with_section(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_heat_network_svod_with_section(p_json json)
 RETURNS TABLE(purpose character varying, param_ov character varying, param_ov_rub character varying, param_gvs character varying, param_gvs_rub character varying, sum_consumption character varying, sum_rub character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::int;
    l_need_detalization_rate_1 int := coalesce(p_json->>'need_detalization_rate_1','0')::int;
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
    l_client_group BIGINT := p_json->>'client_group';
BEGIN
    RETURN QUERY
    SELECT 'Назначение/Тариф','ОВ, Гкал','ОВ, руб','ГВС, Гкал','ГВС, руб','Всего, Гкал','Всего, руб'
    UNION ALL
    SELECT
    	 filter.purpose, filter.consumption_param_1, filter.rub_param_1,
         filter.consumption_param_6,filter.rub_param_6,filter.sum_consumption,filter.sum_rub
    FROM (
    SELECT
      rclas.classifier_name,
      rclas2.classifier_name as cl_name
      ,rpc.purpose_consumption_name
      ,
      CASE
          WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rclas2.classifier_name) = 1
                  AND GROUPING(rr.description) = 1 AND GROUPING(rclas.classifier_name) = 1
              THEN 'Итого'
          WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rr.description) = 1 AND GROUPING(rclas.classifier_name) = 1
              THEN rclas2.classifier_name
          WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rr.description) = 1
              THEN ('    ' || coalesce(rclas.classifier_name,'Без источника'))::varchar
          WHEN GROUPING(rr.description) = 1
              THEN ('        ' || rpc.purpose_consumption_name)::varchar
          ELSE ('            ' || rr.description)::varchar
      END AS purpose,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_1,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_1,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_6,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_6,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS sum_consumption,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS sum_rub
      FROM rul_charge rch
      JOIN rul_connection rc
          ON rch.connection_id = rc.connection_id
      JOIN rul_rate rr
          ON
          CASE WHEN rch.source_id = 2
          		 THEN rc.losses_rate_id
               ELSE
               	  rc.rate_id
          END = rr.rate_id
      JOIN rul_node_calculate_parameter rncp
          ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
          AND rncp.parameter_id IN (7)
          AND rncp.target_use_id in (2,3,4)
      --Добавляю классификаторы---------------------------------------------------------------------------
      JOIN rul_line_parameter rlp
          ON rncp.node_calculate_parameter_id = rlp.node_calculate_parameter_id
      JOIN rul_line rl
          ON rlp.line_id = rl.line_id
      JOIN rul_classifier_network_fragment rcnf
          ON rl.network_fragment_id = rcnf.network_fragment_id
      LEFT JOIN rul_classifier rclas
          ON rclas.classifier_id = rcnf.classifier_id
          AND rclas.classifier_type_id = 3
      LEFT JOIN rul_classifier rclas2
          ON rclas2.classifier_id = rcnf.classifier_id
          AND rclas2.classifier_type_id = 2
      ----------------------------------------------------------------------------------------------------
      JOIN rul_purpose_consumption rpc
          ON rpc.purpose_consumption_id = rc.purpose_consumption_id
      JOIN rul_group_purpose_consumption rgpc
          ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
      JOIN rul_group_consumption rgc
          ON rgc.group_consumption_id = rgpc.group_consumption_id
      JOIN rul_agreement ra
          ON ra.agreement_id = rc.agreement_id
          AND ra.supplier_client_id = ANY (l_client)
      JOIN rul_client rcl
          ON ra.customer_client_id = rcl.client_id
          AND rcl.client_group_id = l_client_group
      JOIN rul_juristic_company rjc
          ON rjc.client_id = ra.customer_client_id
          AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
          AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
      WHERE 1=1
      AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
      GROUP BY GROUPING SETS (
          (rclas.classifier_name,rr.description,rclas2.classifier_name, rpc.purpose_consumption_name),
          (rclas.classifier_name,rclas2.classifier_name, rpc.purpose_consumption_name),
          (rclas.classifier_name,rclas2.classifier_name),
          (rclas2.classifier_name),
          ()
      )
      ORDER BY
          CASE
          WHEN GROUPING(rclas2.classifier_name) = 1
               AND GROUPING(rpc.purpose_consumption_name) = 1
               AND GROUPING(rr.description) = 1
          THEN 4
          ELSE 1
      END,
          GROUPING(rclas2.classifier_name) desc,
          rclas2.classifier_name NULLS LAST,
          GROUPING(rclas.classifier_name) desc,
          rclas.classifier_name NULLS LAST,
          GROUPING(rpc.purpose_consumption_name) desc,
          rpc.purpose_consumption_name NULLS LAST,
          GROUPING(rr.description) desc,
          rr.description NULLS LAST
        ) filter
    WHERE (l_need_detalization_1 = 1 OR
                (
                 filter.purpose = filter.cl_name
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_1 = 1 OR
                (
                 filter.purpose = '        ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = filter.cl_name
                 OR
                 filter.purpose = 'Итого'
                 ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_heat_network_svod_with_source(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_heat_network_svod_with_source(p_json json)
 RETURNS TABLE(purpose character varying, param_ov character varying, param_ov_rub character varying, param_gvs character varying, param_gvs_rub character varying, sum_consumption character varying, sum_rub character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::int;
	l_need_detalization_2 int := coalesce(p_json->>'need_detalization_2','0')::int;
	l_need_detalization_3 int := coalesce(p_json->>'need_detalization_3','0')::int;
    l_need_detalization_4 int := coalesce(p_json->>'need_detalization_4','0')::int;
    l_need_detalization_5 int := coalesce(p_json->>'need_detalization_5','0')::int;
    l_need_detalization_rate_1 int := coalesce(p_json->>'need_detalization_rate_1','0')::int;
	l_need_detalization_rate_2 int := coalesce(p_json->>'need_detalization_rate_2','0')::int;
	l_need_detalization_rate_3 int := coalesce(p_json->>'need_detalization_rate_3','0')::int;
    l_need_detalization_rate_4 int := coalesce(p_json->>'need_detalization_rate_4','0')::int;
    l_need_detalization_rate_5 int := coalesce(p_json->>'need_detalization_rate_5','0')::int;
    l_group_1 BIGINT := coalesce(p_json->>'group_1','0')::BIGINT;
    l_group_2 BIGINT := coalesce(p_json->>'group_2','0')::BIGINT;
    l_group_3 BIGINT := coalesce(p_json->>'group_3','0')::BIGINT;
    l_group_4 BIGINT := coalesce(p_json->>'group_4','0')::BIGINT;
    l_group_5 BIGINT := coalesce(p_json->>'group_5','0')::BIGINT;
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
    l_client_group BIGINT := p_json->>'client_group';
BEGIN
    RETURN QUERY
    SELECT 'Назначение/Тариф','ОВ, Гкал','ОВ, руб','ГВС, Гкал','ГВС, руб','Всего, Гкал','Всего, руб'
    UNION ALL
    SELECT
    	 filter.purpose, filter.consumption_param_1, filter.rub_param_1,
         filter.consumption_param_6,filter.rub_param_6,filter.sum_consumption,filter.sum_rub
    FROM (
    SELECT
      rclas.classifier_name,
      rgc.group_consumption_name
      ,rpc.purpose_consumption_name
      ,
      CASE
          WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
                  AND GROUPING(rr.description) = 1 AND GROUPING(rclas.classifier_name) = 1
              THEN 'Итого'
          WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rr.description) = 1 AND GROUPING(rclas.classifier_name) = 1
              THEN upper(rgc.group_consumption_name)::varchar
          WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rr.description) = 1
              THEN ('    ' || coalesce(rclas.classifier_name,'Без источника'))::varchar
          WHEN GROUPING(rr.description) = 1
              THEN ('        ' || rpc.purpose_consumption_name)::varchar
          ELSE ('            ' || rr.description)::varchar
      END AS purpose,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_1,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_1,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_6,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2) THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_6,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS sum_consumption,
      round(SUM(CASE WHEN rncp.parameter_id = 7 AND rncp.target_use_id in (2,3,4) THEN round(rch.amount,2) END),2)::varchar(256) AS sum_rub
      FROM rul_charge rch
      JOIN rul_connection rc
          ON rch.connection_id = rc.connection_id
      JOIN rul_rate rr
          ON
          CASE WHEN rch.source_id = 2
          		 THEN rc.losses_rate_id
               ELSE
               	  rc.rate_id
          END = rr.rate_id
      JOIN rul_node_calculate_parameter rncp
          ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
          AND rncp.parameter_id IN (7)
          AND rncp.target_use_id in (2,3,4)
      --Добавляю классификаторы---------------------------------------------------------------------------
      JOIN rul_line_parameter rlp
          ON rncp.node_calculate_parameter_id = rlp.node_calculate_parameter_id
      JOIN rul_line rl
          ON rlp.line_id = rl.line_id
      JOIN rul_classifier_network_fragment rcnf
          ON rl.network_fragment_id = rcnf.network_fragment_id
      LEFT JOIN rul_classifier rclas
          ON rclas.classifier_id = rcnf.classifier_id
          AND rclas.classifier_type_id = 3
      ----------------------------------------------------------------------------------------------------
      JOIN rul_purpose_consumption rpc
          ON rpc.purpose_consumption_id = rc.purpose_consumption_id
      JOIN rul_group_purpose_consumption rgpc
          ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
      JOIN rul_group_consumption rgc
          ON rgc.group_consumption_id = rgpc.group_consumption_id
      JOIN rul_agreement ra
          ON ra.agreement_id = rc.agreement_id
          AND ra.supplier_client_id = ANY (l_client)
      JOIN rul_client rcl
          ON ra.customer_client_id = rcl.client_id
          AND rcl.client_group_id = l_client_group
      JOIN rul_juristic_company rjc
          ON rjc.client_id = ra.customer_client_id
          AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
          AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
      WHERE 1=1
      AND rgc.group_consumption_id IN (l_group_1, l_group_2,l_group_3,l_group_4,l_group_5)
      AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
      GROUP BY GROUPING SETS (
          (rclas.classifier_name,rr.description,rgc.group_consumption_name, rpc.purpose_consumption_name),
          (rclas.classifier_name,rgc.group_consumption_name, rpc.purpose_consumption_name),
          (rclas.classifier_name,rgc.group_consumption_name),
          (rgc.group_consumption_name),
          ()
      )
      ORDER BY
          CASE
          WHEN GROUPING(rgc.group_consumption_name) = 1
               AND GROUPING(rpc.purpose_consumption_name) = 1
               AND GROUPING(rr.description) = 1
          THEN 4
          ELSE 1
      END,
          GROUPING(rgc.group_consumption_name) desc,
          rgc.group_consumption_name NULLS LAST,
          GROUPING(rclas.classifier_name) desc,
          rclas.classifier_name NULLS LAST,
          GROUPING(rpc.purpose_consumption_name) desc,
          rpc.purpose_consumption_name NULLS LAST,
          GROUPING(rr.description) desc,
          rr.description NULLS LAST
        ) filter
    WHERE (l_need_detalization_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
	AND (l_need_detalization_4 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_4),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_5 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_5),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose = '        ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose = '        ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose = '        ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
	AND (l_need_detalization_rate_4 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_4),'0')
                 OR
                 filter.purpose = '        ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_5 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_5),'0')
                 OR
                 filter.purpose = '        ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_vodokanal_realization(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_vodokanal_realization(p_json json)
 RETURNS TABLE(purpose_consumption character varying, param_water character varying, param_canalization character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::INT;
	l_need_detalization_2 int := coalesce(p_json->>'need_detalization_2','0')::INT;
    l_group_1 BIGINT := p_json->>'group_1';
    l_group_2 BIGINT := p_json->>'group_2';
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
BEGIN
    RETURN QUERY
    SELECT 'Группа/Назначение','Вода, м3','Стоки, м3'
    UNION ALL
    SELECT
    	purpose_consumption_name, consumption_param_1, consumption_param_6
    FROM (
    SELECT
    rgc.group_consumption_name,
    CASE
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1
            THEN 'Итого реализовано'
        WHEN GROUPING(rpc.purpose_consumption_name) = 1
            THEN upper(rgc.group_consumption_name)
        ELSE ('    в т.ч. ' || rpc.purpose_consumption_name) ::varchar
    END AS purpose_consumption_name,
    round(SUM(CASE WHEN rncp.parameter_id = 1 THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 6 THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_6
FROM rul_charge rch
JOIN rul_connection rc
    ON rch.connection_id = rc.connection_id
JOIN rul_node_calculate_parameter rncp
    ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
    AND rncp.parameter_id IN (1, 6)
JOIN rul_purpose_consumption rpc
    ON rpc.purpose_consumption_id = rc.purpose_consumption_id
JOIN rul_group_purpose_consumption rgpc
    ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
JOIN rul_group_consumption rgc
    ON rgc.group_consumption_id = rgpc.group_consumption_id
JOIN rul_agreement ra
	ON ra.agreement_id = rc.agreement_id
    AND ra.supplier_client_id = ANY (l_client)
WHERE rgc.group_consumption_id IN (l_group_1, l_group_2)
AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
GROUP BY GROUPING SETS (
    (rgc.group_consumption_name, rpc.purpose_consumption_name),  -- детальные строки
    (rgc.group_consumption_name),                                -- итог по группе
    ()                                                           -- общий итог
)
ORDER BY
	CASE
        WHEN GROUPING(rgc.group_consumption_name) = 1
             AND GROUPING(rpc.purpose_consumption_name) = 1
    THEN 4
    ELSE 1  END,
    GROUPING(rgc.group_consumption_name) desc,
    rgc.group_consumption_name NULLS LAST,
    GROUPING(rpc.purpose_consumption_name) desc,
    rpc.purpose_consumption_name NULLS LAST
    ) filter
WHERE (l_need_detalization_1 = 1 OR
			(filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
             OR
             filter.purpose_consumption_name = upper(filter.group_consumption_name)
             OR
             filter.purpose_consumption_name = 'Итого реализовано'
             ))
AND (l_need_detalization_2 = 1 OR
			(filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
             OR
             filter.purpose_consumption_name = upper(filter.group_consumption_name)
             OR
             filter.purpose_consumption_name = 'Итого реализовано'
             ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_report_vodokanal_svod(p_json json)
CREATE OR REPLACE FUNCTION public.get_report_vodokanal_svod(p_json json)
 RETURNS TABLE(purpose character varying, param_water character varying, param_water_rub character varying, param_canalization character varying, param_canalization_rub character varying, sum_consumption character varying, sum_rub character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE
	l_need_detalization_1 int := coalesce(p_json->>'need_detalization_1','0')::INT;
	l_need_detalization_2 int := coalesce(p_json->>'need_detalization_2','0')::INT;
	l_need_detalization_3 int := coalesce(p_json->>'need_detalization_3','0')::INT;
    l_need_detalization_4 int := coalesce(p_json->>'need_detalization_4','0')::INT;
    l_need_detalization_5 int := coalesce(p_json->>'need_detalization_5','0')::INT;
    l_need_detalization_rate_1 int := coalesce(p_json->>'need_detalization_rate_1','0')::INT;
	l_need_detalization_rate_2 int := coalesce(p_json->>'need_detalization_rate_2','0')::INT;
	l_need_detalization_rate_3 int := coalesce(p_json->>'need_detalization_rate_3','0')::INT;
    l_need_detalization_rate_4 int := coalesce(p_json->>'need_detalization_rate_4','0')::INT;
    l_need_detalization_rate_5 int := coalesce(p_json->>'need_detalization_rate_5','0')::INT;
    l_group_1 BIGINT := coalesce(p_json->>'group_1','0')::BIGINT;
    l_group_2 BIGINT := coalesce(p_json->>'group_2','0')::BIGINT;
    l_group_3 BIGINT := coalesce(p_json->>'group_3','0')::BIGINT;
    l_group_4 BIGINT := coalesce(p_json->>'group_4','0')::BIGINT;
    l_group_5 BIGINT := coalesce(p_json->>'group_5','0')::BIGINT;
    l_client BIGINT[] := (SELECT ARRAY(SELECT value::BIGINT FROM json_array_elements_text(p_json->'clients')));
    l_date_report varchar := p_json->>'date_report';
    l_client_group BIGINT := p_json->>'client_group';
BEGIN
    RETURN QUERY
    SELECT 'Назначение/Тариф','Вода, м3','Вода, руб','Стоки, м3','Стоки, руб','Всего, м3','Всего, руб'
    UNION ALL
    SELECT
    	 filter.purpose, filter.consumption_param_1, filter.rub_param_1,
         filter.consumption_param_6,filter.rub_param_6,filter.sum_consumption,filter.sum_rub
    FROM (
    SELECT rgc.group_consumption_name,
	rpc.purpose_consumption_name,
    CASE
    	WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rgc.group_consumption_name) = 1 AND GROUPING(rr.description) = 1
            THEN 'Итого'
        WHEN GROUPING(rpc.purpose_consumption_name) = 1 AND GROUPING(rr.description) = 1
            THEN upper(rgc.group_consumption_name)::varchar
        WHEN GROUPING(rr.description) = 1
            THEN ('    ' || rpc.purpose_consumption_name)::varchar
        ELSE ('        ' || rr.description)::varchar
    END AS purpose,
    round(SUM(CASE WHEN rncp.parameter_id = 1 THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 1 THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_1,
    round(SUM(CASE WHEN rncp.parameter_id = 6 THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS consumption_param_6,
    round(SUM(CASE WHEN rncp.parameter_id = 6 THEN round(rch.amount,2) END),2)::varchar(256) AS rub_param_6,
    round(SUM(CASE WHEN rncp.parameter_id in (1,6) THEN round(rch.sum_consumption,2) END),2)::varchar(256) AS sum_consumption,
    round(SUM(CASE WHEN rncp.parameter_id in (1,6) THEN round(rch.amount,2) END),2)::varchar(256) AS sum_rub
    FROM rul_charge rch
    JOIN rul_connection rc
        ON rch.connection_id = rc.connection_id
    JOIN rul_rate rr
    	ON
          CASE WHEN rch.source_id = 2
          		 THEN rc.losses_rate_id
               ELSE
               	  rc.rate_id
          END = rr.rate_id
    JOIN rul_node_calculate_parameter rncp
        ON rc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
        AND rncp.parameter_id IN (1, 6)
    JOIN rul_purpose_consumption rpc
        ON rpc.purpose_consumption_id = rc.purpose_consumption_id
    JOIN rul_group_purpose_consumption rgpc
        ON rpc.purpose_consumption_id = rgpc.purpose_consumption_id
    JOIN rul_group_consumption rgc
        ON rgc.group_consumption_id = rgpc.group_consumption_id
    JOIN rul_agreement ra
        ON ra.agreement_id = rc.agreement_id
        AND ra.supplier_client_id = ANY (l_client)
    JOIN rul_client rcl
        ON ra.customer_client_id = rcl.client_id
        AND rcl.client_group_id = 1
    JOIN rul_juristic_company rjc
        ON rjc.client_id = ra.customer_client_id
        AND rjc.start_date < to_timestamp(l_date_report,'YYYY-MM-DD')
        AND coalesce(rjc.end_date,'2100-04-30 23:59:59+03') > to_timestamp(l_date_report,'YYYY-MM-DD')
    WHERE rgc.group_consumption_id IN (l_group_1, l_group_2,l_group_3,l_group_4,l_group_5)
    AND billing_start_date = to_timestamp(l_date_report,'YYYY-MM-DD')
    GROUP BY GROUPING SETS (
        (rr.description,rgc.group_consumption_name, rpc.purpose_consumption_name),
        (rgc.group_consumption_name, rpc.purpose_consumption_name),
        (rgc.group_consumption_name),
        ()
    )
    ORDER BY
    	CASE
        WHEN GROUPING(rgc.group_consumption_name) = 1
             AND GROUPING(rpc.purpose_consumption_name) = 1
             AND GROUPING(rr.description) = 1
        THEN 4
        ELSE 1
    END,
        GROUPING(rgc.group_consumption_name) desc,
        rgc.group_consumption_name NULLS LAST,
        GROUPING(rpc.purpose_consumption_name) desc,
        rpc.purpose_consumption_name NULLS LAST,
        GROUPING(rr.description) desc,
        rr.description NULLS LAST
        ) filter
    WHERE (l_need_detalization_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
	AND (l_need_detalization_4 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_4),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_5 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_5),'0')
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_1 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_1),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_2 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_2),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_3 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_3),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
	AND (l_need_detalization_rate_4 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_4),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    AND (l_need_detalization_rate_5 = 1 OR
                (filter.group_consumption_name != COALESCE((select group_consumption_name from rul_group_consumption where group_consumption_id = l_group_5),'0')
                 OR
                 filter.purpose = '    ' ||filter.purpose_consumption_name
                 OR
                 filter.purpose = upper(filter.group_consumption_name)
                 OR
                 filter.purpose = 'Итого'
                 ))
    ;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_serial_number(p_node_panel_id bigint)
CREATE OR REPLACE FUNCTION public.get_serial_number(p_node_panel_id bigint)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
	v_serial_number varchar;
BEGIN
    select MAX(serial_number) into v_serial_number from rul_meter where meter_id =
    	( select meter_id from rul_node_meter where node_meter_id =
        	(select node_meter_id from rul_node_panel where node_panel_id = p_node_panel_id)
        );
    RETURN v_serial_number;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_temperature(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_observation_type_id bigint, p_observation_period_id bigint)
CREATE OR REPLACE FUNCTION public.get_temperature(p_start_date timestamp without time zone, p_end_date timestamp without time zone, p_node_calculate_parameter_id bigint, p_observation_type_id bigint, p_observation_period_id bigint)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
	v_avg_temperature numeric;
    v_days integer;
    v_counts integer;
    v_locality varchar(2048);
    v_month_name text;
BEGIN
 v_month_name := (ARRAY['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                           'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'])[EXTRACT(MONTH FROM p_start_date)];
    v_days := (p_end_date::date - p_start_date::date)+1;
	select round(sum(temperature)/count(*),2),max(l.locality_name), count(1) into v_avg_temperature,v_locality, v_counts
    from rul_observation obs
    join rul_object obj on obs.locality_id = obj.locality_id
    join rul_locality l on obj.locality_id = l.locality_id
    join rul_node n on n.object_id = obj.object_id
    join rul_node_calculate_parameter ncp on ncp.node_id = n.node_id
    where observation_type_id = p_observation_type_id
    and observation_period_id = p_observation_period_id
    and node_calculate_parameter_id = p_node_calculate_parameter_id
    and observation_date >= p_start_date
    and observation_date <= p_end_date;
    IF v_days>v_counts AND p_observation_type_id = 1 THEN
        RAISE EXCEPTION '%', get_message('ERR_GET_TEMPERATURE_AIR', format('%s по %s', v_month_name, v_locality));
    ELSEIF v_days>v_counts AND p_observation_type_id = 2 THEN
         RAISE EXCEPTION '%', get_message('ERR_GET_TEMPERATURE_LAND',format('%s по %s', v_month_name, v_locality));
    end if;
    RETURN v_avg_temperature;
END;
$function$

-- ======================================================================

-- FUNCTION: public.get_v_p(p_section_id bigint, p_start_date timestamp without time zone)
CREATE OR REPLACE FUNCTION public.get_v_p(p_section_id bigint, p_start_date timestamp without time zone)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
V_p numeric;
-- v.0.1
begin
    -- Расчет объема подающего и обратного трубопроводов для потерь.
    -- V_р =  m* (F1*L1*(1+Кстар1) + F2*L2*(1+Кстар2))
	-- Кстар1 =[(<год расчетного периода>-<год прокладки>)*П/s1]^2.6 - при этом если Кстар1>3, то Кстар1=3
	-- Кстар2 =[(<год расчетного периода>-<год прокладки>)*П/s2]^2.6 - при этом если Кстар1>3, то Кстар1=3
    with K1 as
          (
              select case when (((EXTRACT(YEAR FROM p_start_date) - EXTRACT(YEAR FROM section_year))  * П / s1)^2.6 ) * 3 > 3 then 3
                  else (((EXTRACT(YEAR FROM p_start_date) - EXTRACT(YEAR FROM section_year))  * П / s1)^2.6 ) * 3 end as K
                  from
                    (
                    select
                    (select installation_date from public.rul_section where section_id = p_section_id) as section_year,
                    (select value from public.rul_attribute_section_value where attribute_section_id = 6 and section_id = p_section_id) as s1,
                    (select value from public.rul_attribute_section_value where attribute_section_id = 12 and section_id = p_section_id) as П
                    ) vals
          ),
          K2 as
          (
              select case when (((EXTRACT(YEAR FROM p_start_date) - EXTRACT(YEAR FROM section_year))  * П / s2)^2.6 ) * 3 > 3 then 3
                  else (((EXTRACT(YEAR FROM p_start_date) - EXTRACT(YEAR FROM section_year))  * П / s2)^2.6 ) * 3 end as K
                  from
                    (
                    select
                    (select installation_date from public.rul_section where section_id = p_section_id) as section_year,
                    (select value from public.rul_attribute_section_value where attribute_section_id = 7 and section_id = p_section_id) as s2,
                    (select value from public.rul_attribute_section_value where attribute_section_id = 12 and section_id = p_section_id) as П
                    ) vals
          )
      select
          (
            (PI()/4 * ((Dn1 - 2 * s1) ^ 2) * 0.000001) * L1 * (1 + (select K from K1))
            +
            (PI()/4 * ((Dn2 - 2 * s2) ^ 2) * 0.000001) * L2 * (1 + (select K from K2))
          ) * m into V_p
          from
      (
      select (select value from public.rul_attribute_section_value where attribute_section_id = 4 and section_id = p_section_id) as Dn1,
          (select value from public.rul_attribute_section_value where attribute_section_id = 6 and section_id = p_section_id) as s1,
          (select value from public.rul_attribute_section_value where attribute_section_id = 5 and section_id = p_section_id) as Dn2,
          (select value from public.rul_attribute_section_value where attribute_section_id = 7 and section_id = p_section_id) as s2,
          (select value from public.rul_attribute_section_value where attribute_section_id = 8 and section_id = p_section_id) as L1,
          (select value from public.rul_attribute_section_value where attribute_section_id = 9 and section_id = p_section_id) as L2,
          (select value from public.rul_attribute_section_value where attribute_section_id = 11 and section_id = p_section_id) as m
          ) vals;
RETURN V_p;
end;
$function$

-- ======================================================================

-- FUNCTION: public.report_generate(p_main_table text, p_schema text, p_joins report_join[], p_columns text[], p_conditions report_condition[], p_aggregates report_aggregate[], p_group_by text[], p_sort report_sort[], p_limit integer, p_offset integer)
CREATE OR REPLACE FUNCTION public.report_generate(p_main_table text, p_schema text DEFAULT 'public'::text, p_joins report_join[] DEFAULT NULL::report_join[], p_columns text[] DEFAULT NULL::text[], p_conditions report_condition[] DEFAULT NULL::report_condition[], p_aggregates report_aggregate[] DEFAULT NULL::report_aggregate[], p_group_by text[] DEFAULT NULL::text[], p_sort report_sort[] DEFAULT NULL::report_sort[], p_limit integer DEFAULT NULL::integer, p_offset integer DEFAULT NULL::integer)
 RETURNS SETOF record
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    v_sql TEXT := '';
    v_select TEXT := '';
    v_from TEXT := '';
    v_join TEXT := '';
    v_where TEXT := '';
    v_group_by TEXT := '';
    v_order_by TEXT := '';
    v_limit_offset TEXT := '';
    v_col TEXT;
    v_col_parts TEXT[];
    v_col_alias TEXT;
    v_col_name TEXT;
    v_table_name TEXT;
    v_join_item report_join;
    v_cond report_condition;
    v_sort_item report_sort;
    v_agg report_aggregate;
    v_first BOOLEAN := TRUE;
    v_added_cols TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Защита от инъекций: валидация имен
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = p_schema AND table_name = p_main_table
    ) THEN
        RAISE EXCEPTION 'Таблица %.% не существует', p_schema, p_main_table;
    END IF;
    -- SELECT часть
    IF p_aggregates IS NOT NULL AND array_length(p_aggregates, 1) > 0 THEN
        v_first := TRUE;
        FOR v_agg IN SELECT * FROM unnest(p_aggregates) LOOP
            IF NOT v_first THEN v_select := v_select || ', '; END IF;
            v_select := v_select || format('%s(%s)::TEXT AS %s',
                upper(v_agg.function_name),
                quote_ident(v_agg.column_name),
                quote_ident(coalesce(v_agg.alias, v_agg.function_name || '_' || v_agg.column_name))
            );
            v_first := FALSE;
        END LOOP;
        -- Добавляем неагрегированные колонки для GROUP BY (без дубликатов)
        v_added_cols := ARRAY[]::TEXT[];
        IF p_columns IS NOT NULL THEN
            FOREACH v_col IN ARRAY p_columns LOOP
                -- Проверяем дубликаты
                IF NOT (v_col = ANY(v_added_cols)) THEN
                    IF position('.' IN v_col) > 0 THEN
                        -- Формат: alias.column или table.column
                        v_col_parts := string_to_array(v_col, '.');
                        v_select := v_select || ', ' || format('%I.%I::TEXT',
                            v_col_parts[1],
                            v_col_parts[2]
                        );
                    ELSE
                        -- Основная таблица
                        v_select := v_select || ', ' || format('%I.%I::TEXT',
                            p_main_table, v_col
                        );
                    END IF;
                    v_added_cols := array_append(v_added_cols, v_col);
                END IF;
            END LOOP;
        END IF;
    ELSIF p_columns IS NOT NULL AND array_length(p_columns, 1) > 0 THEN
        v_first := TRUE;
        v_added_cols := ARRAY[]::TEXT[];
        FOREACH v_col IN ARRAY p_columns LOOP
            -- Проверяем дубликаты
            IF NOT (v_col = ANY(v_added_cols)) THEN
                IF NOT v_first THEN v_select := v_select || ', '; END IF;
                -- Проверяем есть ли точка (указан алиас/таблица)
                IF position('.' IN v_col) > 0 THEN
                    -- Формат: alias.column или table.column
                    v_col_parts := string_to_array(v_col, '.');
                    v_table_name := v_col_parts[1];
                    v_col_name := v_col_parts[2];
                    v_col_alias := replace(v_col, '.', '_');
                    -- Формируем: "alias"."column"::TEXT AS alias_column
                    v_select := v_select || format('%I.%I::TEXT AS %I',
                        v_table_name,
                        v_col_name,
                        v_col_alias
                    );
                ELSE
                    -- Б��з точки — это основная таблица
                    v_col_alias := p_main_table || '_' || v_col;
                    v_select := v_select || format('%I.%I::TEXT AS %I',
                        p_main_table,
                        v_col,
                        v_col_alias
                    );
                END IF;
                v_added_cols := array_append(v_added_cols, v_col);
                v_first := FALSE;
            END IF;
        END LOOP;
    ELSE
        v_select := '*';
    END IF;
    -- FROM часть
    v_from := format('%I.%I', p_schema, p_main_table);
    -- JOIN часть
    IF p_joins IS NOT NULL THEN
        FOREACH v_join_item IN ARRAY p_joins LOOP
            -- Валидация таблицы для джоина
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = p_schema AND table_name = v_join_item.table_name
            ) THEN
                RAISE EXCEPTION 'Таблица для JOIN %.% не существует', p_schema, v_join_item.table_name;
            END IF;
            v_join := v_join || format(' %s JOIN %I.%I %s ON %I.%I = %I.%I',
                upper(v_join_item.join_type),
                p_schema, v_join_item.table_name,
                quote_ident(coalesce(v_join_item.alias, v_join_item.table_name)),
                quote_ident(p_main_table), quote_ident(v_join_item.left_column),
                quote_ident(coalesce(v_join_item.alias, v_join_item.table_name)),
                quote_ident(v_join_item.right_column)
            );
        END LOOP;
    END IF;
    -- WHERE часть
    IF p_conditions IS NOT NULL THEN
        v_first := TRUE;
        v_where := 'WHERE ';
        FOREACH v_cond IN ARRAY p_conditions LOOP
            IF NOT v_first AND v_cond.logic_operator IS NOT NULL THEN
                v_where := v_where || ' ' || upper(v_cond.logic_operator) || ' ';
            END IF;
            -- Проверяем, есть ли точка в имени колонки
            IF position('.' IN v_cond.column_name) > 0 THEN
                v_col_parts := string_to_array(v_cond.column_name, '.');
                IF upper(v_cond.operator) = 'IN' THEN
                    v_where := v_where || format('%I.%I IN (%s)', v_col_parts[1], v_col_parts[2], v_cond.value);
                ELSIF upper(v_cond.operator) = 'LIKE' THEN
                    v_where := v_where || format('%I.%I LIKE %L', v_col_parts[1], v_col_parts[2], v_cond.value);
                ELSE
                    v_where := v_where || format('%I.%I %s %L', v_col_parts[1], v_col_parts[2], v_cond.operator, v_cond.value);
                END IF;
            ELSE
                -- Без точки — основная таблица
                IF upper(v_cond.operator) = 'IN' THEN
                    v_where := v_where || format('%I.%I IN (%s)', p_main_table, v_cond.column_name, v_cond.value);
                ELSIF upper(v_cond.operator) = 'LIKE' THEN
                    v_where := v_where || format('%I.%I LIKE %L', p_main_table, v_cond.column_name, v_cond.value);
                ELSE
                    v_where := v_where || format('%I.%I %s %L', p_main_table, v_cond.column_name, v_cond.operator, v_cond.value);
                END IF;
            END IF;
            v_first := FALSE;
        END LOOP;
    END IF;
    -- GROUP BY часть
    IF p_group_by IS NOT NULL AND array_length(p_group_by, 1) > 0 THEN
        v_group_by := 'GROUP BY ';
        v_first := TRUE;
        v_added_cols := ARRAY[]::TEXT[];
        FOREACH v_col IN ARRAY p_group_by LOOP
            IF NOT (v_col = ANY(v_added_cols)) THEN
                IF NOT v_first THEN v_group_by := v_group_by || ', '; END IF;
                IF position('.' IN v_col) > 0 THEN
                    v_col_parts := string_to_array(v_col, '.');
                    v_group_by := v_group_by || format('%I.%I', v_col_parts[1], v_col_parts[2]);
                ELSE
                    v_group_by := v_group_by || format('%I.%I', p_main_table, v_col);
                END IF;
                v_added_cols := array_append(v_added_cols, v_col);
                v_first := FALSE;
            END IF;
        END LOOP;
    END IF;
    -- ORDER BY часть
    IF p_sort IS NOT NULL AND array_length(p_sort, 1) > 0 THEN
        v_first := TRUE;
        v_order_by := 'ORDER BY ';
        FOREACH v_sort_item IN ARRAY p_sort LOOP
            IF NOT v_first THEN v_order_by := v_order_by || ', '; END IF;
            IF position('.' IN v_sort_item.column_name) > 0 THEN
                v_col_parts := string_to_array(v_sort_item.column_name, '.');
                v_order_by := v_order_by || format('%I.%I %s',
                    v_col_parts[1],
                    v_col_parts[2],
                    upper(v_sort_item.direction)
                );
            ELSE
                v_order_by := v_order_by || format('%I.%I %s',
                    p_main_table,
                    v_sort_item.column_name,
                    upper(v_sort_item.direction)
                );
            END IF;
            v_first := FALSE;
        END LOOP;
    END IF;
    -- LIMIT/OFFSET
    IF p_limit IS NOT NULL THEN
        v_limit_offset := format('LIMIT %s', p_limit);
        IF p_offset IS NOT NULL THEN
            v_limit_offset := v_limit_offset || format(' OFFSET %s', p_offset);
        END IF;
    END IF;
    -- Сборка финального запроса
    v_sql := format('SELECT %s FROM %s %s %s %s %s %s',
        v_select,
        v_from,
        v_join,
        v_where,
        v_group_by,
        v_order_by,
        v_limit_offset
    );
    RAISE NOTICE 'Сгенерированный SQL: %', v_sql;
    -- Выполнение динамического SQL
    RETURN QUERY EXECUTE v_sql;
END;
$function$

-- ======================================================================

-- FUNCTION: public.report_get_columns(p_tables text[], p_schema text)
CREATE OR REPLACE FUNCTION public.report_get_columns(p_tables text[], p_schema text DEFAULT 'public'::text)
 RETURNS TABLE(table_name text, column_name text, data_type text, is_nullable boolean, column_default text, column_comment text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        c.table_name::TEXT,
        c.column_name::TEXT,
        c.data_type::TEXT,
        (c.is_nullable = 'YES') AS is_nullable,
        c.column_default::TEXT,
        col_description(format('%I.%I', p_schema, c.table_name)::regclass::oid, c.ordinal_position)::TEXT
    FROM information_schema.columns c
    WHERE c.table_schema = p_schema
      AND c.table_name = ANY(p_tables)
    ORDER BY c.table_name, c.ordinal_position;
END;
$function$

-- ======================================================================

-- FUNCTION: public.report_get_join_columns(p_table1 text, p_table2 text, p_schema text)
CREATE OR REPLACE FUNCTION public.report_get_join_columns(p_table1 text, p_table2 text, p_schema text DEFAULT 'public'::text)
 RETURNS TABLE(column_name1 text, column_name2 text, data_type text, is_fk boolean, fk_direction text, confidence numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
    RETURN QUERY
    WITH fk_relations AS (
        -- Прямые FK из таблицы 1 в таблицу 2
        SELECT DISTINCT
            kcu.column_name AS col1,
            ccu.column_name AS col2,
            'table1->table2' AS direction
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = p_schema
          AND tc.table_name = p_table1
          AND ccu.table_schema = p_schema
          AND ccu.table_name = p_table2
        UNION ALL
        -- Обратные FK из таблицы 2 в таблицу 1
        SELECT DISTINCT
            ccu.column_name AS col1,
            kcu.column_name AS col2,
            'table2->table1' AS direction
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = p_schema
          AND tc.table_name = p_table2
          AND ccu.table_schema = p_schema
          AND ccu.table_name = p_table1
    )
    SELECT DISTINCT
        c1.column_name::TEXT,
        c2.column_name::TEXT,
        c1.data_type::TEXT,
        (fk.col1 IS NOT NULL) AS is_fk,
        fk.direction::TEXT AS fk_direction,
        CASE
            WHEN fk.col1 IS NOT NULL THEN 1.0
            WHEN c1.column_name = c2.column_name THEN 0.8
            WHEN c1.column_name ILIKE '%' || c2.column_name || '%'
                 OR c2.column_name ILIKE '%' || c1.column_name || '%'
            THEN 0.6
            ELSE 0.3
        END AS confidence
    FROM information_schema.columns c1
    JOIN information_schema.columns c2
        ON c1.data_type = c2.data_type
        AND (
            c1.column_name = c2.column_name
            OR c1.column_name ILIKE '%id' AND c2.column_name ILIKE '%id'
            OR c1.column_name ILIKE '%_id' AND c2.column_name ILIKE '%_id'
            OR c1.column_name ILIKE '%uuid' AND c2.column_name ILIKE '%uuid'
        )
    LEFT JOIN fk_relations fk
        ON fk.col1 = c1.column_name AND fk.col2 = c2.column_name
    WHERE c1.table_schema = p_schema
      AND c1.table_name = p_table1
      AND c2.table_schema = p_schema
      AND c2.table_name = p_table2
    ORDER BY confidence DESC;
END;
$function$

-- ======================================================================

-- FUNCTION: public.report_get_possible_joins(p_table_name text, p_schema_name text, p_include_semantic_matches boolean)
CREATE OR REPLACE FUNCTION public.report_get_possible_joins(p_table_name text, p_schema_name text DEFAULT 'public'::text, p_include_semantic_matches boolean DEFAULT true)
 RETURNS TABLE(target_table text, target_schema text, join_type text, source_column text, target_column text, constraint_name text, match_confidence numeric, join_suggestion text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    v_table_oid OID;
BEGIN
    -- Получаем OID целевой таблицы для валидации
    SELECT c.oid INTO v_table_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = p_table_name
      AND n.nspname = p_schema_name
      AND c.relkind = 'r';
    IF v_table_oid IS NULL THEN
        RAISE EXCEPTION 'Таблица %.% не существует или не является таблицей',
            p_schema_name, p_table_name;
    END IF;
    -- 1. Внешние ключи ИЗ текущей таблицы (прямые связи)
    RETURN QUERY
    SELECT DISTINCT
        tgt.relname::TEXT AS target_table,
        tgt_nsp.nspname::TEXT AS target_schema,
        'FOREIGN_KEY'::TEXT AS join_type,
        kcu_src.column_name::TEXT AS source_column,
        kcu_tgt.column_name::TEXT AS target_column,
        tc.constraint_name::TEXT AS constraint_name,
        1.0::NUMERIC AS match_confidence,
        format('%I.%I = %I.%I',
            p_table_name, kcu_src.column_name,
            tgt.relname, kcu_tgt.column_name
        ) AS join_suggestion
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu_src
        ON tc.constraint_name = kcu_src.constraint_name
        AND tc.table_schema = kcu_src.table_schema
    JOIN information_schema.constraint_column_usage kcu_tgt
        ON tc.constraint_name = kcu_tgt.constraint_name
    JOIN pg_class tgt ON tgt.relname = kcu_tgt.table_name
    JOIN pg_namespace tgt_nsp ON tgt_nsp.oid = tgt.relnamespace
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = p_schema_name
      AND tc.table_name = p_table_name
      AND tgt_nsp.nspname = kcu_tgt.table_schema
    UNION ALL
    -- 2. Обратные внешние ключи (другие таблицы ссылаются на текущую)
    SELECT DISTINCT
        src.relname::TEXT AS target_table,
        src_nsp.nspname::TEXT AS target_schema,
        'REVERSE_FK'::TEXT AS join_type,
        kcu_tgt.column_name::TEXT AS source_column,
        kcu_src.column_name::TEXT AS target_column,
        tc.constraint_name::TEXT AS constraint_name,
        0.95::NUMERIC AS match_confidence,
        format('%I.%I = %I.%I',
            p_table_name, kcu_tgt.column_name,
            src.relname, kcu_src.column_name
        ) AS join_suggestion
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu_src
        ON tc.constraint_name = kcu_src.constraint_name
        AND tc.table_schema = kcu_src.table_schema
    JOIN information_schema.constraint_column_usage kcu_tgt
        ON tc.constraint_name = kcu_tgt.constraint_name
    JOIN pg_class src ON src.relname = tc.table_name
    JOIN pg_namespace src_nsp ON src_nsp.oid = src.relnamespace
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND kcu_tgt.table_schema = p_schema_name
      AND kcu_tgt.table_name = p_table_name
      AND src_nsp.nspname = tc.table_schema
      AND NOT (src.relname = p_table_name AND src_nsp.nspname = p_schema_name) -- исключаем рекурсию
    ORDER BY match_confidence DESC, target_table, source_column;
END;
$function$

-- ======================================================================

-- FUNCTION: public.report_get_tables(p_schema text)
CREATE OR REPLACE FUNCTION public.report_get_tables(p_schema text DEFAULT 'public'::text)
 RETURNS TABLE(table_name text, table_comment text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        t.table_name::TEXT,
        obj_description(format('%I.%I', t.table_schema, t.table_name)::regclass)::TEXT
    FROM information_schema.tables t
    WHERE t.table_schema = p_schema
      AND t.table_type = 'BASE TABLE'
    ORDER BY t.table_name;
END;
$function$

-- ======================================================================

-- FUNCTION: public.report_validate_identifier(p_identifier text)
CREATE OR REPLACE FUNCTION public.report_validate_identifier(p_identifier text)
 RETURNS boolean
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
BEGIN
    RETURN p_identifier ~ '^[a-zA-Z_][a-zA-Z0-9_]{0,63}$';
END;
$function$

-- ======================================================================

-- FUNCTION: public.round(val double precision, prec integer)
CREATE OR REPLACE FUNCTION public.round(val double precision, prec integer)
 RETURNS numeric
 LANGUAGE sql
 IMMUTABLE
AS $function$
    SELECT ROUND(val::NUMERIC, prec);
$function$
