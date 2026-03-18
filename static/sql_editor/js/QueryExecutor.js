// ═══════════════════════════════════════════════════════════════════
// QueryExecutor.js — SQL execution, explain, format, export, context menu
// ═══════════════════════════════════════════════════════════════════

// ═══ STATEMENT PARSER ═══
function splitStatements(sql) {
    const stmts = [];
    let start = 0;
    let i = 0;
    const len = sql.length;
    while (i < len) {
        if (sql[i] === '-' && i+1 < len && sql[i+1] === '-') {
            while (i < len && sql[i] !== '\n') i++;
            continue;
        }
        if (sql[i] === '/' && i+1 < len && sql[i+1] === '*') {
            i += 2;
            while (i < len && !(sql[i] === '*' && i+1 < len && sql[i+1] === '/')) i++;
            i += 2;
            continue;
        }
        if (sql[i] === '$' && i+1 < len && sql[i+1] === '$') {
            i += 2;
            while (i < len) {
                if (sql[i] === '$' && i+1 < len && sql[i+1] === '$') { i += 2; break; }
                i++;
            }
            continue;
        }
        if (sql[i] === "'") {
            i++;
            while (i < len) {
                if (sql[i] === "'" && i+1 < len && sql[i+1] === "'") { i += 2; continue; }
                if (sql[i] === "'") { i++; break; }
                i++;
            }
            continue;
        }
        if (sql[i] === ';') {
            const stmt = sql.slice(start, i + 1).trim();
            if (stmt && stmt !== ';') stmts.push({ sql: stmt, start, end: i });
            start = i + 1;
            i++;
            continue;
        }
        i++;
    }
    const last = sql.slice(start).trim();
    if (last) stmts.push({ sql: last, start, end: len - 1 });
    return stmts;
}

function getStatementAtCursor(ta) {
    const pos = ta.selectionStart;
    const sql = ta.value;
    const stmts = splitStatements(sql);
    for (const s of stmts) {
        if (pos >= s.start && pos <= s.end + 1) return s.sql;
    }
    return sql.trim();
}

function getStatementAtCursorMonaco(editor) {
    const model = editor.getModel();
    const pos = editor.getPosition();
    const offset = model.getOffsetAt(pos);
    const sql = model.getValue();
    const stmts = splitStatements(sql);
    for (const s of stmts) {
        if (offset >= s.start && offset <= s.end + 1) return s.sql;
    }
    return sql.trim();
}

// ═══ RUN QUERY ═══
async function runQuery() {
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    let sql;
    if (editor) {
        const sel = editor.getModel().getValueInRange(editor.getSelection());
        sql = (sel && sel.trim()) ? sel.trim() : editor.getValue().trim();
    } else {
        const ta = document.querySelector(`.sql-input[data-tab="${tabId}"]`) || document.getElementById('sqlInput');
        if (!ta) return;
        const sel = ta.value.substring(ta.selectionStart, ta.selectionEnd).trim();
        sql = sel || ta.value.trim();
    }
    if (!sql) return;
    await executeSQL(sql, tabId);
}

async function runStatement() {
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    let sql;
    if (editor) {
        const sel = editor.getModel().getValueInRange(editor.getSelection());
        sql = (sel && sel.trim()) ? sel.trim() : getStatementAtCursorMonaco(editor);
    } else {
        const ta = document.querySelector(`.sql-input[data-tab="${tabId}"]`) || document.getElementById('sqlInput');
        if (!ta) return;
        const sel = ta.value.substring(ta.selectionStart, ta.selectionEnd).trim();
        sql = sel || getStatementAtCursor(ta);
    }
    if (!sql) return;
    await executeSQL(sql, tabId);
}

async function executeSQL(sql, tabId) {
    if (!tabId) tabId = S.currentEditorTab;

    const warnings = lintQuery(sql);
    showLintBar(warnings, tabId);

    const limit = parseInt(document.querySelector(`.lim-sel[data-tab="${tabId}"]`)?.value || document.getElementById('rowLimit')?.value || 100);
    const btn = document.querySelector(`.run-tab-btn[data-tab="${tabId}"]`);
    if (btn) { btn.innerHTML = '<span class="loading"></span>'; btn.disabled = true; }
    setSt('run', '⟳ Выполняется...', '', '');
    const _origTitle = document.title;
    document.title = '⟳ Выполняется... — SQL Редактор';

    const scope = document.querySelector(`.tab-content[data-tab-id="${tabId}"]`) || document;
    const rfbar = scope.querySelector('.rfbar') || document.getElementById('rfbar');
    const exportStrip = scope.querySelector('.export-strip') || document.getElementById('exportStrip');
    if (rfbar) rfbar.style.display = 'none';
    if (exportStrip) exportStrip.style.display = 'none';

    const resultsEl = document.querySelector(`.results-content[data-tab="${tabId}"]`) || document.getElementById('resultsContent');
    if (resultsEl) resultsEl.innerHTML = '<div class="empty-hint"><span class="loading"></span></div>';

    const t0 = performance.now();

    try {
        const resp = await fetchWithRetry('/api/sql/execute', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({sql, limit})
        });

        const data = await resp.json();
        const el = ((performance.now() - t0) / 1000).toFixed(3);

        S.history.unshift({
            sql: sql,
            sqlPreview: sql.substring(0, 200),
            ts: new Date().toLocaleTimeString('ru-RU'),
            elapsed: el,
            ok: data.success,
            rows: data.data?.rows?.length ?? data.rows_affected ?? 0,
            conn: S.connInfo
        });
        if (S.history.length > MAX_HISTORY_ITEMS) S.history.pop();
        localStorage.setItem('sqlhist3', JSON.stringify(S.history));

        if (data.success) {
            if (data.data && data.data.columns && data.data.rows) {
                S.master = data.data.rows;
                S.display = [...S.master];
                S.cols = data.data.columns;
                S.sortCol = null;
                S.sortDir = 'asc';
                S.colFilters = {};

                if (rfbar) rfbar.style.display = 'none';
                if (exportStrip) exportStrip.style.display = 'flex';
                setSt('ok', '✓ Выполнено', el + 's', S.master.length + ' строк', tabId);
                document.title = `✓ ${S.master.length} строк — SQL Редактор`;
                setTimeout(() => { document.title = _origTitle; }, 4000);

                renderTable();
            } else {
                S.master = [];
                S.display = [];
                S.cols = [];
                const affected = data.rows_affected ?? 0;
                setSt('ok', '✓ Выполнено', el + 's', 'затронуто: ' + affected, tabId);
                document.title = `✓ OK — SQL Редактор`;
                setTimeout(() => { document.title = _origTitle; }, 4000);
                if (resultsEl) resultsEl.innerHTML = `
                    <div class="msg-ok">
                        <div class="mi"><i class="bi bi-check-circle-fill"></i></div>
                        <div>
                            <div class="mt2">Запрос выполнен успешно</div>
                            Затронуто строк: <strong>${affected}</strong>
                            <div class="md">${eh(sql.substring(0, 120))}${sql.length > 120 ? '…' : ''}</div>
                        </div>
                    </div>`;
            }
        } else {
            S.master = [];
            S.display = [];
            S.cols = [];
            setSt('err', '✗ Ошибка', el + 's', '', tabId);
            document.title = `✗ Ошибка — SQL Редактор`;
            setTimeout(() => { document.title = _origTitle; }, 4000);
            if (resultsEl) resultsEl.innerHTML = `<pre class="err-blk">${eh(data.error || 'Неизвестная ошибка')}</pre>`;
        }
    } catch (e) {
        console.error('[runQuery]', e);
        setSt('err', '✗ Сетевая ошибка', '', '', tabId);
        if (resultsEl) resultsEl.innerHTML = `<pre class="err-blk">${eh(e.message)}</pre>`;
    } finally {
        if (btn) { btn.innerHTML = '<i class="bi bi-play-fill"></i>'; btn.disabled = false; }
    }
}

// ═══ EXPLAIN / EXPLAIN ANALYZE ═══
function explainQuery(analyze) {
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    let sql;
    if (editor) {
        const sel = editor.getModel().getValueInRange(editor.getSelection());
        sql = (sel && sel.trim()) ? sel.trim() : editor.getValue().trim();
    } else {
        const ta = getActiveTA();
        if (!ta) return;
        const sel = ta.value.substring(ta.selectionStart, ta.selectionEnd).trim();
        sql = sel || ta.value.trim();
    }
    if (!sql) return;
    sql = sql.replace(/;\s*$/, '');
    const prefix = analyze ? 'EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)' : 'EXPLAIN';
    executeSQL(prefix + '\n' + sql, tabId);
}

// ═══ QUERY LINTER ═══
function lintQuery(sql) {
    const warnings = [];
    const clean = sql
        .replace(/--[^\n]*/g, '')
        .replace(/\/\*[\s\S]*?\*\//g, '')
        .toUpperCase();

    if (/\bUPDATE\b/.test(clean) && !/\bWHERE\b/.test(clean)) {
        warnings.push({ level: 'danger', msg: 'UPDATE без WHERE: изменятся все строки!' });
    }
    if (/\bDELETE\b/.test(clean) && !/\bWHERE\b/.test(clean)) {
        warnings.push({ level: 'danger', msg: 'DELETE без WHERE: удалятся все строки!' });
    }
    if (/\bTRUNCATE\b/.test(clean)) {
        warnings.push({ level: 'danger', msg: 'TRUNCATE: необратимо удаляет все данные таблицы!' });
    }
    if (/\bDROP\s+(TABLE|DATABASE|SCHEMA|INDEX|VIEW)\b/.test(clean)) {
        warnings.push({ level: 'danger', msg: 'DROP: необратимая операция!' });
    }
    return warnings;
}

function showLintBar(warnings, tabId) {
    const bar = document.getElementById('lintBar-' + tabId) || document.getElementById('lintBar');
    if (!bar) return;
    if (!warnings.length) { bar.classList.remove('show'); bar.innerHTML = ''; return; }
    bar.innerHTML = warnings.map(w =>
        `<span class="lint-warn ${w.level === 'info' ? 'info' : ''}">${w.level === 'danger' ? '⚠ ' : 'ℹ '}${eh(w.msg)}</span>`
    ).join('') + `<button class="lint-dismiss" onclick="this.parentElement.classList.remove('show')" title="Закрыть">×</button>`;
    bar.classList.add('show');
}

// ═══ FORMAT SQL ═══
function formatSQL() {
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    const _fmt = sql => {
        ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN', 'ORDER BY', 'GROUP BY', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'UNION ALL', 'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE FROM', 'RETURNING'].forEach(kw => {
            sql = sql.replace(new RegExp(`\\b${kw}\\b`, 'gi'), '\n' + kw.toUpperCase());
        });
        return sql.replace(/\n{3,}/g, '\n\n').trim();
    };
    if (editor) {
        editor.setValue(_fmt(editor.getValue()));
        editor.focus();
        return;
    }
    const ta = getActiveTA();
    if (!ta) return;
    ta.value = _fmt(ta.value);
    syncNums(ta);
    updateHighlight(ta);
}

// ═══ EXPORT RESULTS ═══
function exportResult(fmt) {
    if (!S.cols.length) return;
    const rows = S.display, cols = S.cols;

    if (fmt === 'json') {
        dl(new Blob([JSON.stringify(rows, null, 2)], {type: 'application/json'}), 'result.json');
    } else if (fmt === 'csv') {
        let csv = cols.map(c => `"${c}"`).join(',') + '\n';
        rows.forEach(r => {
            csv += cols.map(c => {
                const v = r[c];
                if (v == null) return '';
                return `"${String(v).replace(/"/g, '""')}"`;
            }).join(',') + '\n';
        });
        dl(new Blob(['\uFEFF' + csv], {type: 'text/csv'}), 'result.csv');
    } else if (fmt === 'xlsx') {
        if (typeof XLSX === 'undefined') {
            alert('Библиотека XLSX не загружена. Экспорт в Excel недоступен.');
            return;
        }
        const ws = XLSX.utils.aoa_to_sheet([cols, ...rows.map(r => cols.map(c => r[c] ?? ''))]);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, 'Результат');
        XLSX.writeFile(wb, 'result.xlsx');
    }
}

function dl(blob, name) {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    a.click();
}

// ═══ INSERT HELPERS ═══
function insertSnippet(key, tabId) {
    const editor = monacoEditors[tabId];
    if (editor) {
        const snip = SNIPS[key] || '';
        editor.executeEdits('insertSnippet', [{ range: editor.getSelection(), text: snip, forceMoveMarkers: true }]);
        editor.focus();
        return;
    }
    const ta = document.querySelector(`.sql-input[data-tab="${tabId}"]`) || document.getElementById('sqlInput');
    if (!ta) return;
    const s = ta.selectionStart;
    ta.value = ta.value.substring(0, s) + SNIPS[key] + ta.value.substring(ta.selectionEnd);
    ta.selectionStart = ta.selectionEnd = s + SNIPS[key].length;
    syncNums(ta);
    updateHighlight(ta);
    ta.focus();
}

function clearEditor(tabId) {
    const editor = monacoEditors[tabId];
    if (editor) { editor.setValue(''); editor.focus(); return; }
    const ta = document.querySelector(`.sql-input[data-tab="${tabId}"]`) || document.getElementById('sqlInput');
    if (!ta) return;
    ta.value = '';
    syncNums(ta);
    updateHighlight(ta);
}

function insertText(name) {
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    if (editor) {
        editor.executeEdits('insertText', [{ range: editor.getSelection(), text: name, forceMoveMarkers: true }]);
        editor.focus();
        return;
    }
    const ta = getActiveTA();
    if (!ta) return;
    const p = ta.selectionStart;
    ta.value = ta.value.substring(0, p) + name + ta.value.substring(p);
    ta.selectionStart = ta.selectionEnd = p + name.length;
    syncNums(ta);
    updateHighlight(ta);
    ta.focus();
}

// ═══ CONTEXT MENU FOR TREE ═══
function showContextMenu(x, y, tableName) {
    const menu = document.getElementById('ctxMenu');
    if (!menu) return;
    menu.innerHTML = `
        <div class="ctx-item" data-action="select100"><i class="bi bi-table"></i>SELECT 100 строк</div>
        <div class="ctx-item" data-action="count"><i class="bi bi-hash"></i>COUNT(*)</div>
        <div class="ctx-item" data-action="insert_name"><i class="bi bi-cursor-text"></i>Вставить имя</div>
        <div class="ctx-sep"></div>
        <div class="ctx-item" data-action="detail"><i class="bi bi-info-circle"></i>Свойства таблицы</div>
    `;
    menu.dataset.table = tableName;
    menu.querySelectorAll('.ctx-item[data-action]').forEach(item => {
        item.addEventListener('click', function() {
            const action = this.dataset.action;
            const tbl = menu.dataset.table;
            hideContextMenu();
            if (action === 'detail') openDetail(tbl);
            else execCtxAction(action, tbl);
        });
    });
    menu.style.display = 'block';
    menu.style.left = Math.min(x, window.innerWidth - 210) + 'px';
    menu.style.top = Math.min(y, window.innerHeight - 160) + 'px';
}

function hideContextMenu() {
    const m = document.getElementById('ctxMenu');
    if (m) m.style.display = 'none';
}

document.addEventListener('click', function(e) {
    if (!e.target.closest('#ctxMenu')) hideContextMenu();
});

function execCtxAction(action, tableName) {
    hideContextMenu();
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    if (action === 'select100') {
        const sql = `SELECT *\nFROM ${tableName}\nLIMIT 100;`;
        if (editor) { editor.setValue(sql); editor.focus(); }
        else { const ta = getActiveTA(); if (ta) { ta.value = sql; syncNums(ta); updateHighlight(ta); ta.focus(); } }
        executeSQL(sql, tabId);
    } else if (action === 'count') {
        const sql = `SELECT COUNT(*) AS cnt\nFROM ${tableName};`;
        if (editor) { editor.setValue(sql); editor.focus(); }
        else { const ta = getActiveTA(); if (ta) { ta.value = sql; syncNums(ta); updateHighlight(ta); ta.focus(); } }
        executeSQL(sql, tabId);
    } else if (action === 'insert_name') {
        insertText(tableName);
    }
}
