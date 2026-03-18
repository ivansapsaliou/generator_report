// ═══════════════════════════════════════════════════════════════════
// ResultsRenderer.js — Results table, detail views, history, snippets
// ═══════════════════════════════════════════════════════════════════

// ── Resizable columns state ──
const COL_WIDTHS = {};

function _measureColWidth(colName, colIdx, sampleRows = 50) {
    const CH = 7.5;
    const HDR_CH = 7;
    const PAD = 22;
    const MIN_W = 48;
    const MAX_W = 280;

    const hdrW = Math.round(colName.length * HDR_CH + PAD + 20);

    const lens = [];
    const sample = S.display.slice(0, sampleRows);
    for (const row of sample) {
        const v = row[colName];
        if (v === null || v === undefined) continue;
        lens.push(String(v).length);
    }

    let dataW = 0;
    if (lens.length > 0) {
        lens.sort((a, b) => a - b);
        const p80idx = Math.floor(lens.length * 0.80);
        const p80 = lens[Math.min(p80idx, lens.length - 1)];
        const effective = Math.min(p80, 40);
        dataW = Math.round(effective * CH + PAD);
    }

    return Math.min(MAX_W, Math.max(MIN_W, Math.max(hdrW, dataW)));
}

function renderTable() {
    const c = document.querySelector(`.results-content[data-tab="${S.currentEditorTab}"]`) || document.getElementById('resultsContent');
    if (!c) return;
    if (!S.cols.length) {
        c.innerHTML = '<div style="padding:14px;font-size:12px;color:var(--text-3);">Нет данных</div>';
        return;
    }

    const ROW_NUM_W = 42;

    const getW = (i) => {
        const key = `${S.currentEditorTab}_${i}`;
        if (COL_WIDTHS[key]) return COL_WIDTHS[key];
        if (i === -1) return ROW_NUM_W;
        return _measureColWidth(S.cols[i], i);
    };

    const colgroup = `<colgroup>
        <col style="width:${ROW_NUM_W}px;min-width:36px;">
        ${S.cols.map((_, i) => `<col style="width:${getW(i)}px;min-width:40px;">`).join('')}
    </colgroup>`;

    const totalW = ROW_NUM_W + S.cols.reduce((sum, _, i) => sum + getW(i), 0);

    const rowNumTh = `<th style="text-align:right;padding-right:8px;cursor:default;vertical-align:middle;" data-col="-1">
        <span style="color:var(--text-3)">#</span>
        <span class="rt-resizer" data-colidx="-1"></span>
    </th>`;
    const ths = rowNumTh + S.cols.map((col, i) => {
        const cl = S.sortCol === i ? (S.sortDir === 'asc' ? 'sa' : 'sd') : '';
        const fv = (S.colFilters[i] || '').replace(/"/g, '&quot;');
        return `<th class="${cl}" data-col="${i}">
            <div class="rt-th-inner">
                <div class="th-hdr-row">
                    <span class="th-label" onclick="sortBy(${i})">${eh(col)}</span>
                    <span class="th-sort"></span>
                </div>
                <input class="th-filter" type="text" placeholder="Фильтр…" data-colidx="${i}" value="${fv}" oninput="applyColFilter(this)">
            </div>
            <span class="rt-resizer" data-colidx="${i}"></span>
        </th>`;
    }).join('');

    c.innerHTML = `
        <div class="rt-wrap"><table class="rt" style="width:${totalW}px;">${colgroup}<thead><tr>${ths}</tr></thead><tbody></tbody></table></div>
        <div class="rt-xscroll" aria-hidden="true"><div class="rt-xscroll-inner" style="width:${totalW}px;"></div></div>
    `;

    const wrap = c.querySelector('.rt-wrap');
    const xbar = c.querySelector('.rt-xscroll');
    if (wrap && xbar) {
        const syncFromWrap = () => { xbar.scrollLeft = wrap.scrollLeft; };
        const syncFromBar = () => { wrap.scrollLeft = xbar.scrollLeft; };
        wrap.addEventListener('scroll', syncFromWrap, { passive: true });
        xbar.addEventListener('scroll', syncFromBar, { passive: true });
        xbar.scrollLeft = wrap.scrollLeft;
    }

    S._resultsContainer = c;

    // Wire column resizers
    const table = c.querySelector('.rt');
    let _resizeDrag = null;
    c.querySelectorAll('.rt-resizer').forEach(el => {
        el.addEventListener('mousedown', e => {
            e.stopPropagation();
            const colIdx = parseInt(el.dataset.colidx);
            const th = el.closest('th');
            _resizeDrag = { colIdx, startX: e.clientX, startW: th ? th.offsetWidth : 80 };
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            el.classList.add('dragging');
        });
    });
    document.addEventListener('mousemove', e => {
        if (!_resizeDrag) return;
        const newW = Math.max(40, _resizeDrag.startW + e.clientX - _resizeDrag.startX);
        const key = `${S.currentEditorTab}_${_resizeDrag.colIdx}`;
        COL_WIDTHS[key] = newW;
        const cols = table?.querySelectorAll('colgroup col');
        if (cols) {
            const idx = _resizeDrag.colIdx === -1 ? 0 : _resizeDrag.colIdx + 1;
            if (cols[idx]) cols[idx].style.width = newW + 'px';
        }
        const newTotal = ROW_NUM_W + S.cols.reduce((sum, _, i) => sum + getW(i), 0);
        if (table) table.style.width = newTotal + 'px';
        const inner = c.querySelector('.rt-xscroll-inner');
        if (inner) inner.style.width = newTotal + 'px';
    });
    document.addEventListener('mouseup', () => {
        if (_resizeDrag) {
            _resizeDrag = null;
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            c.querySelectorAll('.rt-resizer').forEach(el => el.classList.remove('dragging'));
        }
    });

    // Render body rows
    const tbody = c.querySelector('tbody');
    if (!tbody) return;

    const renderRows = (rows) => {
        tbody.innerHTML = rows.map((row, ri) => {
            const rowNum = `<td style="color:var(--text-3);text-align:right;padding-right:8px;">${ri + 1}</td>`;
            const cells = S.cols.map(col => {
                const v = row[col];
                let cls = '';
                if (v === null || v === undefined) cls = 'vn';
                else if (typeof v === 'number') cls = 'vnum';
                else if (typeof v === 'boolean') cls = 'vbool';
                const disp = v === null || v === undefined ? 'NULL' : String(v);
                return `<td class="${cls}" title="${eh(disp)}">${eh(disp.length > 200 ? disp.substring(0, 200) + '…' : disp)}</td>`;
            }).join('');
            return `<tr>${rowNum}${cells}</tr>`;
        }).join('');

        // Context menu on results
        tbody.addEventListener('contextmenu', onResultCtxMenu);
    };

    renderRows(S.display);
}

function applyFilter() {
    const fi = document.getElementById('resultFilter');
    const q = fi ? fi.value.toLowerCase() : '';
    if (!q) {
        S.display = [...S.master];
    } else {
        S.display = S.master.filter(row =>
            S.cols.some(c => String(row[c] ?? '').toLowerCase().includes(q))
        );
    }
    const cnt = document.getElementById('filtCnt');
    if (cnt) cnt.textContent = q ? `${S.display.length} / ${S.master.length}` : '';
    renderTable();
}

function applyColFilter(input) {
    const colIdx = parseInt(input.dataset.colidx);
    const val = input.value.toLowerCase();
    S.colFilters[colIdx] = val;
    S.display = S.master.filter(row => {
        return Object.entries(S.colFilters).every(([idx, filter]) => {
            if (!filter) return true;
            const col = S.cols[parseInt(idx)];
            return String(row[col] ?? '').toLowerCase().includes(filter);
        });
    });
    const tbody = document.querySelector(`.results-content[data-tab="${S.currentEditorTab}"] tbody`);
    if (!tbody) { renderTable(); return; }
    const c = document.querySelector(`.results-content[data-tab="${S.currentEditorTab}"]`);
    if (c) {
        const rows = c.querySelectorAll('tbody tr');
        // Re-render just the body
        renderTable();
    }
}

function sortBy(colIdx) {
    if (S.sortCol === colIdx) {
        S.sortDir = S.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
        S.sortCol = colIdx;
        S.sortDir = 'asc';
    }
    const col = S.cols[colIdx];
    S.display.sort((a, b) => {
        const va = a[col], vb = b[col];
        if (va === null || va === undefined) return 1;
        if (vb === null || vb === undefined) return -1;
        const cmp = typeof va === 'number' && typeof vb === 'number'
            ? va - vb
            : String(va).localeCompare(String(vb), undefined, { numeric: true });
        return S.sortDir === 'asc' ? cmp : -cmp;
    });
    renderTable();
}

function setSt(t, text, time, rows, tabId = null) {
    const tid = tabId || S.currentEditorTab;
    const scope = document.querySelector(`.tab-content[data-tab-id="${tid}"]`) || document;
    const e = scope.querySelector('.stText');
    if (!e) return;
    e.className = 'stText ' + (t === 'ok' ? 'st-ok' : t === 'err' ? 'st-err' : 'st-run');
    e.textContent = text;
    const stTime = scope.querySelector('.stTime');
    const stRows = scope.querySelector('.stRows');
    if (stTime) stTime.textContent = time;
    if (stRows) stRows.textContent = rows;
}

function switchTab(tab) {
    const scope = document.querySelector(`.tab-content[data-tab-id="${S.currentEditorTab}"]`) || document;
    scope.querySelectorAll('.rtab').forEach((b, idx) => {
        const isData = (tab === 'data' && idx === 0);
        const isHist = (tab === 'history' && idx === 1);
        b.classList.toggle('active', isData || isHist);
    });
    const c = scope.querySelector(`.results-content[data-tab="${S.currentEditorTab}"]`) || scope.querySelector('.results-content') || document.getElementById('resultsContent');
    if (!c) return;
    if (tab === 'history') {
        renderHist();
    } else {
        if (S.cols.length) {
            renderTable();
        } else {
            c.innerHTML = '<div class="empty-hint"><i class="bi bi-terminal"></i><span>Выполните запрос для просмотра результатов</span></div>';
        }
    }
}

function openHistoryTab() {
    switchTab('history');
}

function renderHist() {
    const c = document.querySelector(`.results-content[data-tab="${S.currentEditorTab}"]`) || document.getElementById('resultsContent');
    if (!c) return;
    if (!S.history.length) {
        c.innerHTML = '<div style="padding:16px;font-size:12px;color:var(--text-3);">История пуста</div>';
        return;
    }
    c.innerHTML = '<div class="hi-list">' + S.history.map((h, i) => `
        <div class="hi-it" onclick="loadHist(${i})">
            <div class="hi-meta">${eh(h.ts)} · ${eh(h.elapsed)}s · <span class="${h.ok ? 'hi-ok' : 'hi-err'}">${h.ok ? '✓' : '✗'}</span> ${h.rows} строк${h.conn ? ' · <span style="color:var(--text-3)">'+eh(h.conn)+'</span>' : ''}</div>
            ${eh(h.sqlPreview || h.sql)}
        </div>`).join('') + '</div>';
}

function loadHist(i) {
    const h = S.history[i];
    if (!h) return;
    const sql = h.sql || h.sqlPreview || '';
    const editor = monacoEditors[S.currentEditorTab];
    if (editor) {
        editor.setValue(sql);
        editor.focus();
    } else {
        const ta = getActiveTA();
        if (ta) { ta.value = sql; syncNums(ta); updateHighlight(ta); }
    }
    switchTab('data');
}

// ═══ RENDER DETAIL VIEWS ═══
function renderCols(body, data) {
    if (!data.length) { body.innerHTML = '<div style="padding:16px;font-size:12px;color:var(--text-3);">Нет колонок</div>'; return; }
    body.innerHTML = `<table class="dt"><thead><tr><th>#</th><th>Имя колонки</th><th>Тип данных</th><th>Nullable</th><th>По умолчанию</th><th>Ключи</th><th>Описание</th></tr></thead><tbody>` +
    data.map((c, i) => {
        const keys = [];
        if (c.is_pk) keys.push('<span class="tag tpk">PK</span>');
        if (c.is_fk) keys.push('<span class="tag tfk">FK</span>');
        if (c.is_unique) keys.push('<span class="tag tuq">UQ</span>');
        return `<tr>
            <td style="color:var(--text-3);font-size:11px;">${i + 1}</td>
            <td style="font-weight:600;">${eh(c.column_name)}</td>
            <td style="color:var(--amber);font-size:11px;">${eh(c.data_type)}</td>
            <td>${c.is_nullable ? '<span class="tag tnull">NULL</span>' : '<span class="tag tnn">NOT NULL</span>'}</td>
            <td style="color:var(--text-3);font-size:11px;">${eh(c.column_default || '—')}</td>
            <td>${keys.join(' ')}</td>
            <td style="color:var(--text-2);font-size:11px;">${eh(c.column_comment || '—')}</td></tr>`;
    }).join('') + `</tbody></table>`;
}

function renderIdxs(body, data) {
    if (!data.length) { body.innerHTML = '<div style="padding:16px;font-size:12px;color:var(--text-3);">Нет индексов</div>'; return; }
    body.innerHTML = `<table class="dt"><thead><tr><th>Имя</th><th>Тип</th><th>Уникальный</th><th>Определение</th></tr></thead><tbody>` +
    data.map(x => `<tr>
        <td style="font-weight:600;">${eh(x.indexname)}</td>
        <td style="color:var(--text-3);font-size:11px;">${eh(x.index_type || 'btree')}</td>
        <td>${x.is_unique ? '<span class="tag tuq">ДА</span>' : '<span class="tag tnull">НЕТ</span>'}</td>
        <td style="font-size:11px;color:var(--text-2);white-space:normal;">${eh(x.indexdef || '')}</td></tr>`).join('') + `</tbody></table>`;
}

function renderCons(body, data) {
    if (!data.length) { body.innerHTML = '<div style="padding:16px;font-size:12px;color:var(--text-3);">Нет ограничений</div>'; return; }
    const tm = { p: 'PK', f: 'FK', u: 'UNIQUE', c: 'CHECK', x: 'EXCLUDE' };
    const tg = { 'PK': 'tpk', 'FK': 'tfk', 'UNIQUE': 'tuq', 'CHECK': 'tnn' };
    body.innerHTML = `<table class="dt"><thead><tr><th>Имя</th><th>Тип</th><th>Колонки</th><th>Ссылается на</th></tr></thead><tbody>` +
    data.map(c => {
        const tp = tm[c.constraint_type] || c.constraint_type || '?';
        const g = tg[tp] || 'tnull';
        return `<tr>
            <td style="font-weight:600;">${eh(c.constraint_name)}</td>
            <td><span class="tag ${g}">${tp}</span></td>
            <td style="font-size:11px;">${eh(c.column_name || '—')}</td>
            <td style="font-size:11px;color:var(--text-3);">${eh(c.foreign_table ? c.foreign_table + '.' + c.foreign_column : '—')}</td></tr>`;
    }).join('') + `</tbody></table>`;
}

function renderDDL(body, data) {
    const ddl = data.ddl || '-- DDL недоступен';
    const containerId = 'ddl-ed-' + Date.now();
    body.innerHTML = `<div class="ddl-wrap">
        <button class="btn btn-secondary btn-sm" style="margin-bottom:8px;"
            onclick="navigator.clipboard.writeText(document.getElementById('${containerId}')?.__ddlText||'')">
            <i class="bi bi-clipboard"></i> Копировать
        </button>
        <div id="${containerId}" class="ddl-monaco-container"></div>
    </div>`;
    const containerEl = document.getElementById(containerId);
    if (containerEl) {
        containerEl.__ddlText = ddl;
        if (_monacoApiReady) {
            monaco.editor.create(containerEl, {
                value: ddl,
                language: 'sql',
                theme: _getCurrentMonacoTheme(),
                readOnly: true,
                fontSize: 13,
                lineHeight: 20,
                minimap: { enabled: false },
                automaticLayout: true,
                scrollBeyondLastLine: false,
                renderWhitespace: 'none',
                fontFamily: "'Geist Mono', 'Consolas', 'Courier New', monospace",
                scrollbar: { verticalScrollbarSize: 5, horizontalScrollbarSize: 5 },
                padding: { top: 8, bottom: 8 },
                domReadOnly: true,
            });
        } else {
            const hlHtml = (typeof highlightSQL === 'function') ? highlightSQL(ddl) : eh(ddl);
            containerEl.outerHTML = `<pre class="ddl-code ddl-hl" id="${containerId}">${hlHtml}</pre>`;
        }
    }
}

function renderParams(body, rows) {
    if (!rows || !rows.length) {
        body.innerHTML = '<div style="padding:16px;color:var(--text-3);font-size:13px;">Параметры не найдены</div>';
        return;
    }
    const inParams  = rows.filter(r => r.parameter_mode === 'IN' || r.parameter_mode === 'INOUT' || !r.parameter_mode);
    const outParams = rows.filter(r => r.parameter_mode === 'OUT' || r.parameter_mode === 'INOUT');
    let html = '<div style="padding:10px;">';
    if (inParams.length) {
        html += '<div style="font-size:11px;font-weight:700;color:var(--text-3);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">Входные параметры</div>';
        html += '<table class="dt" style="width:100%;margin-bottom:12px;"><thead><tr><th>Имя</th><th>Тип</th><th>Режим</th><th>По умолчанию</th></tr></thead><tbody>';
        inParams.forEach(p => {
            html += `<tr><td><code>${eh(p.parameter_name||'—')}</code></td><td><span style="color:var(--amber);font-family:var(--font-mono);font-size:11px;">${eh(p.data_type||'—')}</span></td><td>${eh(p.parameter_mode||'IN')}</td><td>${p.parameter_default ? eh(p.parameter_default) : '<span style="color:var(--text-3)">—</span>'}</td></tr>`;
        });
        html += '</tbody></table>';
    }
    if (outParams.length) {
        html += '<div style="font-size:11px;font-weight:700;color:var(--text-3);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">Выходные параметры</div>';
        html += '<table class="dt" style="width:100%;"><thead><tr><th>Имя</th><th>Тип</th><th>Режим</th></tr></thead><tbody>';
        outParams.forEach(p => {
            html += `<tr><td><code>${eh(p.parameter_name||'—')}</code></td><td><span style="color:var(--amber);font-family:var(--font-mono);font-size:11px;">${eh(p.data_type||'—')}</span></td><td>${eh(p.parameter_mode)}</td></tr>`;
        });
        html += '</tbody></table>';
    }
    html += '</div>';
    body.innerHTML = html;
}

function createDetailTabContent(tabId, tableName) {
    const div = document.createElement('div');
    div.className = 'tab-content';
    div.dataset.tabId = tabId;
    div.innerHTML = `
        <div class="detail-tabs">
            <button class="detail-tab active" data-subtab="columns" onclick="switchDetailSubTab('${tabId}', 'columns')">Колонки</button>
            <button class="detail-tab" data-subtab="indexes" onclick="switchDetailSubTab('${tabId}', 'indexes')">Индексы</button>
            <button class="detail-tab" data-subtab="constraints" onclick="switchDetailSubTab('${tabId}', 'constraints')">Ограничения</button>
            <button class="detail-tab" data-subtab="ddl" onclick="switchDetailSubTab('${tabId}', 'ddl')">DDL</button>
        </div>
        <div class="detail-body" data-tab="${tabId}"><div class="tree-loading"><span class="loading"></span> Загрузка...</div></div>
    `;
    return div;
}

function createRoutineDetailTabContent(tabId, name, routineType) {
    const div = document.createElement('div');
    div.className = 'tab-content';
    div.dataset.tabId = tabId;
    div.innerHTML = `
        <div class="detail-tabs">
            <button class="detail-tab active" data-subtab="ddl" onclick="switchRoutineSubTab('${tabId}', 'ddl')">DDL</button>
            <button class="detail-tab" data-subtab="params" onclick="switchRoutineSubTab('${tabId}', 'params')">Параметры</button>
        </div>
        <div class="detail-body" data-tab="${tabId}"><div class="tree-loading"><span class="loading"></span> Загрузка...</div></div>
    `;
    return div;
}

async function switchDetailSubTab(tabId, subtab) {
    const content = document.querySelector(`.tab-content[data-tab-id="${tabId}"]`);
    content.querySelectorAll('.detail-tab').forEach(t => t.classList.toggle('active', t.dataset.subtab === subtab));
    S.tabs[tabId].activeSubTab = subtab;
    await loadDetailTabData(tabId, subtab, S.tabs[tabId].tableName);
}

async function loadDetailTabData(tabId, subtab, tableName) {
    const body = document.querySelector(`.detail-body[data-tab="${tabId}"]`);
    body.innerHTML = '<div class="tree-loading"><span class="loading"></span> Загрузка...</div>';
    try {
        const r = await fetch(`/api/sql/table-detail?table=${encodeURIComponent(tableName)}&tab=${subtab}`);
        const d = await r.json();
        if (!d.success) { body.innerHTML = `<pre class="err-blk">${eh(d.error)}</pre>`; return; }
        if (subtab === 'columns') renderCols(body, d.data);
        else if (subtab === 'indexes') renderIdxs(body, d.data);
        else if (subtab === 'constraints') renderCons(body, d.data);
        else if (subtab === 'ddl') renderDDL(body, d.data);
    } catch (e) {
        body.innerHTML = `<pre class="err-blk">${eh(e.message)}</pre>`;
    }
}

async function switchRoutineSubTab(tabId, subtab) {
    const content = document.querySelector(`.tab-content[data-tab-id="${tabId}"]`);
    content.querySelectorAll('.detail-tab').forEach(t => t.classList.toggle('active', t.dataset.subtab === subtab));
    S.tabs[tabId].activeSubTab = subtab;
    await loadRoutineDetailData(tabId, subtab, S.tabs[tabId].routineName, S.tabs[tabId].routineType);
}

async function loadRoutineDetailData(tabId, subtab, name, routineType) {
    const body = document.querySelector(`.detail-body[data-tab="${tabId}"]`);
    body.innerHTML = '<div class="tree-loading"><span class="loading"></span> Загрузка...</div>';
    try {
        const r = await fetch(`/api/sql/routine-detail?name=${encodeURIComponent(name)}&type=${encodeURIComponent(routineType)}&tab=${subtab}`);
        const d = await r.json();
        if (!d.success) { body.innerHTML = `<pre class="err-blk">${eh(d.error)}</pre>`; return; }
        if (subtab === 'ddl') renderDDL(body, d.data);
        else if (subtab === 'params') renderParams(body, d.data);
    } catch (e) {
        body.innerHTML = `<pre class="err-blk">${eh(e.message)}</pre>`;
    }
}

// ── Context menu on results table ──
let _resultCtxMenu = null;
function onResultCtxMenu(e) {
    const td = e.target.closest('td');
    const tr = e.target.closest('tr');
    if (!td || !tr) return;
    e.preventDefault();
    const cellVal = td.title || td.textContent;
    const cells = [...tr.querySelectorAll('td')].slice(1);
    const rowObj = {};
    S.cols.forEach((col, i) => { rowObj[col] = cells[i]?.title || cells[i]?.textContent || ''; });

    if (!_resultCtxMenu) {
        _resultCtxMenu = document.createElement('div');
        _resultCtxMenu.id = 'resultCtxMenu';
        _resultCtxMenu.style.cssText = 'display:none;position:fixed;z-index:9997;background:var(--bg-2);border:1px solid var(--line-2);border-radius:4px;box-shadow:var(--shadow-md);min-width:200px;padding:4px 0;font-family:var(--font-ui);font-size:12px;';
        document.body.appendChild(_resultCtxMenu);
        document.addEventListener('click', () => { if (_resultCtxMenu) _resultCtxMenu.style.display = 'none'; });
    }

    _resultCtxMenu.innerHTML = `
        <div class="ctx-item" data-act="cell"><i class="bi bi-clipboard"></i>Копировать ячейку</div>
        <div class="ctx-item" data-act="row_tsv"><i class="bi bi-layout-three-columns"></i>Копировать строку (TSV)</div>
        <div class="ctx-item" data-act="row_json"><i class="bi bi-braces"></i>Копировать строку (JSON)</div>
        <div class="ctx-sep"></div>
        <div class="ctx-item" data-act="col_filter"><i class="bi bi-funnel"></i>Фильтр по этому значению</div>
    `;
    _resultCtxMenu.querySelectorAll('.ctx-item[data-act]').forEach(item => {
        item.addEventListener('click', () => {
            const act = item.dataset.act;
            _resultCtxMenu.style.display = 'none';
            if (act === 'cell') {
                navigator.clipboard.writeText(cellVal === 'NULL' ? '' : cellVal).then(() => showNotice('Скопировано'));
            } else if (act === 'row_tsv') {
                const tsv = S.cols.map(col => rowObj[col]).join('\t');
                navigator.clipboard.writeText(tsv).then(() => showNotice('Строка скопирована (TSV)'));
            } else if (act === 'row_json') {
                navigator.clipboard.writeText(JSON.stringify(rowObj, null, 2)).then(() => showNotice('Строка скопирована (JSON)'));
            } else if (act === 'col_filter') {
                const fi = document.getElementById('resultFilter');
                if (fi) { fi.value = cellVal === 'NULL' ? '' : cellVal; applyFilter(); }
            }
        });
    });

    _resultCtxMenu.style.display = 'block';
    _resultCtxMenu.style.left = Math.min(e.clientX, window.innerWidth - 220) + 'px';
    _resultCtxMenu.style.top = Math.min(e.clientY, window.innerHeight - 160) + 'px';
}

// ═══ USER SNIPPETS ═══
function getUserSnippets() {
    try { return JSON.parse(localStorage.getItem('sqlUserSnippets') || '[]'); } catch(e) { return []; }
}

function saveUserSnippetsList(snips) {
    localStorage.setItem('sqlUserSnippets', JSON.stringify(snips));
}

function saveCurrentAsSnippet() {
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    const val = editor ? editor.getValue() : (getActiveTA()?.value || '');
    if (!val.trim()) { showNotice('Редактор пуст', 'err'); return; }
    const name = prompt('Название сниппета:', '');
    if (name == null) return;
    const trimName = name.trim() || ('Сниппет ' + new Date().toLocaleTimeString('ru-RU'));
    const snips = getUserSnippets();
    snips.unshift({ id: Date.now(), name: trimName, sql: val.trim(), ts: new Date().toLocaleDateString('ru-RU') });
    saveUserSnippetsList(snips);
    showNotice('Сниппет «' + trimName + '» сохранён');
}

function showSnippetsModal() {
    const m = document.getElementById('snippetsModal');
    if (!m) return;
    renderSnippetsList();
    m.classList.add('show');
}

function hideSnippetsModal() {
    document.getElementById('snippetsModal')?.classList.remove('show');
}

function renderSnippetsList() {
    const body = document.getElementById('snippetsBody');
    if (!body) return;
    const snips = getUserSnippets();
    if (!snips.length) {
        body.innerHTML = '<div class="snip-empty"><i class="bi bi-collection" style="font-size:24px;display:block;margin-bottom:8px;"></i>Нет сохранённых сниппетов.<br>Нажмите «Сохранить текущий» чтобы добавить.</div>';
        return;
    }
    body.innerHTML = snips.map((s, i) => `
        <div class="snip-item">
            <div class="snip-title">${eh(s.name)}</div>
            <div class="snip-prev">${eh((s.sql||'').substring(0,120))}</div>
            <div class="snip-actions">
                <button class="btn btn-primary btn-sm" onclick="loadSnippet(${i})"><i class="bi bi-play-fill"></i> Загрузить</button>
                <button class="btn btn-secondary btn-sm" onclick="deleteSnippet(${i})" title="Удалить"><i class="bi bi-trash"></i></button>
                <span class="snip-ts">${eh(s.ts || '')}</span>
            </div>
        </div>`).join('');
}

function loadSnippet(i) {
    const snips = getUserSnippets();
    const s = snips[i];
    if (!s) return;
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    if (editor) {
        editor.setValue(s.sql);
        editor.focus();
    } else {
        const ta = getActiveTA();
        if (!ta) return;
        ta.value = s.sql;
        syncNums(ta);
        updateHighlight(ta);
        ta.focus();
    }
    hideSnippetsModal();
}

function deleteSnippet(i) {
    if (!confirm('Удалить сниппет?')) return;
    const snips = getUserSnippets();
    snips.splice(i, 1);
    saveUserSnippetsList(snips);
    renderSnippetsList();
}
