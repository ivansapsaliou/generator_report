// ═══════════════════════════════════════════════════════════════════
// DatabaseTree.js — Left tree panel: building and managing DB objects tree
// ═══════════════════════════════════════════════════════════════════

// ─── FAVORITES ───────────────────────────────────────────────────
const _FAV_KEY = 'sqleditor_favs';

function _getFavs() {
    try { return new Set(JSON.parse(localStorage.getItem(_FAV_KEY) || '[]')); }
    catch { return new Set(); }
}

function _setFavs(favs) {
    try { localStorage.setItem(_FAV_KEY, JSON.stringify([...favs])); } catch {}
}

function toggleFavorite(tableName) {
    const favs = _getFavs();
    if (favs.has(tableName)) favs.delete(tableName);
    else favs.add(tableName);
    _setFavs(favs);
    const active = favs.has(tableName);
    document.querySelectorAll(`.tn-star[data-table]`).forEach(s => {
        if (s.dataset.table === tableName) {
            s.classList.toggle('active', active);
            s.title = active ? 'Убрать из избранного' : 'В избранное';
            s.querySelector('i').className = active ? 'bi bi-star-fill' : 'bi bi-star';
        }
    });
    _renderFavSection();
}

function _renderFavSection() {
    const favs = _getFavs();
    const body = document.getElementById('treeBody');
    if (!body) return;
    let sec = document.getElementById('treeFavSection');
    if (favs.size === 0) { if (sec) sec.remove(); return; }
    if (!sec) {
        sec = document.createElement('div');
        sec.id = 'treeFavSection';
        sec.className = 'tree-fav-section';
        body.insertBefore(sec, body.firstChild);
    }
    let html = `<div class="tn tn-fav-hdr" data-type="group">
        <span class="arr open">&#9660;</span>
        <span class="ico"><i class="bi bi-star-fill" style="color:var(--amber)"></i></span>
        <span class="lbl">Избранное</span><span class="bdg">${favs.size}</span>
    </div><div class="tc tc-fav open">`;
    [...favs].forEach(tbl => {
        html += `<div class="tn" style="padding-left:28px" data-type="table" data-table="${eh(tbl)}">
            <span class="arr" style="visibility:hidden">&#9654;</span>
            <span class="ico"><i class="bi bi-table ico-table"></i></span>
            <span class="lbl">${eh(tbl)}</span>
            <button class="tn-star active" data-table="${eh(tbl)}" title="Убрать из избранного"><i class="bi bi-star-fill"></i></button>
        </div>`;
    });
    html += `</div>`;
    sec.innerHTML = html;
    const hdr = sec.querySelector('.tn-fav-hdr');
    if (hdr) {
        hdr.addEventListener('click', () => {
            const tc = sec.querySelector('.tc-fav');
            if (tc) { tc.classList.toggle('open'); }
            const arr = hdr.querySelector('.arr');
            if (arr) arr.classList.toggle('open');
        });
    }
    sec.querySelectorAll('.tn[data-type="table"]').forEach(node => {
        node.addEventListener('click', () => {
            document.querySelectorAll('#treeBody .tn.sel').forEach(n => n.classList.remove('sel'));
            node.classList.add('sel');
        });
        node.addEventListener('dblclick', e => { e.stopPropagation(); openDetail(node.dataset.table); });
        node.addEventListener('contextmenu', e => {
            e.preventDefault(); e.stopPropagation();
            showContextMenu(e.clientX, e.clientY, node.dataset.table);
        });
        node.addEventListener('mouseenter', () => _scheduleShowTooltip(node.dataset.table, 'public', node));
        node.addEventListener('mouseleave', e => {
            if (!e.relatedTarget || !e.relatedTarget.closest('#treeTooltip')) _scheduleHideTooltip();
        });
    });
}

// ─── HOVER TOOLTIP ───────────────────────────────────────────────
let _treeTooltip    = null;
let _tooltipShowTmr = null;
let _tooltipHideTmr = null;
const _tooltipCache = {};

function _getTreeTooltip() {
    if (!_treeTooltip) {
        _treeTooltip = document.createElement('div');
        _treeTooltip.id = 'treeTooltip';
        _treeTooltip.className = 'tree-tooltip';
        _treeTooltip.addEventListener('mouseenter', () => clearTimeout(_tooltipHideTmr));
        _treeTooltip.addEventListener('mouseleave', () => { if (_treeTooltip) _treeTooltip.style.display = 'none'; });
        document.body.appendChild(_treeTooltip);
    }
    return _treeTooltip;
}

function _scheduleShowTooltip(tableName, schema, anchorEl) {
    clearTimeout(_tooltipShowTmr);
    clearTimeout(_tooltipHideTmr);
    _tooltipShowTmr = setTimeout(() => _showTreeTooltip(tableName, schema || 'public', anchorEl), 400);
}

function _scheduleHideTooltip() {
    clearTimeout(_tooltipShowTmr);
    _tooltipHideTmr = setTimeout(() => {
        if (_treeTooltip) _treeTooltip.style.display = 'none';
    }, 120);
}

function _showTreeTooltip(tableName, schema, anchorEl) {
    const tt = _getTreeTooltip();
    const treeEl = document.getElementById('objectTree');
    const treeRight = treeEl ? treeEl.getBoundingClientRect().right : 240;
    const rect = anchorEl.getBoundingClientRect();

    tt.innerHTML = `<div class="ttt-name"><i class="bi bi-table ico-table"></i>${eh(tableName)}</div>
        <div class="ttt-loading"><span class="loading"></span> Загрузка...</div>`;
    tt.style.cssText = `display:block;left:${treeRight + 6}px;top:${rect.top}px`;

    _fetchTooltipData(tableName, schema).then(data => {
        if (!_treeTooltip || _treeTooltip.style.display === 'none') return;
        const { columns, stats } = data || {};
        let html = `<div class="ttt-name"><i class="bi bi-table ico-table"></i>${eh(schema + '.' + tableName)}</div>`;
        if (stats) {
            html += `<div class="ttt-stats">
                <span><i class="bi bi-list-ol"></i> ~${Number(stats.row_count || 0).toLocaleString()} строк</span>
                <span><i class="bi bi-hdd"></i> ${eh(stats.total_size || '?')}</span>
                <span><i class="bi bi-diagram-3"></i> ${stats.index_count ?? '?'} индекс.</span>
            </div>`;
        }
        if (columns && columns.length) {
            html += `<div class="ttt-cols">`;
            columns.forEach(c => {
                const badges = (c.is_pk ? '<span class="ttt-pk">PK</span>' : '') +
                               (c.is_fk ? '<span class="ttt-fk">FK</span>' : '') +
                               (!c.is_nullable ? '<span class="ttt-nn">NN</span>' : '');
                const comment = c.column_comment ? `<span class="ttt-cc" title="${eh(c.column_comment)}">${eh(c.column_comment)}</span>` : '';
                html += `<div class="ttt-col">${badges}<span class="ttt-cn">${eh(c.column_name)}</span><span class="ttt-ct">${eh(c.data_type)}</span>${comment}</div>`;
            });
            html += `</div>`;
        } else if (!stats) {
            html += `<div class="ttt-loading" style="font-size:10px;color:var(--text-3)">Нет данных</div>`;
        }
        tt.innerHTML = html;
        const ttH = tt.offsetHeight;
        const top = Math.min(rect.top, window.innerHeight - ttH - 10);
        tt.style.top = Math.max(10, top) + 'px';
    });
}

async function _fetchTooltipData(tableName, schema) {
    const key = `${schema}.${tableName}`;
    if (_tooltipCache[key]) return _tooltipCache[key];
    try {
        const [colRes, stRes] = await Promise.all([
            fetch(`/api/sql/table-detail?table=${encodeURIComponent(tableName)}&tab=columns`),
            fetch(`/api/sql/table-stats?table=${encodeURIComponent(tableName)}&schema=${encodeURIComponent(schema)}`)
        ]);
        const colData = await colRes.json();
        const stData  = await stRes.json();
        const result  = {
            columns: colData.success ? colData.data : [],
            stats:   stData.success  ? stData.data  : null
        };
        _tooltipCache[key] = result;
        return result;
    } catch(e) {
        return { columns: [], stats: null };
    }
}

const LAZY_ICONS = {
    views:'bi-eye ico-view', matviews:'bi-layers ico-view',
    functions:'bi-lightning ico-fn', procedures:'bi-gear ico-proc',
    sequences:'bi-123 ico-seq', triggers:'bi-lightning-charge ico-trig',
    types:'bi-tag ico-type', extensions:'bi-plug ico-ext'
};

async function buildTree() {
    const body = document.getElementById('treeBody');
    body.innerHTML = '<div class="tree-loading"><span class="loading"></span> Загрузка...</div>';
    try {
        const [schemasRes, tablesRes] = await Promise.all([
            fetchWithRetry('/api/sql/schemas'),
            fetchWithRetry('/api/get-tables?all=1'),
        ]);
        const sd = await schemasRes.json();
        const td = await tablesRes.json();
        if (!sd.success || !td.success) {
            body.innerHTML = `<div class="tree-loading">Ошибка</div>`;
            return;
        }
        const schemas = sd.data || ['public'];
        const tablesAll = td.data || [];
        S.tables = [...new Set(tablesAll.map(t => t.table_name))];
        _objLinkCache = null;
        _fnLinkCache  = null;
        _procLinkCache= null;

        _scheduleMonacoDecorationUpdate();

        const bySchema = new Map();
        for (const s of schemas) bySchema.set(s, []);
        for (const t of tablesAll) {
            if (!bySchema.has(t.schema)) bySchema.set(t.schema, []);
            bySchema.get(t.schema).push(t);
        }

        body.innerHTML = Array.from(bySchema.entries()).map(([schema, tables]) => {
            return renderGroup(schema, 'bi-layers ico-schema', [
                { label:'Tables', icon:'bi-table ico-group', badge: tables.length,
                  kids: tables.map(t=>({ label:t.table_name, icon:'bi-table ico-table', type:'table', tableName:t.table_name, schema: t.schema, comment:t.table_comment }))
                },
                { label:'Views',               icon:'bi-eye ico-group',          lazy:'views',      schema },
                { label:'Materialized Views',  icon:'bi-layers ico-group',       lazy:'matviews',   schema },
                { label:'Functions',           icon:'bi-lightning ico-fn',       lazy:'functions',  schema },
                { label:'Procedures',          icon:'bi-gear ico-proc',          lazy:'procedures', schema },
                { label:'Sequences',           icon:'bi-123 ico-seq',            lazy:'sequences',  schema },
                { label:'Triggers',            icon:'bi-lightning-charge ico-trig',lazy:'triggers', schema },
                { label:'Types',               icon:'bi-tag ico-type',           lazy:'types',      schema },
                { label:'Extensions',          icon:'bi-plug ico-ext',           lazy:'extensions', schema },
            ], 0);
        }).join('');
        bindTree();
        _renderFavSection();
    } catch(e) {
        console.error('[buildTree]', e);
        body.innerHTML = '<div class="tree-loading">' + eh(e.message) + '</div>';
    }
}

function renderGroup(label, icon, groups, depth) {
    const pad = depth * 14;
    let html = `<div class="tn" style="padding-left:${pad}px" data-type="schema" data-schema="${eh(label)}">
        <span class="arr">▶</span><span class="ico"><i class="bi ${icon}"></i></span>
        <span class="lbl">${eh(label)}</span></div>
    <div class="tc" data-schema="${eh(label)}">`;
    groups.forEach(g => {
        const bdg = g.badge != null ? `<span class="bdg">${g.badge}</span>` : '';
        const dp2 = (depth + 1) * 14;
        html += `<div class="tn" style="padding-left:${dp2}px" data-type="group" data-lazy="${g.lazy||''}" data-schema="${eh(g.schema || label)}">
            <span class="arr">${(g.kids?.length||g.lazy)?'▶':'<span style="visibility:hidden">▶</span>'}</span>
            <span class="ico"><i class="bi ${g.icon}"></i></span>
            <span class="lbl">${eh(g.label)}</span>${bdg}</div>
        <div class="tc" data-lazy="${g.lazy||''}" data-schema="${eh(g.schema || label)}">`;
        if (g.kids) {
            g.kids.forEach(k => {
                const dp3 = (depth + 2) * 14;
                const ta = k.tableName ? `data-table="${eh(k.tableName)}" data-schema="${eh(k.schema || label)}"` : '';
                const tt = k.comment ? ` title="${eh(k.comment)}"` : '';
                const isFav = k.type === 'table' && k.tableName ? _getFavs().has(k.tableName) : false;
                const starBtn = k.type === 'table' && k.tableName
                    ? `<button class="tn-star${isFav ? ' active' : ''}" data-table="${eh(k.tableName)}" title="${isFav ? 'Убрать из избранного' : 'В избранное'}"><i class="bi ${isFav ? 'bi-star-fill' : 'bi-star'}"></i></button>`
                    : '';
                html += `<div class="tn" style="padding-left:${dp3}px" data-type="${k.type||'obj'}" ${ta}${tt}>
                    <span class="arr" style="visibility:hidden">▶</span>
                    <span class="ico"><i class="bi ${k.icon}"></i></span>
                    <span class="lbl">${eh(k.label)}</span>${starBtn}</div>`;
            });
        }
        html += `</div>`;
    });
    html += `</div>`;
    return html;
}

function bindTree() {
    document.querySelectorAll('#treeBody .tn').forEach(node => {
        node.addEventListener('click', function(e) {
            if (e.ctrlKey || e.metaKey) return;
            const tc = this.nextElementSibling;
            if (tc && tc.classList.contains('tc')) {
                const isOpen = tc.classList.contains('open');
                if (!isOpen) {
                    tc.classList.add('open');
                    this.classList.add('open');
                    const lazy = tc.dataset.lazy || this.dataset.lazy;
                    if (lazy && tc.children.length === 0) {
                        loadLazy(tc, lazy);
                    }
                } else {
                    tc.classList.remove('open');
                    this.classList.remove('open');
                }
            }
            document.querySelectorAll('#treeBody .tn.sel').forEach(n => n.classList.remove('sel'));
            this.classList.add('sel');
        });
        if (node.dataset.type === 'table' && node.dataset.table) {
            node.addEventListener('dblclick', function(e) {
                e.stopPropagation();
                openDetail(this.dataset.table);
            });
            node.addEventListener('contextmenu', function(e) {
                e.preventDefault();
                e.stopPropagation();
                showContextMenu(e.clientX, e.clientY, this.dataset.table);
            });
            node.addEventListener('mouseenter', function() {
                _scheduleShowTooltip(this.dataset.table, this.dataset.schema || 'public', this);
            });
            node.addEventListener('mouseleave', function(e) {
                if (!e.relatedTarget || !e.relatedTarget.closest('#treeTooltip')) _scheduleHideTooltip();
            });
        }
    });
    // Delegate star button clicks (uses capture to intercept before node click)
    const treeBody = document.getElementById('treeBody');
    if (treeBody && !treeBody._starDelegated) {
        treeBody._starDelegated = true;
        treeBody.addEventListener('click', function(e) {
            const starBtn = e.target.closest('.tn-star');
            if (!starBtn) return;
            e.stopPropagation();
            toggleFavorite(starBtn.dataset.table);
        }, true);
    }
}

async function loadLazy(container, type) {
    if (type === 'functions' && S._fnList) {
        _renderLazyItems(container, S._fnList, type);
        return;
    }
    if (type === 'procedures' && S._procList) {
        _renderLazyItems(container, S._procList, type);
        return;
    }
    container.innerHTML = '<div class="tree-loading" style="padding-left:42px"><span class="loading"></span></div>';
    try {
        const schema = container.dataset.schema || container.previousElementSibling?.dataset.schema || 'public';
        const r = await fetch('/api/sql/schema-objects?type=' + encodeURIComponent(type) + '&schema=' + encodeURIComponent(schema));
        const d = await r.json();
        if (!d.success) {
            container.innerHTML = `<div style="padding:4px 0 4px 42px;font-size:11px;color:var(--red);">Ошибка: ${eh(d.error)}</div>`;
            return;
        }
        const items = d.data || [];
        _renderLazyItems(container, items, type);
    } catch(e) {
        container.innerHTML = `<div style="padding:6px 10px;font-size:11px;color:var(--red);">Ошибка: ${eh(e.message)}</div>`;
    }
}

function _renderLazyItems(container, items, type) {
    if (!items.length) {
        container.innerHTML = `<div style="padding:4px 0 4px 42px;font-size:11px;color:var(--text-3);">Нет объектов</div>`;
    } else {
        const icon = LAZY_ICONS[type] || 'bi-circle';
        const isRoutine = type === 'functions' || type === 'procedures';
        const routineType = type === 'functions' ? 'function' : 'procedure';
        container.innerHTML = items.map(name => `
            <div class="tn" style="padding-left:42px" data-type="${isRoutine ? routineType : 'obj'}" ${isRoutine ? `data-routine="${eh(name)}" data-routine-type="${routineType}"` : ''}>
                <span class="arr" style="visibility:hidden">▶</span>
                <span class="ico"><i class="bi ${icon}"></i></span>
                <span class="lbl">${eh(name)}</span>
            </div>`).join('');
    }
    const groupNode = container.previousElementSibling;
    if (groupNode && groupNode.classList.contains('tn')) {
        let bdg = groupNode.querySelector('.bdg');
        if (!bdg) {
            bdg = document.createElement('span');
            bdg.className = 'bdg';
            groupNode.appendChild(bdg);
        }
        bdg.textContent = items.length;
    }
    container.querySelectorAll('.tn[data-routine]').forEach(node => {
        node.setAttribute('tabindex', '0');
        node.addEventListener('dblclick', function(e) {
            e.stopPropagation();
            openRoutineDetail(this.dataset.routine, this.dataset.routineType);
        });
        node.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                e.stopPropagation();
                openRoutineDetail(this.dataset.routine, this.dataset.routineType);
            }
        });
    });
}

// ── Debounce filterTree ──
let _treeFilterTimer = null;
function filterTree(q) {
    clearTimeout(_treeFilterTimer);
    _treeFilterTimer = setTimeout(() => {
        const ql = q.toLowerCase();
        document.querySelectorAll('#treeBody .tn').forEach(n => {
            if (n.classList.contains('tn-fav-hdr')) return; // always keep favorites header
            const lbl = n.querySelector('.lbl')?.textContent.toLowerCase() || '';
            n.style.display = !ql || lbl.includes(ql) ? '' : 'none';
        });
    }, 100);
}
