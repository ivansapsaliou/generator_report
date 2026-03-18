// ═══════════════════════════════════════════════════════════════════
// DatabaseTree.js — Left tree panel: building and managing DB objects tree
// ═══════════════════════════════════════════════════════════════════

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
                html += `<div class="tn" style="padding-left:${dp3}px" data-type="${k.type||'obj'}" ${ta}${tt}>
                    <span class="arr" style="visibility:hidden">▶</span>
                    <span class="ico"><i class="bi ${k.icon}"></i></span>
                    <span class="lbl">${eh(k.label)}</span></div>`;
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
            if (!node.title) node.title = 'Двойной клик — открыть структуру';
        }
    });
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
            <div class="tn" style="padding-left:42px" data-type="${isRoutine ? routineType : 'obj'}" ${isRoutine ? `data-routine="${eh(name)}" data-routine-type="${routineType}" title="Двойной клик — открыть DDL и параметры"` : ''}>
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
            const lbl = n.querySelector('.lbl')?.textContent.toLowerCase() || '';
            n.style.display = !ql || lbl.includes(ql) ? '' : 'none';
        });
    }, 100);
}
