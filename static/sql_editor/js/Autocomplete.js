// ═══════════════════════════════════════════════════════════════════
// Autocomplete.js — SQL autocomplete, syntax highlighting, find/replace, cheatsheet
// ═══════════════════════════════════════════════════════════════════

function getWord(ta) {
    if (!ta) ta = getActiveTA();
    const m = ta.value.substring(0, ta.selectionStart).match(/[\w.]+$/);
    return m ? m[0] : '';
}

// ── showAC — guarded against freeze when typing '.' ──
function showAC(force = false, ta = null) {
    if (!ta) ta = getActiveTA();
    if (ta && ta._isMonaco) return;
    const w = getWord(ta);
    if (!force && w.length < 1) { hideAC(); return; }
    const up = w.toUpperCase();
    const items = [];

    const dotIdx = w.lastIndexOf('.');
    if (dotIdx > 0) {
        const tblPart = w.substring(0, dotIdx);
        if (!tblPart || !/[a-zA-Z_]/.test(tblPart)) { hideAC(); return; }
        const colPart = w.substring(dotIdx + 1).toLowerCase();
        const cols = S.colCache[tblPart] || S.colCache[tblPart.toLowerCase()] || [];
        if (cols.length) {
            cols.filter(c => c.toLowerCase().startsWith(colPart)).slice(0, 12).forEach(c => items.push({ label: tblPart + '.' + c, type: 'col' }));
        } else if (!S._acLoading) {
            S._acLoading = tblPart;
            loadTableColumns(tblPart).then(() => {
                S._acLoading = null;
                showAC(force, ta);
            }).catch(() => { S._acLoading = null; });
            return;
        }
    } else {
        SQL_KW.filter(k => k.startsWith(up)).slice(0, 8).forEach(k => items.push({ label: k, type: 'kw' }));
        SQL_FN.filter(f => f.startsWith(up)).slice(0, 6).forEach(f => items.push({ label: f + '(', type: 'fn' }));
        S.tables.filter(t => t.toLowerCase().startsWith(w.toLowerCase())).slice(0, 8).forEach(t => items.push({ label: t, type: 'tbl' }));
    }
    if (!items.length) { hideAC(); return; }
    S.acItems = items;
    S.acIdx = -1;
    const m = document.getElementById('acMenu');
    m.innerHTML = items.map((it, i) => `<div class="ac-it ${it.type}" data-idx="${i}"><span class="ac-tp">${it.type.toUpperCase()}</span>${eh(it.label)}</div>`).join('');
    m.querySelectorAll('.ac-it').forEach(el => {
        el.addEventListener('mousedown', e => {
            e.preventDefault();
            const idx = parseInt(el.dataset.idx);
            applyAC(S.acItems[idx], ta);
        });
    });
    m.style.display = 'block';
    const r = ta.getBoundingClientRect();
    const lines = ta.value.substring(0, ta.selectionStart).split('\n');
    const lineIdx = lines.length - 1;
    const curLine = lines[lineIdx];
    const lineH = 20, padTop = 8, padLeft = 14, charW = 7.84;
    const rawY = r.top + padTop + lineIdx * lineH - ta.scrollTop + lineH + 2;
    const rawX = r.left + padLeft + curLine.length * charW - ta.scrollLeft;
    m.style.top = Math.min(rawY, window.innerHeight - 200) + 'px';
    m.style.left = Math.min(Math.max(rawX, r.left), window.innerWidth - 324) + 'px';
}

function hideAC() { document.getElementById('acMenu').style.display = 'none'; S.acIdx = -1; }

function acMove(d) {
    const els = document.querySelectorAll('.ac-it');
    els.forEach(e => e.classList.remove('sel'));
    S.acIdx = Math.max(0, Math.min(els.length - 1, S.acIdx + d));
    els[S.acIdx]?.classList.add('sel');
    els[S.acIdx]?.scrollIntoView({ block: 'nearest' });
}

function applyAC(item, ta = null) {
    if (!item) return;
    if (!ta) ta = getActiveTA();
    const pos = ta.selectionStart;
    const m = ta.value.substring(0, pos).match(/[\w.]+$/);
    const wl = m ? m[0].length : 0;
    ta.value = ta.value.substring(0, pos - wl) + item.label + ta.value.substring(pos);
    ta.selectionStart = ta.selectionEnd = pos - wl + item.label.length;
    hideAC();
    syncNums(ta);
    updateHighlight(ta);
    ta.focus();
}

// ═══ COLUMN AUTOCOMPLETE ═══
async function loadTableColumns(tableName) {
    if (S.colCache[tableName]) return;
    S.colCache[tableName] = [];
    try {
        const r = await fetch(`/api/table/${encodeURIComponent(tableName)}/columns`);
        if (!r.ok) return;
        const d = await r.json();
        if (d.success || Array.isArray(d.data)) {
            const rows = d.data || [];
            S.colCache[tableName] = rows.map(c => c.column_name || c).filter(Boolean);
            const typeMap = {};
            rows.forEach(c => { if (c.column_name && c.data_type) typeMap[c.column_name] = c.data_type; });
            if (Object.keys(typeMap).length) S.typeCache[tableName] = typeMap;
        }
    } catch(e) { /* ignore */ }
}

// ═══ SYNTAX HIGHLIGHTING ═══
(function() {
    const SQL_KW_SET = new Set(SQL_KW);
    const SQL_FN_SET = new Set(SQL_FN);

    window.highlightSQL = function(text) {
        if (!text) return '\n';
        let html = '';
        let i = 0;
        const len = text.length;

        while (i < len) {
            // Line comment
            if (text[i] === '-' && i + 1 < len && text[i + 1] === '-') {
                let j = i;
                while (j < len && text[j] !== '\n') j++;
                html += '<span class="hl-cmt">' + eh(text.slice(i, j)) + '</span>';
                i = j;
                continue;
            }
            // Block comment
            if (text[i] === '/' && i + 1 < len && text[i + 1] === '*') {
                let j = text.indexOf('*/', i + 2);
                j = j < 0 ? len : j + 2;
                html += '<span class="hl-cmt">' + eh(text.slice(i, j)) + '</span>';
                i = j;
                continue;
            }
            // Dollar-quoted string
            if (text[i] === '$' && i + 1 < len && text[i + 1] === '$') {
                let j = text.indexOf('$$', i + 2);
                j = j < 0 ? len : j + 2;
                html += '<span class="hl-str">' + eh(text.slice(i, j)) + '</span>';
                i = j;
                continue;
            }
            // Single-quoted string
            if (text[i] === "'") {
                let j = i + 1;
                while (j < len) {
                    if (text[j] === "'" && j + 1 < len && text[j + 1] === "'") { j += 2; continue; }
                    if (text[j] === "'") { j++; break; }
                    j++;
                }
                html += '<span class="hl-str">' + eh(text.slice(i, j)) + '</span>';
                i = j;
                continue;
            }
            // Number
            if (/[0-9]/.test(text[i]) || (text[i] === '.' && i + 1 < len && /[0-9]/.test(text[i + 1]))) {
                let j = i;
                while (j < len && /[\d.eExX_a-fA-F]/.test(text[j])) j++;
                html += '<span class="hl-num">' + eh(text.slice(i, j)) + '</span>';
                i = j;
                continue;
            }
            // Identifier or keyword
            if (/[a-zA-Z_]/.test(text[i])) {
                let j = i;
                while (j < len && /[\w]/.test(text[j])) j++;
                const word = text.slice(i, j);
                const upper = word.toUpperCase();
                if (SQL_KW_SET.has(upper)) {
                    html += '<span class="hl-kw">' + eh(word) + '</span>';
                } else if (SQL_FN_SET.has(upper)) {
                    html += '<span class="hl-fn">' + eh(word) + '</span>';
                } else {
                    html += eh(word);
                }
                i = j;
                continue;
            }
            // Operators
            if ('=<>!+-*/%|&^~'.indexOf(text[i]) >= 0) {
                html += '<span class="hl-op">' + eh(text[i]) + '</span>';
                i++;
                continue;
            }
            html += eh(text[i]);
            i++;
        }
        return html + '\n';
    };
})();

// ── rAF-throttled syntax highlighting ──
let _hlFrame = null;
function updateHighlight(ta) {
    if (!ta || ta._isMonaco) return;
    cancelAnimationFrame(_hlFrame);
    _hlFrame = requestAnimationFrame(() => {
        const hl = ta.previousElementSibling;
        if (!hl || !hl.classList.contains('sql-hl')) return;
        hl.innerHTML = highlightSQL(ta.value);
        hl.scrollTop = ta.scrollTop;
        hl.scrollLeft = ta.scrollLeft;
    });
}

// ── Object link + selection highlight wrapper ──
let _objLinkCache = null;
let _fnLinkCache  = null;
let _procLinkCache = null;

function _wrapHighlightSQL() {
    const base = window.highlightSQL;
    window.highlightSQL = function(text) {
        let html = base(text);
        const findBarOpen = document.getElementById('findBar')?.classList.contains('show');

        if (!findBarOpen) {
            const ta = getActiveTA();
            if (ta && ta.selectionStart !== ta.selectionEnd) {
                const sel = ta.value.substring(ta.selectionStart, ta.selectionEnd).trim();
                if (sel.length >= 2 && !/\s/.test(sel)) {
                    try {
                        const re = new RegExp(sel.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi');
                        const ranges = [];
                        let mm;
                        while ((mm = re.exec(text)) !== null) {
                            const isCur = mm.index === ta.selectionStart && mm.index + mm[0].length === ta.selectionEnd;
                            ranges.push({ start: mm.index, end: mm.index + mm[0].length, isCur });
                        }
                        if (ranges.length > 1) {
                            html = _applySelWordMarks(base, text, ranges);
                        }
                    } catch(e) {}
                }
            }
        } else if (typeof FB !== 'undefined' && FB.matches && FB.matches.length > 0) {
            html = _applyMatchMarksRanges(base, text, FB.matches, FB.cur);
        }

        if ((S.tables && S.tables.length) || (S._fnList && S._fnList.length) || (S._procList && S._procList.length)) {
            html = _applyObjectLinks(base, html, text);
        }
        return html;
    };
}

function _applySelWordMarks(base, text, ranges) {
    const events = [];
    let prev = 0;
    ranges.forEach(r => {
        if (r.start > prev) events.push({ type: 'text', val: text.slice(prev, r.start) });
        events.push({ type: 'match', val: text.slice(r.start, r.end), isCur: r.isCur });
        prev = r.end;
    });
    if (prev < text.length) events.push({ type: 'text', val: text.slice(prev) });
    return events.map(ev => {
        const inner = base(ev.val).replace(/\n$/, '');
        if (ev.type === 'match') {
            return `<mark class="${ev.isCur ? 'hl-match-cur' : 'hl-sel-word'}">${inner}</mark>`;
        }
        return inner;
    }).join('') + '\n';
}

function _applyObjectLinks(base, existingHtml, text) {
    if (!_objLinkCache || _objLinkCache.length !== S.tables.length) {
        const unqualified = [...new Set(S.tables.map(t => (t.includes('.') ? t.split('.').pop() : t)))];
        _objLinkCache = unqualified.sort((a, b) => b.length - a.length);
    }
    const fnList   = S._fnList   || [];
    const procList = S._procList || [];
    if (!_fnLinkCache   || _fnLinkCache.length   !== fnList.length)   { _fnLinkCache   = [...fnList].sort((a, b) => b.length - a.length); }
    if (!_procLinkCache || _procLinkCache.length !== procList.length) { _procLinkCache = [...procList].sort((a, b) => b.length - a.length); }

    const allObjs = [
        ..._objLinkCache.map(n => ({ name: n, cls: 'hl-obj-link'  })),
        ..._fnLinkCache  .map(n => ({ name: n, cls: 'hl-fn-link'  })),
        ..._procLinkCache.map(n => ({ name: n, cls: 'hl-proc-link' }))
    ].sort((a, b) => b.name.length - a.name.length);

    if (!allObjs.length) return existingHtml;

    const pattern = allObjs.map(o => o.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
    const nameToClass = {};
    for (let i = allObjs.length - 1; i >= 0; i--) { nameToClass[allObjs[i].name.toLowerCase()] = allObjs[i].cls; }

    let re;
    try { re = new RegExp(`\\b(${pattern})\\b`, 'gi'); }
    catch(e) { return existingHtml; }

    const ranges = [];
    let m;
    while ((m = re.exec(text)) !== null) {
        const lineStart = text.lastIndexOf('\n', m.index) + 1;
        const linePrefix = text.slice(lineStart, m.index);
        if (linePrefix.includes('--')) continue;
        const cls = nameToClass[m[0].toLowerCase()] || 'hl-obj-link';
        ranges.push({ start: m.index, end: m.index + m[0].length, name: m[0], cls });
    }
    if (!ranges.length) return existingHtml;

    const events = [];
    let prev = 0;
    ranges.forEach(r => {
        if (r.start > prev) events.push({ type: 'text', val: text.slice(prev, r.start) });
        events.push({ type: 'obj', val: text.slice(r.start, r.end), name: r.name, cls: r.cls });
        prev = r.end;
    });
    if (prev < text.length) events.push({ type: 'text', val: text.slice(prev) });

    return events.map(ev => {
        const inner = base(ev.val).replace(/\n$/, '');
        if (ev.type === 'obj') {
            return `<span class="${ev.cls}" data-obj="${eh(ev.name)}">${inner}</span>`;
        }
        return inner;
    }).join('') + '\n';
}

// Wire obj link clicks via event delegation (pointer-events toggled via body.ctrl-held CSS)
document.addEventListener('click', function(e) {
    if (!(e.ctrlKey || e.metaKey)) return;
    const link = e.target.closest('.hl-obj-link,.hl-fn-link,.hl-proc-link');
    if (!link) return;
    e.preventDefault();
    const name = link.dataset.obj;
    if (!name) return;
    if (link.classList.contains('hl-fn-link')) {
        openRoutineDetail(name, 'function');
    } else if (link.classList.contains('hl-proc-link')) {
        openRoutineDetail(name, 'procedure');
    } else {
        openDetail(name);
    }
});

// ── Apply highlight marks for find bar ──
function _applyMatchMarks(base, text, needle, caseSensitive, curIdx) {
    if (!needle) return base(text);
    try {
        const re = new RegExp(needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), caseSensitive ? 'g' : 'gi');
        const ranges = [];
        let m;
        while ((m = re.exec(text)) !== null) ranges.push({ start: m.index, end: m.index + m[0].length });
        return _applyMatchMarksRanges(base, text, ranges, curIdx);
    } catch(e) { return base(text); }
}

function _applyMatchMarksRanges(base, text, ranges, curIdx) {
    if (!ranges.length) return base(text);
    const events = [];
    let prev = 0;
    ranges.forEach((r, i) => {
        if (r.start > prev) events.push({ type: 'text', val: text.slice(prev, r.start) });
        events.push({ type: 'match', val: text.slice(r.start, r.end), idx: i });
        prev = r.end;
    });
    if (prev < text.length) events.push({ type: 'text', val: text.slice(prev) });
    return events.map(ev => {
        const inner = base(ev.val).replace(/\n$/, '');
        if (ev.type === 'match') {
            const cls = ev.idx === curIdx ? 'hl-match-cur' : 'hl-match';
            return `<mark class="${cls}">${inner}</mark>`;
        }
        return inner;
    }).join('') + '\n';
}

// Call wrapper — must be after all helper functions are defined
_wrapHighlightSQL();

// ═══════════════════════════════════════════════════════════════════
// FIND / REPLACE
// ═══════════════════════════════════════════════════════════════════
const FB = {
    matches: [],
    cur: -1,
    caseSensitive: false,
    useRegex: false,
};

function showFindBar(withReplace = false) {
    const editor = monacoEditors[S.currentEditorTab];
    if (editor) {
        const action = withReplace ? 'editor.action.startFindReplaceAction' : 'actions.find';
        editor.getAction(action)?.run();
        return;
    }
    const bar = document.getElementById('findBar');
    bar.classList.add('show');
    const ep = document.getElementById('editorPane');
    if (ep) ep.style.position = 'relative';
    const fi = document.getElementById('fbFindInput');
    if (fi) { fi.focus(); fi.select(); }
    const ta = getActiveTA();
    if (ta) {
        const sel = ta.value.substring(ta.selectionStart, ta.selectionEnd).trim();
        if (sel && sel.length < 200 && !sel.includes('\n')) { fi.value = sel; }
    }
    fbSearch();
}

function hideFindBar() {
    if (monacoEditors[S.currentEditorTab]) return;
    document.getElementById('findBar').classList.remove('show');
    FB.matches = []; FB.cur = -1;
    const ta = getActiveTA();
    if (ta && !ta._isMonaco) updateHighlight(ta);
    document.getElementById('fbCount').textContent = '';
}

function fbToggleCase() {
    FB.caseSensitive = !FB.caseSensitive;
    document.getElementById('fbCaseBtn').classList.toggle('active', FB.caseSensitive);
    fbSearch();
}

function fbToggleRegex() {
    FB.useRegex = !FB.useRegex;
    document.getElementById('fbRegexBtn').classList.toggle('active', FB.useRegex);
    fbSearch();
}

function fbSearch() {
    const ta = getActiveTA();
    if (!ta || ta._isMonaco) return;
    const needle = document.getElementById('fbFindInput').value;
    FB.matches = [];
    FB.cur = -1;

    if (!needle) {
        document.getElementById('fbCount').textContent = '';
        updateHighlight(ta);
        return;
    }

    const text = ta.value;
    try {
        let re;
        if (FB.useRegex) {
            re = new RegExp(needle, FB.caseSensitive ? 'g' : 'gi');
        } else {
            re = new RegExp(needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), FB.caseSensitive ? 'g' : 'gi');
        }
        let m;
        while ((m = re.exec(text)) !== null) {
            FB.matches.push({ start: m.index, end: m.index + m[0].length });
            if (FB.matches.length > 5000) break;
        }
    } catch(e) { /* invalid regex */ }

    if (FB.matches.length > 0) {
        const pos = ta.selectionStart;
        FB.cur = FB.matches.findIndex(m => m.start >= pos);
        if (FB.cur === -1) FB.cur = 0;
        _fbScrollToCurrent(ta);
    }

    const cnt = document.getElementById('fbCount');
    cnt.textContent = FB.matches.length ? `${FB.cur + 1} / ${FB.matches.length}` : 'Не найдено';
    cnt.style.color = FB.matches.length ? 'var(--text-3)' : 'var(--red)';

    updateHighlight(ta);
}

function fbMove(dir) {
    if (!FB.matches.length) return;
    FB.cur = (FB.cur + dir + FB.matches.length) % FB.matches.length;
    document.getElementById('fbCount').textContent = `${FB.cur + 1} / ${FB.matches.length}`;
    const ta = getActiveTA();
    if (ta) _fbScrollToCurrent(ta);
    updateHighlight(ta);
}

function _fbScrollToCurrent(ta) {
    const m = FB.matches[FB.cur];
    if (!m) return;
    ta.selectionStart = m.start;
    ta.selectionEnd = m.end;
    const linesBefore = ta.value.substring(0, m.start).split('\n').length;
    ta.scrollTop = Math.max(0, (linesBefore - 4) * 20);
}

function fbReplaceOne() {
    const ta = getActiveTA();
    if (!ta || !FB.matches.length || FB.cur < 0) return;
    const m = FB.matches[FB.cur];
    const rep = document.getElementById('fbReplaceInput').value;
    ta.value = ta.value.substring(0, m.start) + rep + ta.value.substring(m.end);
    ta.selectionStart = ta.selectionEnd = m.start + rep.length;
    syncNums(ta); saveTabContent(ta.dataset.tab, ta.value);
    fbSearch();
}

function fbReplaceAll() {
    const ta = getActiveTA();
    if (!ta || !FB.matches.length) return;
    const needle = document.getElementById('fbFindInput').value;
    const rep = document.getElementById('fbReplaceInput').value;
    let count = 0;
    try {
        let re;
        if (FB.useRegex) {
            re = new RegExp(needle, FB.caseSensitive ? 'g' : 'gi');
        } else {
            re = new RegExp(needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), FB.caseSensitive ? 'g' : 'gi');
        }
        const orig = ta.value;
        ta.value = orig.replace(re, () => { count++; return rep; });
    } catch(e) { showNotice('Некорректное регулярное выражение', 'err'); return; }
    syncNums(ta); updateHighlight(ta); saveTabContent(ta.dataset.tab, ta.value);
    showNotice(`Заменено: ${count}`);
    fbSearch();
}

// Wire find bar input events
document.addEventListener('DOMContentLoaded', () => {
    const fi = document.getElementById('fbFindInput');
    if (fi) {
        fi.addEventListener('input', fbSearch);
        fi.addEventListener('keydown', e => {
            if (e.key === 'Enter') { e.preventDefault(); fbMove(e.shiftKey ? -1 : 1); }
            if (e.key === 'Escape') { e.preventDefault(); hideFindBar(); }
            if (e.altKey && e.key.toLowerCase() === 'c') { e.preventDefault(); fbToggleCase(); }
            if (e.altKey && e.key.toLowerCase() === 'r') { e.preventDefault(); fbToggleRegex(); }
        });
    }
    const ri = document.getElementById('fbReplaceInput');
    if (ri) {
        ri.addEventListener('keydown', e => {
            if (e.key === 'Escape') { e.preventDefault(); hideFindBar(); }
            if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); fbReplaceAll(); }
        });
    }
});

// ═══════════════════════════════════════════════════════════════════
// KEYBOARD CHEATSHEET
// ═══════════════════════════════════════════════════════════════════
function showCheatsheet() {
    document.getElementById('cheatsheetModal')?.classList.add('show');
}
function hideCheatsheet() {
    document.getElementById('cheatsheetModal')?.classList.remove('show');
}

// Wire global hotkeys for find bar and cheatsheet
(function() {
    document.addEventListener('keydown', e => {
        const isMonaco = !!monacoEditors[S.currentEditorTab];
        if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key === 's') {
            if (!isMonaco) {
                e.preventDefault();
                if (typeof formatSQL === 'function') formatSQL();
                if (typeof showNotice === 'function') showNotice('✓ Отформатировано и сохранено', 'ok');
            }
            return;
        }
        if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key === 'f') {
            if (!isMonaco && (document.activeElement?.closest('#sqlEditorRoot'))) {
                e.preventDefault(); showFindBar(false); return;
            }
        }
        if ((e.ctrlKey || e.metaKey) && e.key === 'h') {
            if (!isMonaco) {
                e.preventDefault(); showFindBar(true);
                setTimeout(() => document.getElementById('fbReplaceInput')?.focus(), 50); return;
            }
        }
        if ((e.ctrlKey || e.metaKey) && e.key === '?') {
            e.preventDefault(); showCheatsheet(); return;
        }
        if (e.key === 'Escape') {
            if (document.getElementById('findBar')?.classList.contains('show')) {
                hideFindBar(); getActiveTA()?.focus();
            }
            if (document.getElementById('cheatsheetModal')?.classList.contains('show')) {
                hideCheatsheet();
            }
        }
    });
})();
