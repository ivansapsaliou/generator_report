// ═══════════════════════════════════════════════════════════════════
// EditorManager.js — Monaco editor setup, tab management, key handling
// ═══════════════════════════════════════════════════════════════════

// ═══ MONACO STATE ═══
const monacoEditors = {};
let _monacoApiReady = false;
let _domContentLoaded = false;

function _tryAfterBothReady() {
    if (!_monacoApiReady || !_domContentLoaded) return;
    initEditor();
}

// ═══ MONACO API LOADING ═══
require.config({ paths: { 'vs': 'https://cdn.jsdelivr.net/npm/monaco-editor@0.44.0/min/vs' } });
require(['vs/editor/editor.main'], function() {
    _monacoApiReady = true;
    _setupMonacoTheme();
    _registerMonacoCompletion();
    _tryAfterBothReady();
});

function _getCurrentMonacoTheme() {
    return document.body.classList.contains('light-theme') ? 'sql-light' : 'sql-dark';
}

function _setupMonacoTheme() {
    monaco.editor.defineTheme('sql-dark', {
        base: 'vs-dark',
        inherit: true,
        rules: [
            { token: 'keyword',               foreground: 'cf9fff', fontStyle: 'bold' },
            { token: 'predefined',            foreground: '79c0ff' },
            { token: 'string',                foreground: 'a8d9a4' },
            { token: 'string.escape',         foreground: '56d364' },
            { token: 'string.invalid',        foreground: 'ff7b72' },
            { token: 'number',                foreground: 'f0a843' },
            { token: 'number.float',          foreground: 'f0a843' },
            { token: 'comment',               foreground: '6a737d', fontStyle: 'italic' },
            { token: 'comment.quote',         foreground: '6a737d', fontStyle: 'italic' },
            { token: 'operator',              foreground: 'f39c12' },
            { token: 'delimiter',             foreground: 'e8e3dc' },
            { token: 'delimiter.parenthesis', foreground: 'a09890' },
            { token: 'delimiter.square',      foreground: 'a09890' },
            { token: 'type',                  foreground: 'cf9fff' },
            { token: '',                      foreground: 'e8e3dc' },
        ],
        colors: {
            'editor.background':                   '#141414',
            'editor.foreground':                   '#e8e3dc',
            'editor.lineHighlightBackground':      '#1c1c1c',
            'editor.lineHighlightBorder':          '#00000000',
            'editor.selectionBackground':          '#f0a84330',
            'editor.inactiveSelectionBackground':  '#2e2e2e',
            'editorLineNumber.foreground':         '#5c5550',
            'editorLineNumber.activeForeground':   '#a09890',
            'editorCursor.foreground':             '#f0a843',
            'editorIndentGuide.background':        '#2a2a2a',
            'editorIndentGuide.activeBackground':  '#3d3d3d',
            'editorBracketMatch.background':       '#2d4b2d',
            'editorBracketMatch.border':           '#52b788',
            'scrollbarSlider.background':          '#3d3d3d80',
            'scrollbarSlider.hoverBackground':     '#5d5d5d90',
            'editorWidget.background':             '#1c1c1c',
            'editorWidget.border':                 '#2a2a2a',
            'editorSuggestWidget.background':      '#1c1c1c',
            'editorSuggestWidget.border':          '#2a2a2a',
            'editorSuggestWidget.selectedBackground': '#f0a84322',
            'editorSuggestWidget.highlightForeground': '#f0a843',
            'editorHoverWidget.background':        '#1c1c1c',
            'editorHoverWidget.border':            '#2a2a2a',
        }
    });
    monaco.editor.defineTheme('sql-light', {
        base: 'vs',
        inherit: true,
        rules: [
            { token: 'keyword',               foreground: '7c3aed', fontStyle: 'bold' },
            { token: 'predefined',            foreground: '1d4ed8' },
            { token: 'string',                foreground: '16a34a' },
            { token: 'string.escape',         foreground: '166534' },
            { token: 'string.invalid',        foreground: 'dc2626' },
            { token: 'number',                foreground: 'b8680a' },
            { token: 'number.float',          foreground: 'b8680a' },
            { token: 'comment',               foreground: '9a9490', fontStyle: 'italic' },
            { token: 'comment.quote',         foreground: '9a9490', fontStyle: 'italic' },
            { token: 'operator',              foreground: 'b8680a' },
            { token: 'delimiter',             foreground: '1a1714' },
            { token: 'delimiter.parenthesis', foreground: '5a5450' },
            { token: 'delimiter.square',      foreground: '5a5450' },
            { token: 'type',                  foreground: '7c3aed' },
            { token: '',                      foreground: '1a1714' },
        ],
        colors: {
            'editor.background':                   '#eeebe6',
            'editor.foreground':                   '#1a1714',
            'editor.lineHighlightBackground':      '#e8e4de',
            'editor.lineHighlightBorder':          '#00000000',
            'editor.selectionBackground':          '#b8680a28',
            'editor.inactiveSelectionBackground':  '#d4d0ca',
            'editorLineNumber.foreground':         '#9a9490',
            'editorLineNumber.activeForeground':   '#5a5450',
            'editorCursor.foreground':             '#b8680a',
            'editorIndentGuide.background':        '#d4d0ca',
            'editorIndentGuide.activeBackground':  '#c4c0ba',
            'editorBracketMatch.background':       '#d1fae5',
            'editorBracketMatch.border':           '#059669',
            'scrollbarSlider.background':          '#d1d5db80',
            'scrollbarSlider.hoverBackground':     '#9ca3af90',
            'editorWidget.background':             '#e8e4de',
            'editorWidget.border':                 '#d0ccc6',
            'editorSuggestWidget.background':      '#e8e4de',
            'editorSuggestWidget.border':          '#d0ccc6',
            'editorSuggestWidget.selectedBackground': '#b8680a18',
            'editorSuggestWidget.highlightForeground': '#b8680a',
            'editorHoverWidget.background':        '#e8e4de',
            'editorHoverWidget.border':            '#d0ccc6',
        }
    });
    monaco.editor.setTheme(_getCurrentMonacoTheme());
    new MutationObserver(() => {
        monaco.editor.setTheme(_getCurrentMonacoTheme());
    }).observe(document.body, { attributes: true, attributeFilter: ['class'] });
}

// ═══ MONACO OBJECT DECORATIONS ═══
const _monacoObjDecorations = {};
let _monacoDecTimer = null;

function _scheduleMonacoDecorationUpdate() {
    clearTimeout(_monacoDecTimer);
    _monacoDecTimer = setTimeout(() => {
        Object.entries(monacoEditors).forEach(([tabId, editor]) => {
            _updateMonacoObjectDecorations(editor, tabId);
        });
    }, 400);
}

function _updateMonacoObjectDecorations(editor, tabId) {
    if (!editor) return;
    const model = editor.getModel();
    if (!model) return;

    const tables = S.tables   || [];
    const fns    = S._fnList  || [];
    const procs  = S._procList|| [];
    if (!tables.length && !fns.length && !procs.length) {
        if (_monacoObjDecorations[tabId]) _monacoObjDecorations[tabId].clear();
        return;
    }

    const tableNames = [...new Set(tables.map(n => (n.includes('.') ? n.split('.').pop() : n)))];
    const allObjs = [
        ...tableNames.map(n => ({ name: n, cls: 'monaco-obj-table' })),
        ...fns       .map(n => ({ name: n, cls: 'monaco-obj-fn'    })),
        ...procs     .map(n => ({ name: n, cls: 'monaco-obj-proc'  })),
    ].sort((a, b) => b.name.length - a.name.length);

    const text = model.getValue();
    if (!text.trim()) {
        if (_monacoObjDecorations[tabId]) _monacoObjDecorations[tabId].clear();
        return;
    }

    let re;
    try {
        const pattern = allObjs.map(o => o.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
        re = new RegExp(`\\b(${pattern})\\b`, 'gi');
    } catch (e) { return; }

    const nameToClass = {};
    for (let i = allObjs.length - 1; i >= 0; i--) {
        nameToClass[allObjs[i].name.toLowerCase()] = allObjs[i].cls;
    }

    const decorations = [];
    let m;
    while ((m = re.exec(text)) !== null) {
        const lineStart = text.lastIndexOf('\n', m.index) + 1;
        const linePrefix = text.slice(lineStart, m.index);
        if (linePrefix.includes('--')) continue;
        const cls = nameToClass[m[0].toLowerCase()] || 'monaco-obj-table';
        const startPos = model.getPositionAt(m.index);
        const endPos   = model.getPositionAt(m.index + m[0].length);
        decorations.push({
            range: new monaco.Range(startPos.lineNumber, startPos.column, endPos.lineNumber, endPos.column),
            options: { inlineClassName: cls }
        });
    }

    if (!_monacoObjDecorations[tabId]) {
        _monacoObjDecorations[tabId] = editor.createDecorationsCollection(decorations);
    } else {
        _monacoObjDecorations[tabId].set(decorations);
    }
}

// ═══ MONACO COMPLETION HELPERS ═══
function _buildAliasMap(sql) {
    const map = {};
    const re = /\b(?:FROM|JOIN)\s+((?:\w+\.)?\w+)\s+(?:AS\s+)?(\w+)(?:\s|,|$)/gi;
    let m;
    while ((m = re.exec(sql)) !== null) {
        const tbl = m[1], alias = m[2].toLowerCase();
        const kwSet = new Set(['WHERE','ON','SET','SELECT','JOIN','LEFT','RIGHT','INNER','OUTER','FULL','CROSS','GROUP','ORDER','HAVING','LIMIT','OFFSET','UNION','EXCEPT','INTERSECT','AND','OR','NOT','AS']);
        if (!kwSet.has(alias.toUpperCase())) map[alias] = tbl;
    }
    return map;
}

const _ALIAS_SKIP = new Set(['WHERE','ON','SET','SELECT','JOIN','LEFT','RIGHT','INNER','OUTER','FULL','CROSS','GROUP','ORDER','HAVING','LIMIT','OFFSET','UNION','EXCEPT','INTERSECT','AND','OR','NOT','AS','FROM','INTO','TABLE','INDEX','VIEW']);

function _extractTablesFromSql(sql) {
    const tables = [];
    const re = /(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)/gi;
    let m;
    while ((m = re.exec(sql)) !== null) {
        const name = m[1];
        if (!_ALIAS_SKIP.has(name.toUpperCase())) {
            tables.push(name.includes('.') ? name.split('.').pop() : name);
        }
    }
    return [...new Set(tables)];
}

function _buildTableToAlias(aliasMap) {
    const rev = {};
    for (const [alias, tbl] of Object.entries(aliasMap)) {
        const tblName = (tbl.includes('.') ? tbl.split('.').pop() : tbl).toLowerCase();
        rev[tblName] = alias;
    }
    return rev;
}

function _applyAliasesToJoinSuggestion(suggestion, tableToAlias) {
    let result = suggestion;
    for (const [tblName, alias] of Object.entries(tableToAlias)) {
        const safe = tblName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        result = result.replace(new RegExp('"' + safe + '"\\.', 'gi'), alias + '.');
        result = result.replace(new RegExp('(?<![\\w."])' + safe + '\\.', 'gi'), alias + '.');
    }
    return result;
}

function _getAliasDotStarSuggestions(aliasMap, wordPrefix, range) {
    const filterLower = (wordPrefix || '').toLowerCase();
    const suggestions = [];
    for (const [alias, tbl] of Object.entries(aliasMap)) {
        const item = alias + '.*';
        if (!filterLower || item.startsWith(filterLower)) {
            suggestions.push({
                label: item,
                kind: monaco.languages.CompletionItemKind.Snippet,
                insertText: item,
                range,
                detail: tbl.includes('.') ? tbl.split('.').pop() : tbl,
                sortText: '0' + item,
            });
        }
    }
    return suggestions;
}

async function _loadPossibleJoins(tableName) {
    if (S.joinCache[tableName] !== undefined) return;
    S.joinCache[tableName] = [];
    try {
        const r = await fetch(`/api/table/${encodeURIComponent(tableName)}/possible-joins`);
        if (!r.ok) return;
        const d = await r.json();
        if (d.success && Array.isArray(d.data)) {
            S.joinCache[tableName] = d.data;
        }
    } catch(e) { /* ignore */ }
}

function _registerMonacoCompletion() {
    monaco.languages.registerCompletionItemProvider('sql', {
        triggerCharacters: ['.', ' '],
        provideCompletionItems: async function(model, position) {
            const word = model.getWordUntilPosition(position);
            const range = {
                startLineNumber: position.lineNumber, endLineNumber: position.lineNumber,
                startColumn: word.startColumn, endColumn: word.endColumn
            };

            const fullSql = model.getValue();

            // Dot-triggered column completion
            const lineText = model.getLineContent(position.lineNumber);
            const textBeforeWord = lineText.substring(0, word.startColumn - 1);
            const dotPrefixMatch = textBeforeWord.match(/(\w+)\.$/);

            if (dotPrefixMatch) {
                const prefix = dotPrefixMatch[1];
                const colPrefix = word.word.toLowerCase();
                const aliasMap = _buildAliasMap(fullSql);
                const rawName = aliasMap[prefix.toLowerCase()] || prefix;
                const tableName = rawName.includes('.') ? rawName.split('.').pop() : rawName;
                if (!S.colCache[tableName] && !S.colCache[tableName.toLowerCase()]) {
                    await loadTableColumns(tableName);
                }
                const cols = S.colCache[tableName] || S.colCache[tableName.toLowerCase()] || [];
                const types = S.typeCache[tableName] || S.typeCache[tableName.toLowerCase()] || {};
                return {
                    suggestions: cols
                        .filter(c => !colPrefix || c.toLowerCase().startsWith(colPrefix))
                        .map(c => ({
                            label: c,
                            kind: monaco.languages.CompletionItemKind.Field,
                            insertText: c,
                            range,
                            detail: types[c] ? `${tableName} · ${types[c]}` : tableName,
                        }))
                };
            }

            const textBeforeCursor = model.getValueInRange({
                startLineNumber: 1, startColumn: 1,
                endLineNumber: position.lineNumber, endColumn: position.column
            });

            // Context-aware: WHERE/AND/OR/HAVING/SET
            const contextMatch = textBeforeCursor.match(/\b(WHERE|AND|OR|HAVING|SET)\s+\w*$/i);
            if (contextMatch) {
                const aliasMap = _buildAliasMap(fullSql);
                const tablesInQuery = _extractTablesFromSql(fullSql);
                const allTables = [...new Set([
                    ...tablesInQuery,
                    ...Object.values(aliasMap).map(t => t.includes('.') ? t.split('.').pop() : t)
                ])];
                const suggestions = [ ..._getAliasDotStarSuggestions(aliasMap, word.word, range) ];
                for (const tbl of allTables) {
                    if (!S.colCache[tbl]) await loadTableColumns(tbl);
                    const cols = S.colCache[tbl] || [];
                    const types = S.typeCache[tbl] || {};
                    cols.filter(c => !word.word || c.toLowerCase().startsWith(word.word.toLowerCase()))
                        .forEach(c => suggestions.push({
                            label: c,
                            kind: monaco.languages.CompletionItemKind.Field,
                            insertText: c,
                            range,
                            detail: types[c] ? `${tbl} · ${types[c]}` : tbl,
                        }));
                }
                if (suggestions.length) return { suggestions };
            }

            // Context-aware: JOIN ... ON
            const joinOnMatch = textBeforeCursor.match(/\bJOIN\s+((?:\w+\.)?\w+)(?:\s+(?:AS\s+)?\w+)?\s+ON\s+[\w.]*$/i);
            if (joinOnMatch) {
                const rawJoinTable = joinOnMatch[1];
                const joinTableLc = (rawJoinTable.includes('.') ? rawJoinTable.split('.').pop() : rawJoinTable).toLowerCase();
                if (S.joinCache[joinTableLc] === undefined) {
                    await _loadPossibleJoins(joinTableLc);
                }
                const joinData = S.joinCache[joinTableLc] || [];
                const aliasMap = _buildAliasMap(fullSql);
                const tableToAlias = _buildTableToAlias(aliasMap);
                const tablesInQuery = _extractTablesFromSql(fullSql).map(t => t.toLowerCase());
                const suggestions = [];
                for (const targetInfo of joinData) {
                    if (!tablesInQuery.includes(targetInfo.table_name.toLowerCase())) continue;
                    for (const pj of (targetInfo.possible_joins || [])) {
                        if (pj.join_suggestion) {
                            const suggestion = Object.keys(tableToAlias).length
                                ? _applyAliasesToJoinSuggestion(pj.join_suggestion, tableToAlias)
                                : pj.join_suggestion;
                            suggestions.push({
                                label: suggestion,
                                kind: monaco.languages.CompletionItemKind.Snippet,
                                insertText: suggestion,
                                range,
                                detail: `${pj.join_type || 'JOIN'} · ${targetInfo.table_name}`,
                                documentation: { value: `Confidence: ${pj.match_confidence}` },
                            });
                        }
                    }
                }
                if (suggestions.length) return { suggestions };
            }

            // Default: keywords, functions, table names, alias.*
            const up = (word.word || '').toUpperCase();
            const suggestions = [];
            SQL_KW.filter(k => k.startsWith(up)).forEach(k => suggestions.push({
                label: k, kind: monaco.languages.CompletionItemKind.Keyword,
                insertText: k, range
            }));
            SQL_FN.filter(f => f.startsWith(up)).forEach(f => suggestions.push({
                label: f, kind: monaco.languages.CompletionItemKind.Function,
                insertText: f + '(', range
            }));
            (S.tables || []).filter(t => t.toLowerCase().startsWith(word.word.toLowerCase())).forEach(t => suggestions.push({
                label: t, kind: monaco.languages.CompletionItemKind.Class,
                insertText: t, range
            }));
            const defaultAliasMap = _buildAliasMap(fullSql);
            suggestions.push(..._getAliasDotStarSuggestions(defaultAliasMap, word.word, range));
            return { suggestions };
        }
    });
}

// ═══ MONACO EDITOR INIT ═══
function initMonacoEditor(tabId, containerEl) {
    if (!_monacoApiReady) return null;
    if (!containerEl) {
        containerEl = document.getElementById('monaco-' + tabId) ||
                      document.querySelector(`.monaco-editor-container[data-tab="${tabId}"]`);
    }
    if (!containerEl) return null;
    if (monacoEditors[tabId]) return monacoEditors[tabId];
    if (!containerEl.offsetParent && !document.body.contains(containerEl)) return null;

    const saved = loadTabContent(tabId);
    const defaultSql = tabId === 'tab-1'
        ? '-- SQL Редактор\n-- F9 / Ctrl+Enter — выполнить  |  Ctrl+Space — автодополнение\n-- Ctrl+Click по таблице или функции в редакторе — открыть определение\n\nSELECT version();'
        : '';
    const initialValue = saved !== null ? saved : defaultSql;

    const editor = monaco.editor.create(containerEl, {
        value: initialValue,
        language: 'sql',
        theme: _getCurrentMonacoTheme(),
        fontSize: 14,
        lineHeight: 22,
        tabSize: 4,
        minimap: { enabled: false },
        automaticLayout: true,
        scrollBeyondLastLine: false,
        renderWhitespace: 'none',
        fontFamily: "'Geist Mono', 'Consolas', 'Courier New', monospace",
        cursorStyle: 'line',
        wordWrap: 'off',
        scrollbar: { verticalScrollbarSize: 6, horizontalScrollbarSize: 6 },
        padding: { top: 8, bottom: 8 },
        quickSuggestions: { other: true, comments: false, strings: false },
    });

    monacoEditors[tabId] = editor;

    editor.addCommand(monaco.KeyCode.F9, () => runQuery());
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => runQuery());
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.Enter, () => runStatement());
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyK, () => formatSQL());
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
        formatSQL();
        showNotice('✓ Отформатировано и сохранено', 'ok');
    });

    editor.onDidChangeModelContent(() => {
        saveTabContent(tabId, editor.getValue());
        _scheduleMonacoDecorationUpdate();
        _scheduleLintMarkersUpdate(editor, tabId);
    });

    editor.onDidChangeCursorPosition(e => {
        if (S.currentEditorTab === tabId) {
            const cp = document.getElementById('cursorPos');
            if (cp) cp.textContent = `Ln ${e.position.lineNumber}, Col ${e.position.column}`;
        }
    });

    editor.onMouseDown(e => {
        if (!(e.event.ctrlKey || e.event.metaKey)) return;
        if (e.target.type !== monaco.editor.MouseTargetType.CONTENT_TEXT) return;
        const pos = e.target.position;
        if (!pos) return;
        const model = editor.getModel();
        if (!model) return;
        const wordInfo = model.getWordAtPosition(pos);
        if (!wordInfo) return;
        const name    = wordInfo.word;
        const nameLc  = name.toLowerCase();
        if ((S.tables || []).some(t => { const tl = t.toLowerCase(); return tl === nameLc || tl.split('.').pop() === nameLc; })) {
            e.event.preventDefault();
            openDetail(name);
            return;
        }
        if ((S._fnList  || []).some(f => f.toLowerCase() === nameLc)) {
            e.event.preventDefault();
            openRoutineDetail(name, 'function');
            return;
        }
        if ((S._procList|| []).some(p => p.toLowerCase() === nameLc)) {
            e.event.preventDefault();
            openRoutineDetail(name, 'procedure');
        }
    });

    requestAnimationFrame(() => _updateMonacoObjectDecorations(editor, tabId));
    return editor;
}

// ═══ MONACO LINT MARKERS ═══
let _lintMarkersTimer = null;
function _scheduleLintMarkersUpdate(editor, tabId) {
    clearTimeout(_lintMarkersTimer);
    _lintMarkersTimer = setTimeout(() => _updateMonacoLintMarkers(editor, tabId), 600);
}

function _updateMonacoLintMarkers(editor, tabId) {
    if (!editor || typeof monaco === 'undefined') return;
    const model = editor.getModel();
    if (!model) return;
    const sql = model.getValue();
    const warnings = (typeof lintQuery === 'function') ? lintQuery(sql) : [];
    const markers = [];

    if (warnings.length > 0) {
        const lines = sql.split('\n');
        const kwPatterns = {
            'UPDATE':   /\bUPDATE\b/i,
            'DELETE':   /\bDELETE\b/i,
            'TRUNCATE': /\bTRUNCATE\b/i,
            'DROP':     /\bDROP\b/i,
        };
        warnings.forEach(w => {
            let lineNum = 1;
            for (const [kw, re] of Object.entries(kwPatterns)) {
                if (w.msg.toUpperCase().includes(kw)) {
                    for (let i = 0; i < lines.length; i++) {
                        const stripped = lines[i].replace(/--[^\n]*/g, '');
                        if (re.test(stripped)) { lineNum = i + 1; break; }
                    }
                    break;
                }
            }
            const lineText = lines[lineNum - 1] || '';
            markers.push({
                severity: monaco.MarkerSeverity.Warning,
                message:  w.msg,
                startLineNumber: lineNum,
                startColumn: 1,
                endLineNumber: lineNum,
                endColumn: lineText.length + 1,
            });
        });
    }

    monaco.editor.setModelMarkers(model, 'sql-linter', markers);
    if (typeof showLintBar === 'function') {
        showLintBar(warnings, tabId);
    }
}


function _monacoAdapter(editor, tabId) {
    const adapter = {
        _isMonaco: true,
        _editor: editor,
        dataset: { tab: tabId },
        _pendingSelEnd: null,
        get value() { return editor.getValue(); },
        set value(v) { editor.setValue(typeof v === 'string' ? v : ''); },
        get selectionStart() {
            const model = editor.getModel(), sel = editor.getSelection();
            return (model && sel) ? model.getOffsetAt({ lineNumber: sel.startLineNumber, column: sel.startColumn }) : 0;
        },
        get selectionEnd() {
            const model = editor.getModel(), sel = editor.getSelection();
            return (model && sel) ? model.getOffsetAt({ lineNumber: sel.endLineNumber, column: sel.endColumn }) : 0;
        },
        set selectionEnd(offset) { this._pendingSelEnd = offset; },
        set selectionStart(offset) {
            const model = editor.getModel();
            if (!model) return;
            const endOff = (this._pendingSelEnd !== null && this._pendingSelEnd !== undefined)
                ? this._pendingSelEnd : offset;
            const sp = model.getPositionAt(offset);
            const ep = model.getPositionAt(endOff);
            editor.setSelection({ startLineNumber: sp.lineNumber, startColumn: sp.column,
                                   endLineNumber: ep.lineNumber,   endColumn: ep.column });
            this._pendingSelEnd = null;
        },
        focus() { editor.focus(); },
        closest() { return null; },
        getBoundingClientRect() {
            const d = editor.getContainerDomNode();
            return d ? d.getBoundingClientRect() : { top: 0, left: 0, right: 0, bottom: 0 };
        },
        get scrollTop() { return 0; },
        set scrollTop(v) { },
    };
    return adapter;
}

// ═══ HELPERS ═══
function getActiveTA() {
    const tabId = S.currentEditorTab;
    const editor = monacoEditors[tabId];
    if (editor) return _monacoAdapter(editor, tabId);
    return document.querySelector(`.sql-input[data-tab="${tabId}"]`) || document.getElementById('sqlInput');
}

function wireupTA(ta) {
    let _acTimer = null;
    ta.addEventListener('input', () => {
        syncNums(ta);
        updateHighlight(ta);
        saveTabContent(ta.dataset.tab, ta.value);
        clearTimeout(_acTimer);
        _acTimer = setTimeout(() => showAC(false, ta), 150);
    });
    ta.addEventListener('keydown', onKD);
    ta.addEventListener('scroll', () => syncScroll(ta));
    ta.addEventListener('click', () => updateCursor(ta));
    ta.addEventListener('keyup', () => updateCursor(ta));
}

function initEditor() {
    initMonacoEditor('tab-1');
}

function syncNums(ta) {
    if (!ta || ta._isMonaco) return;
    if (!ta.tagName) ta = document.getElementById('sqlInput');
    if (!ta) return;
    const ln = document.getElementById('lineNums') || ta.closest('.code-wrap')?.previousElementSibling;
    if (!ln) return;
    const lines = ta.value.split('\n').length;
    ln.textContent = Array.from({length: lines}, (_, i) => i + 1).join('\n');
    ln.scrollTop = ta.scrollTop;
}

function syncScroll(ta) {
    if (!ta) ta = document.getElementById('sqlInput');
    if (!ta || ta._isMonaco) return;
    const hl = ta.previousElementSibling;
    if (hl && hl.classList.contains('sql-hl')) {
        hl.scrollTop = ta.scrollTop;
        hl.scrollLeft = ta.scrollLeft;
    }
    const ln = document.getElementById('lineNums') || ta.closest('.code-wrap')?.previousElementSibling;
    if (ln) ln.scrollTop = ta.scrollTop;
}

function updateCursor(ta) {
    if (!ta || ta._isMonaco) return;
    const cp = document.getElementById('cursorPos');
    if (!cp) return;
    const text = ta.value.substring(0, ta.selectionStart);
    const lines = text.split('\n');
    cp.textContent = `Ln ${lines.length}, Col ${lines[lines.length - 1].length + 1}`;
}

// ── Key handler ──
function onKD(e) {
    const ta = e.target;
    if (document.getElementById('acMenu').style.display !== 'none') {
        if (e.key==='ArrowDown'){e.preventDefault();acMove(1);return;}
        if (e.key==='ArrowUp'){e.preventDefault();acMove(-1);return;}
        if (e.key==='Tab') {
            e.preventDefault();
            const idx = S.acIdx >= 0 ? S.acIdx : 0;
            if (S.acItems[idx]) { applyAC(S.acItems[idx], ta); return; }
        }
        if (e.key==='Enter'&&S.acIdx>=0){e.preventDefault();applyAC(S.acItems[S.acIdx], ta);return;}
        if (e.key==='Escape'){hideAC();return;}
    }
    if ((e.ctrlKey||e.metaKey)&&e.key==='Enter'){e.preventDefault();runQuery();return;}
    if ((e.ctrlKey||e.metaKey)&&e.shiftKey&&e.key==='Enter'){e.preventDefault();runStatement();return;}
    if (e.key==='F9'){e.preventDefault();runQuery();return;}
    if ((e.ctrlKey||e.metaKey)&&e.key===' '){e.preventDefault();showAC(true, ta);return;}
    if ((e.ctrlKey||e.metaKey)&&e.key==='k'){e.preventDefault();formatSQL();return;}
    if ((e.ctrlKey||e.metaKey)&&e.key==='/'){e.preventDefault();toggleLineComments(ta);return;}
    if (e.key==='Tab'){
        e.preventDefault();
        const s=ta.selectionStart;
        const end=ta.selectionEnd;
        ta.value=ta.value.substring(0,s)+'    '+ta.value.substring(end);
        ta.selectionStart=ta.selectionEnd=s+4;
        syncNums(ta);
        updateHighlight(ta);
    }
}

// ── Toggle line comments (Ctrl+/) ──
function toggleLineComments(ta) {
    const val = ta.value;
    const selStart = ta.selectionStart;
    const selEnd = ta.selectionEnd;
    const beforeStart = val.lastIndexOf('\n', selStart - 1);
    const lineStart = beforeStart + 1;
    const lineEnd = selEnd > selStart && val[selEnd - 1] === '\n' ? selEnd - 1 : selEnd;
    const afterEnd = val.indexOf('\n', lineEnd);
    const lastLineEnd = afterEnd === -1 ? val.length : afterEnd;
    const chunk = val.substring(lineStart, lastLineEnd);
    const lines = chunk.split('\n');
    const allCommented = lines.every(l => l.trimStart().startsWith('--'));
    const newLines = allCommented
        ? lines.map(l => l.replace(/^(\s*)--\s?/, '$1'))
        : lines.map(l => '-- ' + l);
    const newChunk = newLines.join('\n');
    ta.value = val.substring(0, lineStart) + newChunk + val.substring(lastLineEnd);
    const delta = newChunk.length - chunk.length;
    ta.selectionStart = selStart + (selStart > lineStart ? Math.min(delta, 0) : 0);
    ta.selectionEnd = selEnd + delta;
    syncNums(ta);
    updateHighlight(ta);
    saveTabContent(ta.dataset.tab, ta.value);
}

// ═══ TAB MANAGEMENT ═══
function addNewSQLTab() {
    S.tabCounter++;
    const tabId = 'tab-' + S.tabCounter;
    S.tabs[tabId] = { type: 'sql', title: 'SQL Запрос ' + S.tabCounter, data: {} };
    const tabEl = document.createElement('div');
    tabEl.className = 'tab';
    tabEl.dataset.tabId = tabId;
    tabEl.onclick = () => switchEditorTab(tabId);
    tabEl.innerHTML = `<i class="bi bi-code-slash"></i><span class="tab-title">${S.tabs[tabId].title}</span><button class="tab-close" onclick="closeTab(event, '${tabId}')">×</button>`;
    wireTabRename(tabEl, tabId);
    tabEl.addEventListener('mousedown', e => { if (e.button === 1) { e.preventDefault(); closeTab(e, tabId); } });
    document.getElementById('tabsList').appendChild(tabEl);
    const content = createSQLTabContent(tabId);
    document.getElementById('tabsContainer').appendChild(content);
    switchEditorTab(tabId);
    if (_monacoApiReady) {
        requestAnimationFrame(() => initMonacoEditor(tabId));
    }
}

function wireTabRename(tabEl, tabId) {
    const span = tabEl.querySelector('.tab-title');
    if (!span) return;
    span.addEventListener('dblclick', e => {
        e.stopPropagation();
        const input = document.createElement('input');
        input.value = span.textContent;
        input.style.cssText = 'background:transparent;border:none;border-bottom:1px solid var(--amber);outline:none;font:inherit;color:var(--amber);width:110px;padding:0;';
        span.replaceWith(input);
        input.focus(); input.select();
        const commit = () => {
            const newTitle = input.value.trim() || span.textContent;
            const newSpan = document.createElement('span');
            newSpan.className = 'tab-title';
            newSpan.textContent = newTitle;
            input.replaceWith(newSpan);
            if (S.tabs[tabId]) S.tabs[tabId].title = newTitle;
            wireTabRename(tabEl, tabId);
        };
        input.addEventListener('blur', commit);
        input.addEventListener('keydown', e2 => {
            if (e2.key === 'Enter') { e2.preventDefault(); commit(); }
            if (e2.key === 'Escape') { e2.preventDefault(); input.value = span.textContent; commit(); }
        });
    });
}

function createSQLTabContent(tabId) {
    const div = document.createElement('div');
    div.className = 'tab-content';
    div.dataset.tabId = tabId;
    div.innerHTML = `
        <div class="editor-toolbar" style="display:none;"></div>
        <div class="lint-bar" id="lintBar-${tabId}"></div>
        <div class="vSplit" style="flex:1;display:flex;flex-direction:column;min-height:0;min-width:0;max-width:100%;overflow:hidden;">
            <div class="editorPane" style="display:flex;flex-direction:column;height:40%;min-height:80px;flex-shrink:0;border-bottom:1px solid var(--line);">
                <div class="pane-hdr"><span><i class="bi bi-code-slash"></i>SQL запрос</span></div>
                <div class="monaco-editor-container" data-tab="${tabId}"></div>
            </div>
            <div class="hResizer"></div>
            <div class="resultsPane">
                <div class="res-hdr">
                    <div style="display:flex;align-items:center;gap:6px;min-width:0;flex:1;">
                        <button class="btn btn-primary btn-sm run-tab-btn" data-tab="${tabId}" onclick="runQuery()" title="Выполнить выделенное или всё (F9)" aria-label="Выполнить выделенное или всё (F9)"><i class="bi bi-play-fill"></i></button>
                        <div class="tb-div"></div>
                        <button class="btn btn-secondary btn-sm" onclick="showFindBar(false)" title="Найти (Ctrl+F)"><i class="bi bi-search"></i></button>
                        <button class="btn btn-secondary btn-sm" onclick="showCheatsheet()" title="Горячие клавиши (Ctrl+?)"><i class="bi bi-keyboard"></i></button>
                        <div class="tb-div"></div>
                        <button class="btn btn-secondary btn-sm" onclick="explainQuery(false)" title="EXPLAIN текущего запроса" aria-label="EXPLAIN текущего запроса"><i class="bi bi-search"></i></button>
                        <button class="btn btn-secondary btn-sm" onclick="explainQuery(true)" title="EXPLAIN ANALYZE текущего запроса" aria-label="EXPLAIN ANALYZE текущего запроса"><i class="bi bi-search-heart"></i></button>
                        <div class="tb-div"></div>
                        <span class="tb-lbl">Лимит:</span>
                        <select class="lim-sel" data-tab="${tabId}">
                            <option value="50">50</option><option value="100" selected>100</option>
                            <option value="500">500</option><option value="1000">1 000</option>
                            <option value="5000">5 000</option><option value="0">Всё</option>
                        </select>
                    </div>
                    <div style="display:flex;align-items:center;gap:8px;">
                        <div class="res-tabs">
                            <button class="rtab active" onclick="switchTab('data')">Данные</button>
                            <button class="rtab" onclick="switchTab('history')">История</button>
                        </div>
                        <div class="exp-strip export-strip" style="display:none;">
                            <button class="btn btn-secondary btn-sm" onclick="exportResult('csv')" title="CSV"><i class="bi bi-filetype-csv"></i></button>
                            <button class="btn btn-secondary btn-sm" onclick="exportResult('json')" title="JSON"><i class="bi bi-filetype-json"></i></button>
                            <button class="btn btn-secondary btn-sm" onclick="exportResult('xlsx')" title="Excel"><i class="bi bi-file-earmark-excel"></i></button>
                        </div>
                    </div>
                </div>
                <div class="rfbar" style="display:none;"></div>
                <div class="results-content" data-tab="${tabId}"><div class="empty-hint"><i class="bi bi-terminal"></i><span>Выполните запрос</span></div></div>
                <div class="stbar"><span class="stText">Готов</span><span class="st-d">|</span><span class="stTime"></span><span class="st-d">|</span><span class="stRows"></span></div>
            </div>
        </div>
    `;
    return div;
}

function switchEditorTab(tabId) {
    S.currentEditorTab = tabId;
    document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tabId === tabId));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.toggle('active', c.dataset.tabId === tabId));
    const editor = monacoEditors[tabId];
    if (editor) requestAnimationFrame(() => editor.layout());
    initSplitters();
}

function closeTab(event, tabId) {
    event.stopPropagation();
    if (tabId === 'tab-1') return;
    if (monacoEditors[tabId]) {
        monacoEditors[tabId].dispose();
        delete monacoEditors[tabId];
    }
    if (_monacoObjDecorations[tabId]) {
        delete _monacoObjDecorations[tabId];
    }
    delete S.tabs[tabId];
    document.querySelector(`.tab[data-tab-id="${tabId}"]`)?.remove();
    document.querySelector(`.tab-content[data-tab-id="${tabId}"]`)?.remove();
    if (S.currentEditorTab === tabId) switchEditorTab('tab-1');
}

async function openDetail(tableName) {
    S.tabCounter++;
    const tabId = 'detail-' + S.tabCounter;
    S.tabs[tabId] = { type: 'detail', title: tableName, tableName, activeSubTab: 'columns' };
    const tabEl = document.createElement('div');
    tabEl.className = 'tab';
    tabEl.dataset.tabId = tabId;
    tabEl.onclick = () => switchEditorTab(tabId);
    tabEl.innerHTML = `<i class="bi bi-table"></i><span>${tableName}</span><button class="tab-close" onclick="closeTab(event, '${tabId}')">×</button>`;
    document.getElementById('tabsList').appendChild(tabEl);
    const content = createDetailTabContent(tabId, tableName);
    document.getElementById('tabsContainer').appendChild(content);
    switchEditorTab(tabId);
    await loadDetailTabData(tabId, 'columns', tableName);
}

async function openRoutineDetail(name, routineType) {
    S.tabCounter++;
    const tabId = 'routine-' + S.tabCounter;
    const icon = routineType === 'function' ? 'bi-lightning' : 'bi-gear';
    S.tabs[tabId] = { type: 'routine', title: name, routineName: name, routineType, activeSubTab: 'ddl' };
    const tabEl = document.createElement('div');
    tabEl.className = 'tab';
    tabEl.dataset.tabId = tabId;
    tabEl.onclick = () => switchEditorTab(tabId);
    tabEl.innerHTML = `<i class="bi ${icon}"></i><span>${name}</span><button class="tab-close" onclick="closeTab(event, '${tabId}')">×</button>`;
    document.getElementById('tabsList').appendChild(tabEl);
    const content = createRoutineDetailTabContent(tabId, name, routineType);
    document.getElementById('tabsContainer').appendChild(content);
    switchEditorTab(tabId);
    await loadRoutineDetailData(tabId, 'ddl', name, routineType);
}
