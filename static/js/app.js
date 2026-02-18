class ReportBuilder {
    constructor(mainTable, templateId = null) {
        this.mainTable = mainTable;
        this.templateId = templateId ? parseInt(templateId, 10) : null;
        this.templateName = '';
        this.columns = {};
        this.joins = [];
        this.conditions = [];
        this.aggregates = [];
        this.sort = [];

        // Данные для сортировки/фильтрации результата
        this.currentData = null;
        this.masterRows = [];
        this.displayRows = [];
        this.sortColumn = null;
        this.sortDirection = 'asc';
        this.filterText = '';

        this.init(this.templateId);
    }

    async init(templateId) {

        if (templateId) {
            await this.loadTemplate(templateId);
        } else {
            await Promise.all([
                this.loadColumns([this.mainTable]),
                this.loadPossibleJoins()
            ]);
        }

        this.setupEventListeners();
        this.setupCollapsibles();
        this.adjustColumnHeights();
    }

    async loadTemplateSPA(id) {

        // очищаем текущее состояние
        this.clearSelectedColumns();
        this.joins = [];
        this.conditions = [];
        this.aggregates = [];
        this.sort = [];

        document.getElementById('conditionsContainer').innerHTML = '';
        document.getElementById('aggregatesContainer').innerHTML = '';
        document.getElementById('sortContainer').innerHTML = '';

        try {
            const response = await fetch(`/api/load-report/${id}`);
            const result = await response.json();

            if (!result.success) {
                this.showError(result.error);
                return;
            }

            this.templateId = parseInt(id, 10);
            this.templateName = result.data?.name || this.templateName || '';

            let config = result.data.config;

            if (typeof config === 'string') {
                config = JSON.parse(config);
            }

            // Если таблица отличается — переключаем
            if (config.main_table !== this.mainTable) {
                this.mainTable = config.main_table;
            }

            // обновляем URL без перезагрузки
            history.pushState(null, '', `?template_id=${id}`);

            // восстанавливаем состояние
            this.joins = config.joins || [];
            this.conditions = config.conditions || [];
            this.aggregates = config.aggregates || [];
            this.sort = config.sort || [];

            const allTables = [this.mainTable, ...this.joins.map(j => j.table_name)];
            const uniqueTables = [...new Set(allTables)];

            await Promise.all([
                this.loadColumns(uniqueTables),
                this.loadPossibleJoins()
            ]);

            // перенос выбранных колонок
            const selectedCols = config.columns || [];
            const availableSelect = document.getElementById('availableColumns');
            const selectedSelect = document.getElementById('selectedColumns');

            selectedCols.forEach(colName => {
                const option = Array.from(availableSelect.options)
                    .find(opt => opt.value === colName);
                if (option) selectedSelect.appendChild(option);
            });

            this.renderJoins();
            this.renderConditionsFromConfig();
            this.renderAggregatesFromConfig();
            this.renderSortFromConfig();

            document.getElementById('limit').value = config.limit || 100;
            document.getElementById('offset').value = config.offset || 0;

        } catch (error) {
            console.error(error);
            this.showError('Ошибка загрузки шаблона');
        }
    }

    async loadTemplate(id) {
        try {
            const response = await fetch(`/api/load-report/${id}`);
            const result = await response.json();

            if (result.success) {
                this.templateId = parseInt(id, 10);
                this.templateName = result.data?.name || this.templateName || '';

                let config = result.data.config;

                // ИСПРАВЛЕНИЕ: На случай, если бэкенд вернул строку, парсим её здесь тоже
                if (typeof config === 'string') {
                    config = JSON.parse(config);
                }
                
                // Проверка на валидность конфига
                if (!config) {
                    throw new Error('Конфигурация шаблона пуста или повреждена');
                }

                // 1. Восстанавливаем состояние
                this.joins = config.joins || [];
                this.conditions = config.conditions || [];
                this.aggregates = config.aggregates || [];
                this.sort = config.sort || [];

                // 2. Загружаем колонки для всех таблиц из шаблона
                const allTables = [this.mainTable, ...this.joins.map(j => j.table_name)];
                const uniqueTables = [...new Set(allTables)];
                await Promise.all([
                    this.loadColumns(uniqueTables),
                    this.loadPossibleJoins()
                ]); 

                // 3. Переносим выбранные колонки
                const selectedCols = config.columns || [];
                const availableSelect = document.getElementById('availableColumns');
                const selectedSelect = document.getElementById('selectedColumns');

                if (availableSelect && selectedSelect) {
                    selectedCols.forEach(colName => {
                        const option = Array.from(availableSelect.options).find(opt => opt.value === colName);
                        if (option) {
                            selectedSelect.appendChild(option);
                        }
                    });
                }

                // 4. Рендерим остальные блоки
                this.renderJoins();
                this.renderConditionsFromConfig();
                this.renderAggregatesFromConfig();
                this.renderSortFromConfig();

                // 5. Устанавливаем лимиты
                document.getElementById('limit').value = config.limit || 100;
                document.getElementById('offset').value = config.offset || 0;

                //this.showSuccess('Шаблон загружен!');
            } else {
                this.showError('Ошибка загрузки шаблона: ' + result.error);
                this.loadColumns([this.mainTable]);
                this.loadPossibleJoins();
            }
        } catch (error) {
            console.error(error);
            this.showError('Ошибка подключения при загрузке шаблона');
            this.loadColumns([this.mainTable]);
            this.loadPossibleJoins();
        }
    }

    getCurrentTemplateConfig() {
        return {
            main_table: this.mainTable,
            columns: this.getSelectedColumns(),
            joins: this.joins,
            conditions: this.conditions,
            aggregates: this.aggregates,
            sort: this.sort,
            limit: document.getElementById('limit')?.value,
            offset: document.getElementById('offset')?.value
        };
    }

    // Вспомогательный метод для рендеринга условий из конфига (без добавления в массив, так как массив уже заполнен)
    renderConditionsFromConfig() {
        const container = document.getElementById('conditionsContainer');
        if (!container) return;
        
        container.innerHTML = ''; // Очищаем заглушку
        const selectedColumns = this.getSelectedColumns();

        this.conditions.forEach((cond, index) => {
            const conditionDiv = document.createElement('div');
            conditionDiv.id = `condition-${index}`;
            conditionDiv.style.cssText = 'display: grid; grid-template-columns: 1fr 0.8fr 1fr 0.8fr auto; gap: 6px; margin-bottom: 8px; align-items: end;';
            
            // Генерируем HTML, выбирая текущее значение
            conditionDiv.innerHTML = `
                <select class="form-select condition-column" data-index="${index}" style="font-size: 11px;">
                    <option>-- Колонка --</option>
                    ${selectedColumns.map(c => `<option value="${c}" ${c === cond.column_name ? 'selected' : ''}>${c}</option>`).join('')}
                </select>
                <select class="form-select condition-operator" data-index="${index}" style="font-size: 11px;">
                    <option value="=" ${cond.operator === '=' ? 'selected' : ''}>=</option>
                    <option value="!=" ${cond.operator === '!=' ? 'selected' : ''}>!=</option>
                    <option value=">" ${cond.operator === '>' ? 'selected' : ''}>&gt;</option>
                    <option value="<" ${cond.operator === '<' ? 'selected' : ''}>&lt;</option>
                    <option value="LIKE" ${cond.operator === 'LIKE' ? 'selected' : ''}>LIKE</option>
                </select>
                <input type="text" class="form-control condition-value" data-index="${index}" placeholder="Значение" value="${cond.value || ''}" style="font-size: 11px;">
                <select class="form-select condition-logic" data-index="${index}" style="font-size: 11px;">
                    <option value="AND" ${cond.logic_operator === 'AND' ? 'selected' : ''}>AND</option>
                    <option value="OR" ${cond.logic_operator === 'OR' ? 'selected' : ''}>OR</option>
                </select>
                <button type="button" class="btn btn-sm btn-danger remove-condition" data-index="${index}" style="padding: 4px 6px; font-size: 10px;">
                    <i class="bi bi-trash"></i>
                </button>
            `;
            container.appendChild(conditionDiv);

            // Вешаем слушатели (те же, что в addCondition)
            conditionDiv.querySelector('.condition-column').addEventListener('change', (e) => {
                this.conditions[index].column_name = e.target.value;
            });
            conditionDiv.querySelector('.condition-operator').addEventListener('change', (e) => {
                this.conditions[index].operator = e.target.value;
            });
            conditionDiv.querySelector('.condition-value').addEventListener('input', (e) => {
                this.conditions[index].value = e.target.value;
            });
            conditionDiv.querySelector('.condition-logic').addEventListener('change', (e) => {
                this.conditions[index].logic_operator = e.target.value;
            });
            conditionDiv.querySelector('.remove-condition').addEventListener('click', (e) => {
                const idx = parseInt(e.currentTarget.dataset.index);
                document.getElementById(`condition-${idx}`).remove();
                this.conditions.splice(idx, 1);
            });
        });
    }

    // Вспомогательный метод для рендеринга агрегатов
    renderAggregatesFromConfig() {
        const container = document.getElementById('aggregatesContainer');
        if (!container) return;
        
        container.innerHTML = '';
        const selectedColumns = this.getSelectedColumns();

        this.aggregates.forEach((agg, index) => {
            const aggregateDiv = document.createElement('div');
            aggregateDiv.id = `aggregate-${index}`;
            aggregateDiv.style.cssText = 'display: grid; grid-template-columns: 0.8fr 1fr 1fr auto; gap: 6px; margin-bottom: 8px; align-items: end;';
            
            aggregateDiv.innerHTML = `
                <select class="form-select aggregate-function" data-index="${index}" style="font-size: 11px;">
                    <option value="SUM" ${agg.function_name === 'SUM' ? 'selected' : ''}>SUM</option>
                    <option value="COUNT" ${agg.function_name === 'COUNT' ? 'selected' : ''}>COUNT</option>
                    <option value="AVG" ${agg.function_name === 'AVG' ? 'selected' : ''}>AVG</option>
                    <option value="MIN" ${agg.function_name === 'MIN' ? 'selected' : ''}>MIN</option>
                    <option value="MAX" ${agg.function_name === 'MAX' ? 'selected' : ''}>MAX</option>
                </select>
                <select class="form-select aggregate-column" data-index="${index}" style="font-size: 11px;">
                    <option>-- Колонка --</option>
                    ${selectedColumns.map(c => `<option value="${c}" ${c === agg.column_name ? 'selected' : ''}>${c}</option>`).join('')}
                </select>
                <input type="text" class="form-control aggregate-alias" data-index="${index}" placeholder="Алиас" value="${agg.alias || ''}" style="font-size: 11px;">
                <button type="button" class="btn btn-sm btn-danger remove-aggregate" data-index="${index}" style="padding: 4px 6px; font-size: 10px;">
                    <i class="bi bi-trash"></i>
                </button>
            `;
            container.appendChild(aggregateDiv);

            aggregateDiv.querySelector('.aggregate-function').addEventListener('change', (e) => {
                this.aggregates[index].function_name = e.target.value;
            });
            aggregateDiv.querySelector('.aggregate-column').addEventListener('change', (e) => {
                this.aggregates[index].column_name = e.target.value;
            });
            aggregateDiv.querySelector('.aggregate-alias').addEventListener('input', (e) => {
                this.aggregates[index].alias = e.target.value;
            });
            aggregateDiv.querySelector('.remove-aggregate').addEventListener('click', (e) => {
                const idx = parseInt(e.currentTarget.dataset.index);
                document.getElementById(`aggregate-${idx}`).remove();
                this.aggregates.splice(idx, 1);
            });
        });
    }

    // Вспомогательный метод для рендеринга сортировки
    renderSortFromConfig() {
        const container = document.getElementById('sortContainer');
        if (!container) return;
        
        container.innerHTML = '';
        const selectedColumns = this.getSelectedColumns();

        this.sort.forEach((s, index) => {
            const sortDiv = document.createElement('div');
            sortDiv.id = `sort-${index}`;
            sortDiv.style.cssText = 'display: grid; grid-template-columns: 1fr 0.6fr auto; gap: 6px; margin-bottom: 8px; align-items: end;';
            
            sortDiv.innerHTML = `
                <select class="form-select sort-column" data-index="${index}" style="font-size: 11px;">
                    <option>-- Колонка --</option>
                    ${selectedColumns.map(c => `<option value="${c}" ${c === s.column_name ? 'selected' : ''}>${c}</option>`).join('')}
                </select>
                <select class="form-select sort-direction" data-index="${index}" style="font-size: 11px;">
                    <option value="ASC" ${s.direction === 'ASC' ? 'selected' : ''}>ASC</option>
                    <option value="DESC" ${s.direction === 'DESC' ? 'selected' : ''}>DESC</option>
                </select>
                <button type="button" class="btn btn-sm btn-danger remove-sort" data-index="${index}" style="padding: 4px 6px; font-size: 10px;">
                    <i class="bi bi-trash"></i>
                </button>
            `;
            container.appendChild(sortDiv);

            sortDiv.querySelector('.sort-column').addEventListener('change', (e) => {
                this.sort[index].column_name = e.target.value;
            });
            sortDiv.querySelector('.sort-direction').addEventListener('change', (e) => {
                this.sort[index].direction = e.target.value;
            });
            sortDiv.querySelector('.remove-sort').addEventListener('click', (e) => {
                const idx = parseInt(e.currentTarget.dataset.index);
                document.getElementById(`sort-${idx}`).remove();
                this.sort.splice(idx, 1);
            });
        });
    }

    async loadColumns(tables) {
        try {
            if (tables.length === 1) {
                const response = await fetch(`/api/table/${tables[0]}/columns`);
                const data = await response.json();

                if (data.success) {
                    // Исправление: заменяем весь объект columns, чтобы удалить колонки удаленных таблиц
                    this.columns = { [tables[0]]: data.data };
                    this.populateColumnsList();
                } else {
                    this.showError('Ошибка загрузки колонок: ' + data.error);
                }
            } else {
                const response = await fetch('/api/tables/columns', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ tables })
                });
                const data = await response.json();

                if (data.success) {
                    this.columns = data.data;
                    this.populateColumnsList();
                } else {
                    this.showError('Ошибка загрузки колонок: ' + data.error);
                }
            }
        } catch (error) {
            this.showError('Ошибка подключения: ' + error.message);
            console.error('Load columns error:', error);
        }
    }

    populateColumnsList() {
        const availableSelect = document.getElementById('availableColumns');
        if (!availableSelect) {
            console.warn('availableColumns select not found');
            return;
        }

        availableSelect.innerHTML = '';

        for (const [tableName, cols] of Object.entries(this.columns)) {
            // Получаем цвет для текущей таблицы
            const tableColor = this.getTableColor(tableName);

            cols.forEach(col => {
                let optionValue;
                let optionText;

                if (tableName === this.mainTable) {
                    optionValue = col.column_name;
                    optionText = `${col.column_name} (${col.data_type})`;
                } else {
                    const joinInfo = this.joins.find(j => j.table_name === tableName);
                    const alias = joinInfo ? (joinInfo.alias || joinInfo.table_name) : tableName;
                    
                    optionValue = `${alias}.${col.column_name}`;
                    optionText = `${alias}.${col.column_name} (${col.data_type})`;
                }

                const option = document.createElement('option');
                option.value = optionValue;
                option.textContent = optionText;
                
                // ИЗМЕНЕНО: Применяем бледный фон и темный текст для читаемости
                option.style.backgroundColor = tableColor;
                option.style.color = '#1e293b'; 

                if (col.column_comment) {
                    option.title = col.column_comment;
                }
                availableSelect.appendChild(option);
            });
        }

        this.setupDoubleClickMove();
    }

    setupDoubleClickMove() {
        const availableSelect = document.getElementById('availableColumns');
        const selectedSelect = document.getElementById('selectedColumns');

        if (availableSelect) {
            availableSelect.addEventListener('dblclick', (e) => {
                if (e.target.tagName === 'OPTION') {
                    selectedSelect.appendChild(e.target);
                }
            });
        }

        if (selectedSelect) {
            selectedSelect.addEventListener('dblclick', (e) => {
                if (e.target.tagName === 'OPTION') {
                    availableSelect.appendChild(e.target);
                }
            });
        }
    }

    setupDragAndDrop() {
        const availableSelect = document.getElementById('availableColumns');
        const selectedSelect = document.getElementById('selectedColumns');

        [availableSelect, selectedSelect].forEach(select => {
            if (!select) return;

            // Drag start
            select.addEventListener('dragstart', (e) => {
                if (e.target.tagName === 'OPTION') {
                    this.draggedOption = e.target;
                    e.target.classList.add('dragging');
                    e.dataTransfer.effectAllowed = 'move';
                }
            });

            // Drag end
            select.addEventListener('dragend', (e) => {
                if (this.draggedOption) {
                    this.draggedOption.classList.remove('dragging');
                }
            });

            // Drag over
            select.addEventListener('dragover', (e) => {
                e.preventDefault();
                e.dataTransfer.dropEffect = 'move';
                select.style.backgroundColor = 'rgba(59, 130, 246, 0.1)';
            });

            // Drag leave
            select.addEventListener('dragleave', (e) => {
                if (e.target === select) {
                    select.style.backgroundColor = '';
                }
            });

            // Drop
            select.addEventListener('drop', (e) => {
                e.preventDefault();
                select.style.backgroundColor = '';
                if (this.draggedOption && this.draggedOption.parentElement !== select) {
                    select.appendChild(this.draggedOption);
                    this.draggedOption = null;
                }
            });
        });
    }

    clearSelectedColumns() {
        const selected = document.getElementById('selectedColumns');
        const available = document.getElementById('availableColumns');
        
        if (!selected || !available) {
            console.warn('Не найдены селекты колонок');
            return;
        }

        // Переносим все опции обратно в available
        while (selected.options.length > 0) {
            available.appendChild(selected.options[0]);
        }

        // Можно показать уведомление (опционально)
        // this.showSuccess('Выбранные колонки очищены после добавления JOIN');
    }

    setupCollapsibles() {
        const sections = [
            { headerId: 'joinsHeader', containerId: 'joinsContainer' },
            { headerId: 'conditionsHeader', containerId: 'conditionsContainer' },
            { headerId: 'aggregatesHeader', containerId: 'aggregatesContainer' },
            { headerId: 'sortHeader', containerId: 'sortContainer' },
            { headerId: 'parametersHeader', containerId: 'parametersContainer' }
        ];

        sections.forEach(section => {
            const header = document.getElementById(section.headerId);
            const container = document.getElementById(section.containerId);

            if (header && container) {
                header.addEventListener('click', (e) => {
                    // ИСПРАВЛЕНИЕ: Если кликнули по кнопке внутри заголовка, не сворачиваем
                    if (e.target.closest('button')) return;

                    const isHidden = container.style.display === 'none';
                    container.style.display = isHidden ? 'block' : 'none';
                    header.classList.toggle('collapsed', !isHidden);
                });
            }
        });
    }

    adjustColumnHeights() {
        // Увеличиваем высоту списков колонок
        const availableSelect = document.getElementById('availableColumns');
        const selectedSelect = document.getElementById('selectedColumns');

        const newHeight = '300px'; // Желаемая высота

        if (availableSelect) {
            availableSelect.style.height = newHeight;
            availableSelect.style.minHeight = newHeight;
        }

        if (selectedSelect) {
            selectedSelect.style.height = newHeight;
            selectedSelect.style.minHeight = newHeight;
        }
    }



    async loadPossibleJoins() {
        const sourceTables = [this.mainTable, ...this.joins.map(j => j.table_name)];
        const uniqueSources = [...new Set(sourceTables)];

        // 1. Изменяем map, чтобы возвращать объект { source, joins }
        const promises = uniqueSources.map(async (sourceTable) => {
            try {
                const response = await fetch(`/api/table/${sourceTable}/possible-joins`);
                const data = await response.json();
                return { source: sourceTable, joins: data.success ? data.data : [] };
            } catch (error) {
                console.error(`Ошибка загрузки джойнов для таблицы ${sourceTable}:`, error);
                return { source: sourceTable, joins: [] };
            }
        });

        try {
            const results = await Promise.all(promises);
            const mergedJoins = {};
            const joinedTableNames = new Set(this.joins.map(j => j.table_name));

            // 2. Обрабатываем результаты, зная источник
            results.forEach(({ source, joins: tableJoins }) => {
                tableJoins.forEach(joinInfo => {
                    const targetTable = joinInfo.table_name;

                    if (targetTable === this.mainTable || joinedTableNames.has(targetTable)) {
                        return;
                    }

                    if (!mergedJoins[targetTable]) {
                        mergedJoins[targetTable] = {
                            table_name: targetTable,
                            schema: joinInfo.schema,
                            possible_joins: []
                        };
                    }

                    // 3. Добавляем source_table в каждый вариант джоина
                    joinInfo.possible_joins.forEach(pJoin => {
                        const isDuplicate = mergedJoins[targetTable].possible_joins.some(j => 
                            j.source_column === pJoin.source_column && 
                            j.target_column === pJoin.target_column
                        );
                        
                        if (!isDuplicate) {
                            mergedJoins[targetTable].possible_joins.push({
                                ...pJoin,
                                source_table: source // <-- ДОБАВЛЯЕМ ИСТОЧНИК
                            });
                        }
                    });
                });
            });

            this.populateJoinTables(Object.values(mergedJoins));

        } catch (error) {
            console.error('Ошибка обработки возможных джойнов:', error);
        }
    }

    populateJoinTables(joins) {
        const select = document.getElementById('joinTableSelect');
        if (!select) return;

        // Очищаем список перед заполнением
        select.innerHTML = '';

        // Добавляем опцию по умолчанию (плейсхолдер)
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = '-- Выберите таблицу --';
        select.appendChild(defaultOption);

        // Заполняем список таблиц
        joins.forEach(joinInfo => {
            const option = document.createElement('option');
            option.value = joinInfo.table_name;
            option.textContent = joinInfo.table_name;
            option.dataset.joins = JSON.stringify(joinInfo.possible_joins);
            select.appendChild(option);
        });
    }

        getTableColor(tableName) {
        // Палитра бледных (pastel) цветов для подсветки таблиц
        const colors = [
            '#e3f2fd', // Blue 50
            '#fce4ec', // Pink 50
            '#e8f5e9', // Green 50
            '#fff3e0', // Orange 50
            '#f3e5f5', // Purple 50
            '#e0f7fa', // Cyan 50
            '#fff9c4', // Yellow 50
            '#ffebee', // Red 50
            '#e8eaf6', // Indigo 50
            '#efebe9', // Brown 50
            '#f1f8e9', // Light Green 50
            '#e0f2f1'  // Teal 50
        ];

        // Простой хеш имени таблицы, чтобы цвет был постоянным для одной и той же таблицы
        let hash = 0;
        for (let i = 0; i < tableName.length; i++) {
            hash = tableName.charCodeAt(i) + ((hash << 5) - hash);
        }
        const index = Math.abs(hash) % colors.length;
        return colors[index];
    }

    setupEventListeners() {
        const addColumnsBtn = document.getElementById('addColumns');
        const removeColumnsBtn = document.getElementById('removeColumns');
        const addAllColumnsBtn = document.getElementById('addAllColumns');
        const removeAllColumnsBtn = document.getElementById('removeAllColumns');

        if (addColumnsBtn) addColumnsBtn.addEventListener('click', () => this.moveColumns('availableColumns', 'selectedColumns'));
        if (removeColumnsBtn) removeColumnsBtn.addEventListener('click', () => this.moveColumns('selectedColumns', 'availableColumns'));
        if (addAllColumnsBtn) addAllColumnsBtn.addEventListener('click', () => this.moveAllColumns('availableColumns', 'selectedColumns'));
        if (removeAllColumnsBtn) removeAllColumnsBtn.addEventListener('click', () => this.moveAllColumns('selectedColumns', 'availableColumns'));

        const joinTableSelect = document.getElementById('joinTableSelect');
        if (joinTableSelect) {
            joinTableSelect.addEventListener('change', (e) => {
                const table = e.target.value;
                if (table) {
                    const joinsJson = e.target.selectedOptions[0].dataset.joins;
                    const joins = JSON.parse(joinsJson || '[]');
                    this.populateJoinColumns(joins);
                }
            });
        }

        const addConditionBtn = document.getElementById('addConditionBtn');
        if (addConditionBtn) addConditionBtn.addEventListener('click', () => this.addCondition());

        const addAggregateBtn = document.getElementById('addAggregateBtn');
        if (addAggregateBtn) addAggregateBtn.addEventListener('click', () => this.addAggregate());

        const addSortBtn = document.getElementById('addSortBtn');
        if (addSortBtn) addSortBtn.addEventListener('click', () => this.addSort());

        const generateBtn = document.getElementById('generateBtn');
        if (generateBtn) generateBtn.addEventListener('click', () => this.generateReport());

        const resetBtn = document.getElementById('resetBtn');
        if (resetBtn) resetBtn.addEventListener('click', () => this.resetForm());

        // Export buttons
        document.querySelectorAll('.export-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const format = e.currentTarget.dataset.format;
                this.exportToFormat(format);
            });
        });
    }

    moveColumns(fromId, toId) {
        const fromSelect = document.getElementById(fromId);
        const toSelect = document.getElementById(toId);

        if (!fromSelect || !toSelect) return;

        const selected = Array.from(fromSelect.selectedOptions);
        selected.forEach(option => {
            toSelect.appendChild(option);
        });
    }

    moveAllColumns(fromId, toId) {
        const fromSelect = document.getElementById(fromId);
        const toSelect = document.getElementById(toId);

        if (!fromSelect || !toSelect) return;

        Array.from(fromSelect.options).forEach(option => {
            toSelect.appendChild(option.cloneNode(true));
        });
        fromSelect.innerHTML = '';
    }

    populateJoinColumns(possibleJoins) {
        const container = document.getElementById('joinColumnsContainer');
        if (!container) return;

        container.innerHTML = '';

        if (!possibleJoins || possibleJoins.length === 0) {
            container.textContent = 'Нет доступных колонок для джойна';
            return;
        }

        const label = document.createElement('label');
        label.className = 'form-label';
        label.textContent = 'Выберите колонки:';
        label.style.fontSize = '12px';
        container.appendChild(label);

        const select = document.createElement('select');
        select.id = 'joinColumnSelect';
        select.className = 'form-select';
        select.style.fontSize = '12px';

        possibleJoins.forEach((join) => {
            const option = document.createElement('option');
            option.textContent = `${join.source_column} = ${join.target_column}`;
            option.dataset.join = JSON.stringify(join);
            select.appendChild(option);
        });

        container.appendChild(select);
        container.style.marginBottom = '12px';
    }

    getConfidenceText(confidence) {
        if (confidence >= 1.0) return 'FK (100%)';
        if (confidence >= 0.9) return 'Обратный FK (95%)';
        if (confidence >= 0.8) return 'Совпадение (80%)';
        return 'Низкая (60%)';
    }

    getConfidenceClass(confidence) {
        if (confidence >= 0.9) return 'text-success';
        if (confidence >= 0.7) return 'text-warning';
        return 'text-danger';
    }

    async addJoin() {
        const joinTableSelect = document.getElementById('joinTableSelect');
        const joinType = document.getElementById('joinType');
        const joinAlias = document.getElementById('joinAlias');
        const joinColumnSelect = document.getElementById('joinColumnSelect');

        if (!joinTableSelect || !joinType || !joinColumnSelect) return;

        const table = joinTableSelect.value;
        const alias = joinAlias ? joinAlias.value : '';
        const joinIndex = joinColumnSelect.value;

        if (!table || !joinIndex) {
            this.showError('Выберите таблицу и колонки для джойна');
            return;
        }

        const joinData = JSON.parse(joinColumnSelect.selectedOptions[0].dataset.join);

        const join = {
            source_table: joinData.source_table, 
            table_name: table,
            alias: alias || table,
            join_type: joinType.value,
            left_column: joinData.source_column,
            right_column: joinData.target_column,
            confidence: joinData.match_confidence
        };

        this.joins.push(join);
        this.renderJoins();

        this.clearSelectedColumns();

        const allTables = [this.mainTable, ...this.joins.map(j => j.table_name)];
        const uniqueTables = [...new Set(allTables)];
        await this.loadColumns(uniqueTables);

        await this.loadPossibleJoins();

        if (joinTableSelect) joinTableSelect.value = '';
        if (joinAlias) joinAlias.value = '';
        const container = document.getElementById('joinColumnsContainer');
        if (container) container.innerHTML = '';
    }
    

renderJoins() {
        const container = document.getElementById('joinsContainer');
        if (!container) return;

        if (this.joins.length === 0) {
            container.innerHTML = 'Нет джойнов';
            return;
        }

        container.innerHTML = '';

        this.joins.forEach((join, index) => {
            const confidenceText = this.getConfidenceText(join.confidence);

            const item = document.createElement('div');
            item.style.cssText = 'background: white; border: 1px solid var(--border-color); border-radius: 6px; padding: 10px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: start; font-size: 11px; transition: all 0.3s ease;';
            
            item.innerHTML = `
                <div>
                    <strong>${join.join_type}</strong> ${join.table_name} ${join.alias ? `AS ${join.alias}` : ''}<br>
                    ${join.left_column} = ${join.right_column}<br>
                    <span style="color: #64748b;">${confidenceText}</span>
                </div>
                <button type="button" class="btn btn-sm btn-danger remove-join" data-index="${index}" style="padding: 4px 8px; margin-left: 10px;">
                    <i class="bi bi-trash"></i>
                </button>
            `;

            // Темная тема для джойна
            const applyDarkThemeToJoin = () => {
                if (document.body.classList.contains('dark-theme')) {
                    item.style.background = 'rgba(42, 63, 95, 0.6)';
                    item.style.borderColor = 'var(--dark-border)';
                    item.style.color = 'var(--dark-text)';
                }
            };
            applyDarkThemeToJoin();

            container.appendChild(item);
        });

        document.querySelectorAll('.remove-join').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                const index = parseInt(e.currentTarget.dataset.index);
                const removedJoin = this.joins[index];

                // 1. Удаляем колонки из списка выбранных (selectedColumns)
                const selectedSelect = document.getElementById('selectedColumns');
                if (selectedSelect) {
                    // Определяем префиксы для удаления: table_name и alias (если есть)
                    const prefixesToRemove = [
                        `${removedJoin.table_name}.`
                    ];
                    if (removedJoin.alias && removedJoin.alias !== removedJoin.table_name) {
                        prefixesToRemove.push(`${removedJoin.alias}.`);
                    }

                    // Проходим в обратном порядке, чтобы безопасно удалять элементы
                    for (let i = selectedSelect.options.length - 1; i >= 0; i--) {
                        const optionValue = selectedSelect.options[i].value;
                        const shouldRemove = prefixesToRemove.some(prefix => optionValue.startsWith(prefix));
                        
                        if (shouldRemove) {
                            selectedSelect.remove(i);
                        }
                    }
                }

                // 2. Удаляем джойн из массива и обновляем UI
                this.joins.splice(index, 1);
                this.renderJoins();

                // 3. Обновляем список доступных колонок для оставшихся таблиц
                const allTables = [this.mainTable, ...this.joins.map(j => j.table_name)];
                const uniqueTables = [...new Set(allTables)];
                await this.loadColumns(uniqueTables);

                await this.loadPossibleJoins();
            });
        });
    }

    addCondition() {
        const container = document.getElementById('conditionsContainer');
        if (!container) return;

        // ИСПРАВЛЕНИЕ: Удаляем пустую заглушку перед добавлением первого условия
        if (container.children.length === 1 && !container.children[0].id) {
            container.innerHTML = '';
        }

        const index = this.conditions.length;
        const selectedColumns = this.getSelectedColumns();

        const conditionDiv = document.createElement('div');
        conditionDiv.id = `condition-${index}`;
        conditionDiv.style.cssText = 'display: grid; grid-template-columns: 1fr 0.8fr 1fr 0.8fr auto; gap: 6px; margin-bottom: 8px; align-items: end;';
        conditionDiv.innerHTML = `
            <select class="form-select condition-column" data-index="${index}" style="font-size: 11px;">
                <option>-- Колонка --</option>
                ${selectedColumns.map(c => `<option value="${c}">${c}</option>`).join('')}
            </select>
            <select class="form-select condition-operator" data-index="${index}" style="font-size: 11px;">
                <option value="=">=</option>
                <option value="!=">!=</option>
                <option value=">">&gt;</option>
                <option value="<">&lt;</option>
                <option value="LIKE">LIKE</option>
            </select>
            <input type="text" class="form-control condition-value" data-index="${index}" placeholder="Значение" style="font-size: 11px;">
            <select class="form-select condition-logic" data-index="${index}" style="font-size: 11px;">
                <option value="AND">AND</option>
                <option value="OR">OR</option>
            </select>
            <button type="button" class="btn btn-sm btn-danger remove-condition" data-index="${index}" style="padding: 4px 6px; font-size: 10px;">
                <i class="bi bi-trash"></i>
            </button>
        `;

        container.appendChild(conditionDiv);

        this.conditions.push({
            column_name: '',
            operator: '=',
            value: '',
            logic_operator: 'AND'
        });

        conditionDiv.querySelector('.condition-column').addEventListener('change', (e) => {
            this.conditions[index].column_name = e.target.value;
        });

        conditionDiv.querySelector('.condition-operator').addEventListener('change', (e) => {
            this.conditions[index].operator = e.target.value;
        });

        conditionDiv.querySelector('.condition-value').addEventListener('input', (e) => {
            this.conditions[index].value = e.target.value;
        });

        conditionDiv.querySelector('.condition-logic').addEventListener('change', (e) => {
            this.conditions[index].logic_operator = e.target.value;
        });

        conditionDiv.querySelector('.remove-condition').addEventListener('click', (e) => {
            const idx = parseInt(e.currentTarget.dataset.index);
            document.getElementById(`condition-${idx}`).remove();
            this.conditions.splice(idx, 1);
        });
    }

    addAggregate() {
        const container = document.getElementById('aggregatesContainer');
        if (!container) return;

        // ИСПРАВЛЕНИЕ: Удаляем пустую заглушку перед добавлением первого агреганта
        if (container.children.length === 1 && !container.children[0].id) {
            container.innerHTML = '';
        }

        const index = this.aggregates.length;
        const selectedColumns = this.getSelectedColumns();

        const aggregateDiv = document.createElement('div');
        aggregateDiv.id = `aggregate-${index}`;
        aggregateDiv.style.cssText = 'display: grid; grid-template-columns: 0.8fr 1fr 1fr auto; gap: 6px; margin-bottom: 8px; align-items: end;';
        aggregateDiv.innerHTML = `
            <select class="form-select aggregate-function" data-index="${index}" style="font-size: 11px;">
                <option value="SUM">SUM</option>
                <option value="COUNT">COUNT</option>
                <option value="AVG">AVG</option>
                <option value="MIN">MIN</option>
                <option value="MAX">MAX</option>
            </select>
            <select class="form-select aggregate-column" data-index="${index}" style="font-size: 11px;">
                <option>-- Колонка --</option>
                ${selectedColumns.map(c => `<option value="${c}">${c}</option>`).join('')}
            </select>
            <input type="text" class="form-control aggregate-alias" data-index="${index}" placeholder="Алиас" style="font-size: 11px;">
            <button type="button" class="btn btn-sm btn-danger remove-aggregate" data-index="${index}" style="padding: 4px 6px; font-size: 10px;">
                <i class="bi bi-trash"></i>
            </button>
        `;

        container.appendChild(aggregateDiv);

        this.aggregates.push({
            function_name: 'SUM',
            column_name: '',
            alias: ''
        });

        aggregateDiv.querySelector('.aggregate-function').addEventListener('change', (e) => {
            this.aggregates[index].function_name = e.target.value;
        });

        aggregateDiv.querySelector('.aggregate-column').addEventListener('change', (e) => {
            this.aggregates[index].column_name = e.target.value;
        });

        aggregateDiv.querySelector('.aggregate-alias').addEventListener('input', (e) => {
            this.aggregates[index].alias = e.target.value;
        });

        aggregateDiv.querySelector('.remove-aggregate').addEventListener('click', (e) => {
            const idx = parseInt(e.currentTarget.dataset.index);
            document.getElementById(`aggregate-${idx}`).remove();
            this.aggregates.splice(idx, 1);
        });
    }

    addSort() {
        const container = document.getElementById('sortContainer');
        if (!container) return;

        // ИСПРАВЛЕНИЕ: Удаляем пустую заглушку перед добавлением первой сортировки
        if (container.children.length === 1 && !container.children[0].id) {
            container.innerHTML = '';
        }

        const index = this.sort.length;
        const selectedColumns = this.getSelectedColumns();

        const sortDiv = document.createElement('div');
        sortDiv.id = `sort-${index}`;
        sortDiv.style.cssText = 'display: grid; grid-template-columns: 1fr 0.6fr auto; gap: 6px; margin-bottom: 8px; align-items: end;';
        sortDiv.innerHTML = `
            <select class="form-select sort-column" data-index="${index}" style="font-size: 11px;">
                <option>-- Колонка --</option>
                ${selectedColumns.map(c => `<option value="${c}">${c}</option>`).join('')}
            </select>
            <select class="form-select sort-direction" data-index="${index}" style="font-size: 11px;">
                <option value="ASC">ASC</option>
                <option value="DESC">DESC</option>
            </select>
            <button type="button" class="btn btn-sm btn-danger remove-sort" data-index="${index}" style="padding: 4px 6px; font-size: 10px;">
                <i class="bi bi-trash"></i>
            </button>
        `;

        container.appendChild(sortDiv);

        this.sort.push({
            column_name: '',
            direction: 'ASC'
        });

        sortDiv.querySelector('.sort-column').addEventListener('change', (e) => {
            this.sort[index].column_name = e.target.value;
        });

        sortDiv.querySelector('.sort-direction').addEventListener('change', (e) => {
            this.sort[index].direction = e.target.value;
        });

        sortDiv.querySelector('.remove-sort').addEventListener('click', (e) => {
            const idx = parseInt(e.currentTarget.dataset.index);
            document.getElementById(`sort-${idx}`).remove();
            this.sort.splice(idx, 1);
        });
    }

    getSelectedColumns() {
        const selected = [];
        const selectedSelect = document.getElementById('selectedColumns');
        if (selectedSelect) {
            Array.from(selectedSelect.options).forEach(option => {
                selected.push(option.value);
            });
        }
        return selected;
    }

    async generateReport() {
        const selectedColumns = this.getSelectedColumns();

        if (selectedColumns.length === 0 && this.aggregates.length === 0) {
            this.showError('Выберите хотя бы одну колонку');
            return;
        }

        const data = {
            main_table: this.mainTable,
            columns: selectedColumns,
            joins: this.joins,
            conditions: this.conditions.filter(c => c.column_name && c.value),
            aggregates: this.aggregates.filter(a => a.column_name),
            group_by: this.aggregates.length > 0 ? selectedColumns : [],
            sort: this.sort.filter(s => s.column_name),
            limit: parseInt(document.getElementById('limit')?.value) || 100,
            offset: parseInt(document.getElementById('offset')?.value) || 0
        };

        const generateBtn = document.getElementById('generateBtn');
        if (generateBtn) {
            generateBtn.innerHTML = '<span class="loading"></span> Генерация...';
            generateBtn.disabled = true;
        }

        try {
            const response = await fetch('/api/generate-report', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            const result = await response.json();

            if (result.success) {
                this.showResult(result.data);
            } else {
                this.showError('Ошибка: ' + (result.error || 'Неизвестная ошибка'));
            }
        } catch (error) {
            this.showError('Ошибка подключения: ' + error.message);
        } finally {
            if (generateBtn) {
                generateBtn.innerHTML = '<i class="bi bi-play-fill"></i> Генерировать';
                generateBtn.disabled = false;
            }
        }
    }

    showResult(data) {
        const container = document.getElementById('resultContainer');
        if (!container) return;

        container.innerHTML = '';

        if (data.rows.length === 0) {
            container.textContent = 'Результаты не найдены';
            document.getElementById('resultModal').style.display = 'flex';
            return;
        }

        // Сохраняем данные
        this.currentData = data;
        this.masterRows = data.rows;
        this.sortColumn = null;
        this.sortDirection = 'asc';

        // Создаем таблицу (структура)
        const table = document.createElement('table');
        table.className = 'table result-table';
        table.style.tableLayout = 'fixed';
        table.style.width = '100%';
        table.id = 'reportResultTable'; 

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        
        this.currentData.columns.forEach((col, idx) => {
            const th = document.createElement('th');
            th.style.position = 'relative';
            th.style.verticalAlign = 'top'; // Контент прижат к верху
            th.style.cursor = 'default'; // Курсор по умолчанию, стрелка только на тексте
            th.style.minWidth = '10px';
            th.style.width = '150px';
            th.style.padding = '5px';
            th.style.overflow = 'hidden';

            // Контейнер для содержимого заголовка
            const contentDiv = document.createElement('div');
            contentDiv.style.display = 'flex';
            contentDiv.style.flexDirection = 'column';
            contentDiv.style.width = '100%';
            contentDiv.style.paddingRight = '10px'; // Отступ справа для зоны ресайза
            contentDiv.style.boxSizing = 'border-box';

            // 1. Заголовок колонки (для сортировки)
            const titleSpan = document.createElement('span');
            titleSpan.textContent = col;
            titleSpan.id = `th-text-${idx}`;
            titleSpan.style.cursor = 'pointer';
            titleSpan.style.userSelect = 'none';
            titleSpan.style.whiteSpace = 'nowrap';
            titleSpan.style.overflow = 'hidden';
            titleSpan.style.textOverflow = 'ellipsis';
            titleSpan.style.display = 'block';
            titleSpan.style.marginBottom = '4px';

            // Клик для сортировки
            titleSpan.addEventListener('click', (e) => {
                if (e.target.classList.contains('resizer-zone')) return;
                this.handleSort(idx);
            });
            contentDiv.appendChild(titleSpan);

            // 2. Поле фильтрации для колонки
            const filterInput = document.createElement('input');
            filterInput.type = 'text';
            filterInput.className = 'form-control th-filter-input';
            filterInput.dataset.colIndex = idx; // Связываем инпут с индексом колонки
            filterInput.placeholder = 'Фильтр...';
            filterInput.style.fontSize = '11px';
            filterInput.style.padding = '2px 4px';
            // Важно: padding-right, чтобы текст не налезал на зону ресайза
            filterInput.style.paddingRight = '15px'; 
            filterInput.style.boxSizing = 'border-box'; // Чтобы width 100% считался с padding
            filterInput.style.width = '100%'; // Растягивается на всю ширину th

            // Событие ввода для фильтрации
            filterInput.addEventListener('input', () => this.updateView());
            contentDiv.appendChild(filterInput);

            th.appendChild(contentDiv);
            headerRow.appendChild(th);
        });
        
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        table.appendChild(tbody);

        container.appendChild(table);

        // Инициализация ресайза
        this.makeTableColumnsResizable(table);

        // Первоначальная отрисовка данных
        this.updateView();
        
        document.getElementById('resultModal').style.display = 'flex';
    }

    updateView() {
        // 1. Собираем значения фильтров из всех инпутов
        const inputs = document.querySelectorAll('.th-filter-input');
        const activeFilters = {};
        
        inputs.forEach(input => {
            const val = input.value.trim().toLowerCase();
            if (val) {
                activeFilters[input.dataset.colIndex] = val;
            }
        });

        // 2. Фильтрация строк
        let rowsToSort = this.masterRows;
        
        if (Object.keys(activeFilters).length > 0) {
            rowsToSort = this.masterRows.filter(row => {
                // Проверяем строку по ВСЕМ активным фильтрам (логика И)
                // Если хотя бы один фильтр не совпадает, строка исключается
                for (const [colIdxStr, filterText] of Object.entries(activeFilters)) {
                    const colIdx = parseInt(colIdxStr);
                    const colName = this.currentData.columns[colIdx];
                    const cellValue = row[colName];

                    // Проверка на null/undefined
                    if (cellValue == null) return false;

                    // Проверка вхождения текста
                    if (!String(cellValue).toLowerCase().includes(filterText)) {
                        return false;
                    }
                }
                return true; // Все фильтры прошли
            });
        }

        // 3. Сортировка (если включена)
        if (this.sortColumn !== null) {
            const colName = this.currentData.columns[this.sortColumn];
            const dir = this.sortDirection === 'asc' ? 1 : -1;

            rowsToSort.sort((a, b) => {
                const valA = a[colName];
                const valB = b[colName];

                if (valA == null) return 1 * dir;
                if (valB == null) return -1 * dir;

                const strA = String(valA).toLowerCase();
                const strB = String(valB).toLowerCase();

                if (strA < strB) return -1 * dir;
                if (strA > strB) return 1 * dir;
                return 0;
            });
        }

        this.displayRows = rowsToSort;
        this.renderTableBody();
        this.updateSortIcons();
    }

    renderTableBody() {
        const table = document.getElementById('reportResultTable');
        if (!table) return;
        
        const tbody = table.querySelector('tbody');
        tbody.innerHTML = '';

        if (this.displayRows.length === 0) {
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.colSpan = this.currentData.columns.length;
            td.textContent = 'Нет записей, соответствующих фильтру';
            td.style.textAlign = 'center';
            td.style.padding = '20px';
            td.style.color = '#6c757d';
            tr.appendChild(td);
            tbody.appendChild(tr);
            return;
        }

        this.displayRows.forEach(row => {
            const tr = document.createElement('tr');
            this.currentData.columns.forEach(col => {
                const td = document.createElement('td');
                td.textContent = row[col] !== null && row[col] !== undefined ? row[col] : '';
                td.style.overflow = 'hidden';
                td.style.textOverflow = 'ellipsis';
                td.style.whiteSpace = 'nowrap';
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
    }

    updateSortIcons() {
        // Сбрасываем иконки
        this.currentData.columns.forEach((_, idx) => {
            const span = document.getElementById(`th-text-${idx}`);
            if (span) {
                // Убираем старую иконку, если есть
                let text = span.textContent.replace(' ▲', '').replace(' ▼', '');
                span.textContent = text;
            }
        });

        // Устанавливаем текущую
        if (this.sortColumn !== null) {
            const span = document.getElementById(`th-text-${this.sortColumn}`);
            if (span) {
                span.textContent += this.sortDirection === 'asc' ? ' ▲' : ' ▼';
            }
        }
    }

    handleSort(columnIndex) {
        if (this.sortColumn === columnIndex) {
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortColumn = columnIndex;
            this.sortDirection = 'asc';
        }
        this.updateView();
    }

    makeTableColumnsResizable(table) {
        const headers = table.querySelectorAll('thead th');
        
        headers.forEach((header, idx) => {
            let isResizing = false;
            let startX = 0;
            let startWidth = 0;

            const resizerZone = document.createElement('div');
            resizerZone.className = 'resizer-zone';
            resizerZone.style.cssText = `
                position: absolute;
                right: -5px;
                top: 0;
                width: 10px;
                height: 100%;
                cursor: col-resize;
                z-index: 100;
            `;

            resizerZone.addEventListener('mouseenter', () => {
                resizerZone.style.background = 'rgba(255, 255, 255, 0.7)';
            });

            resizerZone.addEventListener('mouseleave', () => {
                resizerZone.style.background = 'transparent';
            });

            resizerZone.addEventListener('mousedown', (e) => {
                isResizing = true;
                startX = e.clientX;
                startWidth = header.offsetWidth;
                e.preventDefault();

                const onMouseMove = (e) => {
                    if (!isResizing) return;
                    
                    const diff = e.clientX - startX;
                    const newWidth = startWidth + diff;
                    
                    // Меняем ширину TH
                    header.style.width = newWidth + 'px';
                    header.style.minWidth = '10px'; 

                    // Важно: так как input внутри имеет width: 100%, 
                    // он автоматически подстроится под новую ширину header.
                    // Дополнительно менять ширину input не нужно.

                    // Меняем ширину ячеек в tbody (для визуального выравнивания границ)
                    const colIdx = Array.from(headers).indexOf(header);
                    const cells = table.querySelectorAll(`tbody td:nth-child(${colIdx + 1})`);
                    cells.forEach(cell => {
                        cell.style.width = newWidth + 'px';
                        cell.style.minWidth = '10px';
                    });
                };

                const onMouseUp = () => {
                    isResizing = false;
                    document.removeEventListener('mousemove', onMouseMove);
                    document.removeEventListener('mouseup', onMouseUp);
                };

                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            });

            header.appendChild(resizerZone);
        });
    }

    exportToFormat(format) {
        if (!this.currentData || !Array.isArray(this.currentData.columns)) {
            this.showError('Нет данных для экспорта');
            return;
        }

        // Экспортируем текущий вид (с учетом фильтра/сортировки)
        const columns = this.currentData.columns;
        const dataRows = Array.isArray(this.displayRows) ? this.displayRows : this.masterRows;
        const rows = dataRows.map(r => columns.map(c => r?.[c] ?? ''));

        switch(format) {
            case 'csv':
                this.exportToCSV(columns, rows);
                break;
            case 'json':
                this.exportToJSON(columns, rows);
                break;
            case 'xlsx':
                this.exportToXLSX(columns, rows);
                break;
            case 'xlsx_styled':
                this.exportToStyledXLSX(columns, rows);
                break;
            case 'xml':
                this.exportToXML(columns, rows);
                break;
            case 'html':
                this.exportToHTML(columns, rows);
                break;
        }
    }

    exportToCSV(columns, rows) {
        let csv = [];
        
        csv.push('\uFEFF' + columns.map(col => this.escapeCSV(col)).join(','));
        
        rows.forEach(row => {
            csv.push(row.map(cell => this.escapeCSV(cell)).join(','));
        });

        const blob = new Blob([csv.join('\n')], { type: 'text/csv;charset=utf-8;' });
        this.downloadFile(blob, 'report.csv');
    }

    escapeCSV(value) {
        if (value === null || value === undefined) return '';
        const str = String(value);
        if (str.includes(',') || str.includes('\n') || str.includes('"')) {
            return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
    }

    exportToJSON(columns, rows) {
        const data = rows.map(row => {
            const obj = {};
            columns.forEach((col, idx) => {
                obj[col] = row[idx];
            });
            return obj;
        });

        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json;charset=utf-8;' });
        this.downloadFile(blob, 'report.json');
    }

    exportToXML(columns, rows) {
        let xml = '<?xml version="1.0" encoding="UTF-8"?>\n';
        xml += '<report>\n';
        
        rows.forEach(row => {
            xml += '  <row>\n';
            columns.forEach((col, idx) => {
                const tagName = col.replace(/[^a-zA-Z0-9_]/g, '_');
                const value = this.escapeXML(row[idx]);
                xml += `    <${tagName}>${value}</${tagName}>\n`;
            });
            xml += '  </row>\n';
        });
        
        xml += '</report>';

        const blob = new Blob([xml], { type: 'application/xml;charset=utf-8;' });
        this.downloadFile(blob, 'report.xml');
    }

    escapeXML(value) {
        if (value === null || value === undefined) return '';
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;');
    }

    exportToHTML(columns, rows) {
        let html = `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #fffaf5; }
        table { border-collapse: collapse; width: 100%; background: white; }
        th { background: #2563eb; color: white; padding: 12px; text-align: left; border: 1px solid #e2e8f0; }
        td { padding: 10px; border: 1px solid #e2e8f0; }
        tr:hover { background: rgba(59, 130, 246, 0.03); }
    </style>
</head>
<body>
    <h1>Report</h1>
    <table>
        <thead>
            <tr>
                ${columns.map(col => `<th>${this.escapeHTML(col)}</th>`).join('')}
            </tr>
        </thead>
        <tbody>
            ${rows.map(row => `
            <tr>
                ${row.map(cell => `<td>${this.escapeHTML(cell)}</td>`).join('')}
            </tr>
            `).join('')}
        </tbody>
    </table>
</body>
</html>`;

        const blob = new Blob([html], { type: 'text/html;charset=utf-8;' });
        this.downloadFile(blob, 'report.html');
    }

    escapeHTML(value) {
        if (value === null || value === undefined) return '';
        const div = document.createElement('div');
        div.textContent = value;
        return div.innerHTML;
    }

    exportToXLSX(columns, rows) {
        if (typeof XLSX === 'undefined') {
            this.showError('Библиотека XLSX не подключена');
            return;
        }

        const aoa = [columns, ...rows];
        const ws = XLSX.utils.aoa_to_sheet(aoa);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, 'Отчет');

        XLSX.writeFile(wb, 'report.xlsx');
    }

    async exportToStyledXLSX(columns, rows) {
        try {
            const response = await fetch('/api/export/styled-excel', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    columns: columns,
                    rows: rows,
                    sheet_name: 'Отчет',
                    title: 'Отчет ' + new Date().toLocaleDateString('ru-RU')
                })
            });
            
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.error || 'Export failed');
            }
            
            // Handle blob response
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'report_styled_' + new Date().toISOString().slice(0,10) + '.xlsx';
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();
            
        } catch (e) {
            this.showError('Ошибка экспорта: ' + e.message);
        }
    }

    downloadFile(blob, filename) {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    async saveTemplate() {
        const templateNameInput = document.getElementById('templateName');
        if (!templateNameInput) return;

        const name = templateNameInput.value.trim();
        if (!name) {
            this.showError('Введите название шаблона');
            return;
        }

        const data = {
            name: name,
            config: this.getCurrentTemplateConfig()
        };

        try {
            const response = await fetch('/api/save-report', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            const result = await response.json();

            if (result.success) {
                document.getElementById('saveTemplateModal').style.display = 'none';
                templateNameInput.value = '';
                this.templateId = result.report_id ? parseInt(result.report_id, 10) : this.templateId;
                this.templateName = name;
                if (this.templateId) {
                    history.pushState(null, '', `?template_id=${this.templateId}`);
                }
                this.showSuccess('Шаблон сохранен!');
            } else {
                this.showError('Ошибка: ' + result.error);
            }
        } catch (error) {
            this.showError('Ошибка: ' + error.message);
        }
    }

    async updateTemplate() {
        if (!this.templateId) {
            this.showError('Шаблон не выбран. Используйте "Сохранить как новый".');
            return;
        }

        const templateNameInput = document.getElementById('templateName');
        const name = templateNameInput ? templateNameInput.value.trim() : '';
        if (!name) {
            this.showError('Введите название шаблона');
            return;
        }

        const data = {
            name: name,
            config: this.getCurrentTemplateConfig()
        };

        try {
            const response = await fetch(`/api/templates/${this.templateId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            const result = await response.json();

            if (result.success) {
                document.getElementById('saveTemplateModal').style.display = 'none';
                this.templateName = name;
                this.showSuccess('Шаблон обновлен!');
            } else {
                this.showError('Ошибка: ' + (result.error || 'Не удалось обновить шаблон'));
            }
        } catch (error) {
            this.showError('Ошибка: ' + error.message);
        }
    }

    async resetForm() {
        if (confirm('Сбросить все?')) {
            const availableSelect = document.getElementById('availableColumns');
            const selectedSelect = document.getElementById('selectedColumns');

            if (selectedSelect && availableSelect) {
                Array.from(selectedSelect.options).forEach(option => {
                    availableSelect.appendChild(option);
                });
            }

            this.joins = [];
            this.renderJoins();

            document.getElementById('conditionsContainer').innerHTML = '';
            this.conditions = [];

            document.getElementById('aggregatesContainer').innerHTML = '';
            this.aggregates = [];

            document.getElementById('sortContainer').innerHTML = '';
            this.sort = [];

            document.getElementById('limit').value = '100';
            document.getElementById('offset').value = '0';

            this.showSuccess('Сброшено!');
        }
    }

    showError(message) {
        alert('Ошибка: ' + message);
    }

    showSuccess(message) {
        alert('✓ ' + message);
    }

    // ==================== CHART FUNCTIONALITY ====================
    chartInstance = null;
    currentChartData = null;

    openChartModal() {
        const modal = document.getElementById('chartModal');
        const xAxisSelect = document.getElementById('xAxisColumn');
        const yAxisSelect = document.getElementById('yAxisColumn');
        
        // Заполняемselect'ы колонками
        const columns = this.getSelectedColumns();
        xAxisSelect.innerHTML = '';
        yAxisSelect.innerHTML = '<option value="">— COUNT по умолчанию —</option>';
        
        columns.forEach(col => {
            xAxisSelect.add(new Option(col, col));
            yAxisSelect.add(new Option(col, col));
        });
        
        modal.style.display = 'flex';
    }

    async showChart() {
        const modal = document.getElementById('chartModal');
        modal.style.display = 'none';
        
        const xAxis = document.getElementById('xAxisColumn').value;
        const yAxis = document.getElementById('yAxisColumn').value;
        const aggregateFunc = document.getElementById('aggregateFunction').value;
        const chartLimit = document.getElementById('chartLimit').value;
        const chartType = document.getElementById('modalChartType').value;
        
        if (!xAxis) {
            this.showError('Выберите колонку для оси X');
            return;
        }
        
        // Переключаем на вид графика
        document.querySelectorAll('.view-toggle-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector('[data-view="chart"]').classList.add('active');
        
        document.getElementById('resultTableWrapper').style.display = 'none';
        document.getElementById('resultChartWrapper').style.display = 'block';
        
        // Показываем результат
        document.getElementById('resultModal').style.display = 'flex';
        
        // Загружаем данные
        await this.fetchAndRenderChart(xAxis, yAxis, aggregateFunc, chartLimit, chartType);
    }

    async fetchAndRenderChart(xAxis, yAxis, aggregateFunc, limit, chartType) {
        const container = document.getElementById('resultChartWrapper');
        container.innerHTML = '<div class="chart-loading">Загрузка данных...</div>';
        
        try {
            const response = await fetch('/api/chart-data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    main_table: this.mainTable,
                    x_axis: xAxis,
                    y_axis: yAxis,
                    aggregate_function: aggregateFunc,
                    joins: this.joins,
                    conditions: this.conditions,
                    limit: parseInt(limit)
                })
            });
            
            const result = await response.json();
            
            if (!result.success) {
                throw new Error(result.error);
            }
            
            this.currentChartData = result.data;
            this.renderChart(chartType);
            
        } catch (err) {
            container.innerHTML = '<div class="empty-state" style="color:var(--red);">Ошибка: ' + err.message + '</div>';
        }
    }

    renderChart(chartType = 'bar') {
        const container = document.getElementById('resultChartWrapper');
        
        // Очищаем контейнер
        container.innerHTML = `
            <div style="margin-bottom:15px;display:flex;gap:10px;align-items:center;">
                <select id="chartType" class="form-select" style="width:auto;" onchange="window.reportBuilder.renderChart(this.value)">
                    <option value="bar" ${chartType === 'bar' ? 'selected' : ''}>Гистограмма</option>
                    <option value="line" ${chartType === 'line' ? 'selected' : ''}>График</option>
                    <option value="pie" ${chartType === 'pie' ? 'selected' : ''}>Круговая</option>
                    <option value="doughnut" ${chartType === 'doughnut' ? 'selected' : ''}>Пончик</option>
                    <option value="polarArea" ${chartType === 'polarArea' ? 'selected' : ''}>Полярная</option>
                </select>
                <button onclick="window.reportBuilder.fetchAndRenderChart(
                    document.getElementById('xAxisColumn').value,
                    document.getElementById('yAxisColumn').value,
                    document.getElementById('aggregateFunction').value,
                    document.getElementById('chartLimit').value,
                    document.getElementById('chartType').value
                )" class="btn btn-primary btn-sm"><i class="bi bi-arrow-repeat"></i> Обновить</button>
            </div>
            <div style="position:relative;height:400px;">
                <canvas id="resultChart"></canvas>
            </div>
        `;
        
        if (!this.currentChartData || this.currentChartData.length === 0) {
            container.innerHTML += '<div class="empty-state">Нет данных для графика</div>';
            return;
        }
        
        const ctx = document.getElementById('resultChart').getContext('2d');
        
        // Уничтожаем старый график
        if (this.chartInstance) {
            this.chartInstance.destroy();
        }
        
        const labels = this.currentChartData.map(d => d.label);
        const values = this.currentChartData.map(d => d.value);
        
        // Цветовая схема
        const colors = [
            '#f0a843', '#5b8dee', '#52b788', '#e05252', '#9b59b6', 
            '#e67e22', '#1abc9c', '#f39c12', '#3498db', '#2ecc71'
        ];
        
        const bgColors = chartType === 'line' 
            ? 'rgba(240, 168, 67, 0.1)' 
            : colors.map(c => c + 'cc');
        const borderColors = chartType === 'line' 
            ? '#f0a843' 
            : colors;
        
        this.chartInstance = new Chart(ctx, {
            type: chartType,
            data: {
                labels: labels,
                datasets: [{
                    label: 'Значения',
                    data: values,
                    backgroundColor: bgColors,
                    borderColor: borderColors,
                    borderWidth: chartType === 'line' ? 2 : 1,
                    fill: chartType === 'line',
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: chartType === 'pie' || chartType === 'doughnut' || chartType === 'polarArea',
                        position: 'right',
                        labels: { color: '#e8e3dc' }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.parsed.y !== null ? context.parsed.y : '';
                            }
                        }
                    }
                },
                scales: chartType === 'pie' || chartType === 'doughnut' || chartType === 'polarArea' ? {} : {
                    y: {
                        beginAtZero: true,
                        ticks: { color: '#a09890' },
                        grid: { color: '#2a2a2a' }
                    },
                    x: {
                        ticks: { color: '#a09890' },
                        grid: { color: '#2a2a2a' }
                    }
                }
            }
        });
    }

    toggleView(view) {
        if (view === 'table') {
            document.getElementById('resultTableWrapper').style.display = 'block';
            document.getElementById('resultChartWrapper').style.display = 'none';
        } else {
            // Открываем модальное окно настройки графика
            this.openChartModal();
        }
    }
}