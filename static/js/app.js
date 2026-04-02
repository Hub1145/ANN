const socket = io();

let currentLang = 'en-US';
let currentConfig = null;
let isBotRunning = false;
let activeAccountIdx = 0;
let activeSymbol = null;
let maxLeverages = {};
let currentBalance = 0; // Total equity for hero

document.addEventListener('DOMContentLoaded', () => {
    // Load translations from hidden div
    const transData = document.getElementById('translations-data');
    if (transData) {
        window.allTranslations = JSON.parse(transData.textContent);
    }
    setupEventListeners();
    setupSocketListeners();
    
    // Set initial content then initialize popovers
    updateHelpTooltips();
    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        const popover = new bootstrap.Popover(popoverTriggerEl, {
            trigger: 'click',
            placement: 'top',
            container: 'body',
            html: true
        });

        popoverTriggerEl.addEventListener('shown.bs.popover', () => {
            setTimeout(() => {
                popover.hide();
            }, 5000);
        });
        return popover;
    });
    

    // Initial config load via websocket
    socket.emit('get_config');
});

function setupEventListeners() {
    // ... existing ...
    document.getElementById('lang-select').addEventListener('change', (e) => {
        currentLang = e.target.value;
        saveLiveConfig({ language: currentLang });
        applyUiTranslations();
    });

    document.getElementById('addNewSymbolBtn').addEventListener('click', async () => {
        const inputStr = document.getElementById('newSymbolInput').value;
        const symbol = inputStr ? inputStr.trim().toUpperCase() : '';
        if (symbol && symbol.length > 3) {
            if (!currentConfig.symbols.includes(symbol)) {
                currentConfig.symbols.push(symbol);
                // Initialize default strategy with the 8-step TP ladder
                let newStrat = {
                    strategy_type: 'SMART_TRADE',
                    tp_enabled: true,
                    consolidated_reentry: true,
                    direction: 'LONG',
                    entry_type: 'LIMIT',
                    leverage: currentConfig.symbols[0] ? (currentConfig.symbol_strategies[currentConfig.symbols[0]].leverage || 20) : 20,
                    margin_type: 'CROSSED',
                    tp_targets: Array.from({ length: 8 }, (_, i) => ({
                        percent: ((i + 1) * 0.6).toFixed(1),
                        volume: 12.5
                    })),
                    entry_price: 0,
                    trade_amount_usdc: 10,
                    stop_loss_enabled: false,
                    stop_loss_price: 0,
                    trailing_enabled: true,
                    trailing_deviation: 0.5,
                    use_existing_assets: false
                };

                currentConfig.symbol_strategies[symbol] = newStrat;
                await saveLiveConfig();
                initSymbolPicker();
                renderSymbolsInModal();
                document.getElementById('newSymbolInput').value = '';
                document.getElementById('selectActiveSymbol').value = symbol;
                activeSymbol = symbol;
                updateUIFromConfig();
            }
        }
    });


    // ... balance % buttons ...
    document.getElementById('startStopBtn').addEventListener('click', () => {
        const btn = document.getElementById('startStopBtn');
        const spinner = document.getElementById('startStopSpinner');
        btn.disabled = true;
        if (spinner) spinner.classList.remove('d-none');

        if (isBotRunning) socket.emit('stop_bot');
        else socket.emit('start_bot');
    });

    // Asset Switcher
    document.getElementById('selectActiveSymbol').addEventListener('change', (e) => {
        activeSymbol = e.target.value;
        const unit = 'USDC';
        const labels = ['asset-symbol-label', 'base-asset-label', 'entry-price-asset-label', 'tp-price-asset-label', 'sl-price-asset-label', 'sl-order-price-asset-label'];
        labels.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerText = unit;
        });

        // Clear stale price display immediately
        document.getElementById('bid-price').innerText = '0.00';
        document.getElementById('ask-price').innerText = '0.00';

        updateUIFromConfig();
    });

    // Balance % Buttons
    document.querySelectorAll('#pct-buttons button').forEach(btn => {
        btn.addEventListener('click', () => {
            const pct = parseInt(btn.dataset.pct);
            const mode = document.getElementById('selectTradeAmountMode').value;

            if (mode === 'pct') {
                // In percentage mode, set the % value directly
                document.getElementById('inputTradeAmountUSDC').value = pct;
                updateStrategyField('trade_amount_usdc', pct);
            } else {
                // In fixed mode, calculate based on current total equity (for UX display)
                const totalEquity = currentBalance || 0;
                const amount = (totalEquity * (pct / 100)).toFixed(2);
                document.getElementById('inputTradeAmountUSDC').value = amount;
                updateStrategyField('trade_amount_usdc', parseFloat(amount));
            }

            updateTotalBaseUnits();
            document.querySelectorAll('#pct-buttons button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        });
    });

    document.getElementById('selectTradeAmountMode').addEventListener('change', (e) => {
        const mode = e.target.value;
        const isPct = (mode === 'pct');
        updateStrategyField('trade_amount_is_pct', isPct);

        // Update labels
        document.getElementById('asset-symbol-label').classList.toggle('d-none', isPct);
        document.getElementById('pct-symbol-label').classList.toggle('d-none', !isPct);

        updateTotalBaseUnits();
    });

    // Amount & Entry Updates
    document.getElementById('inputTradeAmountUSDC').addEventListener('input', () => {
        updateStrategyField('trade_amount_usdc', parseFloat(document.getElementById('inputTradeAmountUSDC').value));
        updateTotalBaseUnits();
    });
    document.getElementById('inputEntryPrice').addEventListener('input', (e) => {
        updateStrategyField('entry_price', parseFloat(e.target.value) || 0);
        updateTotalBaseUnits();
        updatePerformanceMetrics();
    });

    // Pillar Toggles & Overlays
    document.getElementById('tpToggle').addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.getElementById('tp-overlay').classList.toggle('d-none', checked);
        updateStrategyField('tp_enabled', checked);
    });

    document.getElementById('btn-enable-tp').addEventListener('click', () => {
        document.getElementById('tpToggle').click();
    });

    document.getElementById('stopLossToggle').addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.getElementById('sl-overlay').classList.toggle('d-none', checked);
        updateStrategyField('stop_loss_enabled', checked);
    });

    document.getElementById('btn-enable-sl').addEventListener('click', () => {
        document.getElementById('stopLossToggle').click();
    });

    document.getElementById('useExistingToggle').addEventListener('change', (e) => {
        updateStrategyField('use_existing', e.target.checked);
        const costBasisGroup = document.getElementById('cost-basis-group');
        if (costBasisGroup) costBasisGroup.classList.toggle('d-none', !e.target.checked);
    });

    const inputCostBasis = document.getElementById('inputCostBasis');
    if (inputCostBasis) {
        inputCostBasis.addEventListener('input', (e) => {
            const val = parseFloat(e.target.value);
            if (!isNaN(val)) {
                updateStrategyField('entry_price', val);
                updateTotalBaseUnits();
                updatePerformanceMetrics();
            }
        });
    }

    // Add listeners to update metrics on trigger changes
    ['inputTradeAmountUSDC', 'inputEntryPrice', 'stopLossPrice'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', updatePerformanceMetrics);
    });

    document.getElementById('trailingBuyToggle').addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.getElementById('trailing-buy-settings').classList.toggle('d-none', !checked);
        updateStrategyField('trailing_buy_enabled', checked);
    });

    document.getElementById('consolidatedReentryToggle').addEventListener('change', (e) => {
        updateStrategyField('consolidated_reentry', e.target.checked);
    });

    document.getElementById('trailingBuyDeviation').addEventListener('input', (e) => {
        updateStrategyField('trailing_buy_deviation', parseFloat(e.target.value));
    });
    // Trailing TP Slider
    const trailingSlider = document.getElementById('trailingDeviation');
    if (trailingSlider) {
        trailingSlider.addEventListener('input', (e) => {
            const val = e.target.value;
            const label = document.getElementById('trailing-deviation-label');
            if (label) label.innerText = `${val}%`;
            updateStrategyField('trailing_deviation', parseFloat(val));
        });
    }

    document.getElementById('consolidatedTpToggle').addEventListener('change', (e) => {
        updateStrategyField('consolidated_tp', e.target.checked);
    });

    document.getElementById('entryGridToggle').addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.getElementById('entry-grid-settings').classList.toggle('d-none', !checked);
        updateStrategyField('entry_grid_enabled', checked);
    });

    document.getElementById('addEntryTargetBtn').addEventListener('click', () => {
        const dev = parseFloat(document.getElementById('inputEntryGridPrice').value);
        const vol = parseFloat(document.getElementById('inputEntryGridVolume').value);
        if (isNaN(dev) || isNaN(vol)) return;

        const strat = currentConfig.symbol_strategies[activeSymbol];
        if (!strat.entry_targets) strat.entry_targets = [];
        strat.entry_targets.push({ deviation: dev.toFixed(2), volume: vol.toFixed(2) });
        strat.entry_targets.sort((a, b) => parseFloat(a.deviation) - parseFloat(b.deviation));
        renderEntryTargets();
        saveLiveConfig();
    });



    // Buy Type Tabs
    document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            updateOrderTypeUI(tab.dataset.type);
            updateStrategyField('entry_type', tab.dataset.type);
        });
    });

    // Direction Tabs
    document.querySelectorAll('#direction-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const dir = tab.dataset.direction;
            document.querySelectorAll('#direction-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            updateStrategyField('direction', dir);
            updateDirectionUI(dir);
            updateTotalBaseUnits();
            updatePerformanceMetrics();
        });
    });

    // Cond Type Tabs
    document.querySelectorAll('#cond-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#cond-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            updateOrderTypeUI(tab.dataset.type);
            updateStrategyField('entry_type', tab.dataset.type);
        });
    });

    // Take Profit Type Tabs
    document.querySelectorAll('#tp-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#tp-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            const isMarket = tab.dataset.type === 'MARKET';
            const ui = (allTranslations[currentLang] || {}).ui || {};
            if (document.getElementById('label-tp_description')) {
                document.getElementById('label-tp_description').innerText = isMarket ? (ui.desc_market || 'Market mode enabled') : (ui.desc_limit || 'Limit mode enabled');
            }
            updateStrategyField('tp_market_mode', isMarket);
        });
    });

    if (document.getElementById('trailingTpToggle')) {
        document.getElementById('trailingTpToggle').addEventListener('change', (e) => {
            const checked = e.target.checked;
            if (document.getElementById('trailing-params')) {
                document.getElementById('trailing-params').classList.toggle('d-none', !checked);
            }
            updateStrategyField('trailing_tp_enabled', checked);
        });
    }

    // New TP Redesign Listeners
    const inputTpPercent = document.getElementById('inputTpPercent');
    const inputTpVolume = document.getElementById('inputTpVolume');
    const tpVolumeSlider = document.getElementById('tpVolumeSlider');

    if (inputTpPercent) {
        inputTpPercent.addEventListener('input', () => {
            const pct = parseFloat(inputTpPercent.value) || 0;
            const entry = parseFloat(document.getElementById('inputEntryPrice').value) || 0;
            const strat = currentConfig.symbol_strategies[activeSymbol] || {};
            const direction = strat.direction || 'LONG';
            const price = direction === 'LONG' ? entry * (1 + pct / 100) : entry * (1 - pct / 100);
            document.getElementById('tp-price-preview').innerText = `${price.toFixed(2)} USDC`;
        });
    }

    if (tpVolumeSlider && inputTpVolume) {
        tpVolumeSlider.addEventListener('input', (e) => {
            inputTpVolume.value = e.target.value;
            updateTpVolumePreviews(e.target.value);
        });
        inputTpVolume.addEventListener('input', (e) => {
            tpVolumeSlider.value = e.target.value;
            updateTpVolumePreviews(e.target.value);
        });
    }

    document.getElementById('addTpTargetBtn').addEventListener('click', () => {
        const pct = parseFloat(document.getElementById('inputTpPercent').value);
        const volume = parseFloat(document.getElementById('inputTpVolume').value);
        if (isNaN(pct) || isNaN(volume) || volume <= 0) return;
        
        const strat = currentConfig.symbol_strategies[activeSymbol];
        if (!strat.tp_targets) strat.tp_targets = [];
        
        const currentTotal = strat.tp_targets.reduce((sum, t) => sum + (parseFloat(t.volume) || 0), 0);
        if (currentTotal + volume > 100.01) {
            alert("Total TP volume cannot exceed 100%");
            return;
        }

        strat.tp_targets.push({ percent: pct.toFixed(2), volume: volume.toFixed(2) });
        strat.tp_targets.sort((a, b) => parseFloat(a.percent) - parseFloat(b.percent));
        
        renderTpTargets();
        saveLiveConfig();
    });

    document.getElementById('cancelTpBtn').addEventListener('click', () => {
        inputTpPercent.value = '';
        inputTpVolume.value = '12.5';
        tpVolumeSlider.value = '12.5';
        document.getElementById('tp-price-preview').innerText = '0.00 USDC';
    });

    document.getElementById('bid-price').addEventListener('click', (e) => {
        const val = parseFloat(e.target.innerText);
        if (!isNaN(val)) {
            document.getElementById('inputEntryPrice').value = val;
            updateStrategyField('entry_price', val);
            updateTotalBaseUnits();
        }
    });

    document.getElementById('ask-price').addEventListener('click', (e) => {
        const val = parseFloat(e.target.innerText);
        if (!isNaN(val)) {
            document.getElementById('inputEntryPrice').value = val;
            updateStrategyField('entry_price', val);
            updateTotalBaseUnits();
        }
    });

    // Stop Loss Type Tabs
    document.querySelectorAll('#sl-type-tabs .pillar-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('#sl-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            const type = tab.dataset.type;
            const ui = (allTranslations[currentLang] || {}).ui || {};

            const group = document.getElementById('sl-order-price-group');
            if (group) group.classList.toggle('d-none', type === 'COND_MARKET');

            const desc = document.getElementById('label-sl_description');
            if (desc) {
                desc.innerText = type === 'COND_MARKET' ?
                    (ui.desc_sl_market || 'The order will be executed at market price when triggered') :
                    (ui.desc_sl_limit || 'The order will be placed on the exchange order book when the price meets Stop Loss conditions');
            }
            updateStrategyField('sl_type', type);
        });
    });

    document.getElementById('stopLossPrice').addEventListener('input', (e) => {
        const val = parseFloat(e.target.value);
        updateStrategyField('stop_loss_price', val);
        updateInternalPct('sl', val);
        // Sync Order Price
        const orderPriceInput = document.getElementById('slOrderPrice');
        if (orderPriceInput) {
            orderPriceInput.value = e.target.value;
            updateStrategyField('sl_order_price', val);
            updateInternalPct('sl-order', val);
        }
    });

    if (document.getElementById('slOrderPrice')) {
        document.getElementById('slOrderPrice').addEventListener('input', (e) => {
            const val = parseFloat(e.target.value);
            updateStrategyField('sl_order_price', val);
            updateInternalPct('sl-order', val);
        });
    }

    // Timeout Adjustment
    const timeoutInput = document.getElementById('slTimeoutDuration');
    if (document.getElementById('slTimeoutMinus')) {
        document.getElementById('slTimeoutMinus').addEventListener('click', () => {
            timeoutInput.value = Math.max(0, parseInt(timeoutInput.value) - 10);
            updateStrategyField('sl_timeout_duration', parseInt(timeoutInput.value));
        });
    }
    if (document.getElementById('slTimeoutPlus')) {
        document.getElementById('slTimeoutPlus').addEventListener('click', () => {
            timeoutInput.value = parseInt(timeoutInput.value) + 10;
            updateStrategyField('sl_timeout_duration', parseInt(timeoutInput.value));
        });
    }

    document.getElementById('slTimeoutToggle').addEventListener('change', (e) => {
        const controls = document.getElementById('sl-timeout-controls');
        if (controls) controls.classList.toggle('d-none', !e.target.checked);
        updateStrategyField('sl_timeout_enabled', e.target.checked);
    });


    document.getElementById('trailingSlToggle').addEventListener('change', (e) => {
        updateStrategyField('trailing_sl_enabled', e.target.checked);
    });

    document.getElementById('moveToBreakevenToggle').addEventListener('change', (e) => {
        updateStrategyField('move_to_breakeven', e.target.checked);
    });

    document.getElementById('trailingTpToggle').addEventListener('change', (e) => {
        updateStrategyField('trailing_tp_enabled', e.target.checked);
    });

    // Settings Modal Toggle
    document.getElementById('configBtn').addEventListener('click', () => {
        populateSettingsModal();
        new bootstrap.Modal(document.getElementById('settingsModal')).show();
    });

    document.getElementById('saveSettingsBtn').addEventListener('click', async () => {
        await saveSettingsFromModal();
        bootstrap.Modal.getInstance(document.getElementById('settingsModal')).hide();
        loadConfig(); // Refresh after save
    });


    if (document.getElementById('btnAddTrade')) {
        document.getElementById('btnAddTrade').addEventListener('click', () => {
            if (!activeSymbol || !currentConfig) return;
            const strat = currentConfig.symbol_strategies[activeSymbol];
            if (!strat) return;

            const btn = document.getElementById('btnAddTrade');
            const oldHtml = btn.innerHTML;
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Adding...';

            socket.emit('start_add_trade', {
                account_idx: activeAccountIdx,
                symbol: activeSymbol,
                settings: strat
            });

            setTimeout(() => {
                btn.disabled = false;
                btn.innerHTML = oldHtml;
            }, 2000);
        });
    }

    // Custom Bottom Tabs
    document.querySelectorAll('[data-tab-target]').forEach(tab => {
        tab.addEventListener('click', () => {
            const target = tab.dataset.tabTarget;
            document.querySelectorAll('.tab-pane-custom').forEach(p => p.classList.add('d-none'));
            document.querySelector(target).classList.remove('d-none');
            document.querySelectorAll('[data-tab-target]').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
        });
    });
}

function updateInternalPct(type, val) {
    if (!activeSymbol || isNaN(val)) return;
    const entry = parseFloat(document.getElementById('inputEntryPrice').value) || parseFloat(document.getElementById('bid-price').innerText);
    if (!entry) return;
    const diff = (val - entry) / entry * 100;
    const badge = document.getElementById(`${type}-pct-badge`);
    badge.innerText = `${diff > 0 ? '+' : ''}${diff.toFixed(2)}%`;
}

function updateTotalBaseUnits() {
    if (!activeSymbol || !currentConfig) return;
    const amountVal = parseFloat(document.getElementById('inputTradeAmountUSDC').value || 0);
    const priceText = document.getElementById('bid-price').innerText;
    const currentPrice = parseFloat(priceText) || 1;
    const entryInput = parseFloat(document.getElementById('inputEntryPrice').value);

    // All accounts use the same settings (global symbol_strategies)
    const strat = currentConfig.symbol_strategies[activeSymbol] || {};

    const leverage = strat.leverage || 20;
    const isPct = document.getElementById('selectTradeAmountMode').value === 'pct';

    let totalUSDC = amountVal;
    if (isPct) {
        // If %, show estimate based on current dashboard total equity for UI preview
        totalUSDC = (currentBalance * (amountVal / 100.0));
    }

    // The user wants to see Total Notional in USDC (Margin * Leverage)
    const totalNotionalUSDC = totalUSDC * leverage;
    document.getElementById('inputTotalUnits').value = totalNotionalUSDC.toFixed(2);

    // Still calculate units for min requirements check
    const totalUnits = totalNotionalUSDC / (entryInput || currentPrice);
    checkMinRequirements(totalUnits);
}

function checkMinRequirements(units) {
    const minBTC = 0.00015;
    const isBelow = activeSymbol.includes('BTC') && units < minBTC;
    const warning = document.getElementById('min-req-warning');
    if (warning) warning.classList.toggle('d-none', !isBelow);
}

function updateStrategyField(field, value) {
    if (currentConfig && activeSymbol) {
        // All accounts use the same settings (global symbol_strategies)
        if (!currentConfig.symbol_strategies[activeSymbol]) currentConfig.symbol_strategies[activeSymbol] = {};
        currentConfig.symbol_strategies[activeSymbol][field] = value;

        // Auto-save on discrete changes
        saveLiveConfig();
    }
}

async function loadConfig() {
    socket.emit('get_config');
}

function applyUiTranslations() {
    const ui = (allTranslations[currentLang] || {}).ui || {};
    for (const [key, text] of Object.entries(ui)) {
        const el = document.getElementById(`label-${key}`);
        if (el) el.innerText = text;

        // Custom handling for placeholders
        if (key === 'settings_add_symbol_placeholder') {
            const elInp = document.getElementById('newSymbolInput');
            if (elInp) elInp.placeholder = text;
        }
    }
    updateHelpTooltips();
}

function updateHelpTooltips() {
    const ui = (allTranslations[currentLang] || {}).ui || {};
    const helpIcons = [
        'use_existing', 'trailing_buy', 'consolidated_reentry', 
        'entry_grid', 'consolidated_tp', 'trailing_tp', 
        'sl_timeout', 'trailing_sl', 'move_to_breakeven'
    ];

    helpIcons.forEach(id => {
        const el = document.getElementById(`help-${id}`);
        if (el) {
            const content = ui[`help_${id}`] || '';
            el.setAttribute('data-bs-content', content);
            el.setAttribute('data-bs-original-title', ui.settings_title || 'Info'); // Optional title
            const popover = bootstrap.Popover.getInstance(el);
            if (popover) {
                popover.setContent({
                    '.popover-body': content
                });
            }
        }
    });
}

function updateUIFromConfig() {
    if (!activeSymbol || !currentConfig) return;

    // All accounts use the same settings (global symbol_strategies)
    const strat = currentConfig.symbol_strategies[activeSymbol] || {};

    // Update active symbol picker to match
    const picker = document.getElementById('selectActiveSymbol');
    if (picker && picker.value !== activeSymbol) {
        picker.value = activeSymbol;
    }

    const isPct = strat.trade_amount_is_pct || false;
    document.getElementById('inputTradeAmountUSDC').value = strat.trade_amount_usdc || 100;
    document.getElementById('asset-symbol-label').classList.toggle('d-none', isPct);
    document.getElementById('pct-symbol-label').classList.toggle('d-none', !isPct);

    document.getElementById('inputEntryPrice').value = strat.entry_price || 0;

    const consolidatedTpOn = strat.consolidated_tp || false;
    document.getElementById('consolidatedTpToggle').checked = consolidatedTpOn;

    const entryGridOn = strat.entry_grid_enabled || false;
    document.getElementById('entryGridToggle').checked = entryGridOn;
    document.getElementById('entry-grid-settings').classList.toggle('d-none', !entryGridOn);
    renderEntryTargets();

    const entryType = strat.entry_type || 'LIMIT';
    updateOrderTypeUI(entryType);

    const tpOn = strat.tp_enabled !== false;
    document.getElementById('tpToggle').checked = tpOn;
    // Explicitly toggle overlay visibility
    const tpOverlay = document.getElementById('tp-overlay');
    if (tpOn) tpOverlay.classList.add('d-none');
    else tpOverlay.classList.remove('d-none');
    const tpMarket = strat.tp_market_mode || false;
    document.querySelectorAll('#tp-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', (t.dataset.type === 'MARKET') === tpMarket));

    if (typeof renderTpTargets === 'function') renderTpTargets();

    const slOn = strat.stop_loss_enabled || false;
    document.getElementById('stopLossToggle').checked = slOn;

    const trailingTpOn = strat.trailing_tp_enabled || false;
    document.getElementById('trailingTpToggle').checked = trailingTpOn;
    document.getElementById('trailing-params').classList.toggle('d-none', !trailingTpOn);
    document.getElementById('trailingDeviation').value = strat.trailing_deviation || 0.5;
    document.getElementById('trailing-deviation-label').innerText = `${strat.trailing_deviation || 0.5}%`;

    const slPriceEl = document.getElementById('stopLossPrice');
    if (slPriceEl) {
        slPriceEl.value = strat.stop_loss_price || 0;
        updateInternalPct('sl', strat.stop_loss_price || 0);
    }

    const slTimeoutToggle = document.getElementById('slTimeoutToggle');
    if (slTimeoutToggle) slTimeoutToggle.checked = strat.sl_timeout_enabled || false;

    const slTimeoutControls = document.getElementById('sl-timeout-controls');
    if (slTimeoutControls) slTimeoutControls.classList.toggle('d-none', !strat.sl_timeout_enabled);

    const slTimeoutDuration = document.getElementById('slTimeoutDuration');
    if (slTimeoutDuration) slTimeoutDuration.value = strat.sl_timeout_duration || 300;

    const slType = strat.sl_type || 'COND_LIMIT';
    document.querySelectorAll('#sl-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.type === slType));

    const slOrderPriceGroup = document.getElementById('sl-order-price-group');
    if (slOrderPriceGroup) slOrderPriceGroup.classList.toggle('d-none', slType === 'COND_MARKET');

    if (strat.sl_trigger_source) {
        const btn = document.getElementById('slTriggerSourceBtn');
        if (btn) btn.innerText = strat.sl_trigger_source;
    }

    const slOrderPrice = strat.sl_order_price || strat.stop_loss_price || 0;
    const slOrderPriceInput = document.getElementById('slOrderPrice');
    if (slOrderPriceInput) {
        slOrderPriceInput.value = slOrderPrice;
        updateInternalPct('sl-order', slOrderPrice);
    }

    const trailingSlToggle = document.getElementById('trailingSlToggle');
    if (trailingSlToggle) trailingSlToggle.checked = strat.trailing_sl_enabled || false;

    const moveToBreakevenToggle = document.getElementById('moveToBreakevenToggle');
    if (moveToBreakevenToggle) moveToBreakevenToggle.checked = strat.move_to_breakeven || false;

    const useExistingOn = strat.use_existing || false;
    document.getElementById('useExistingToggle').checked = useExistingOn;
    const costBasisGroup = document.getElementById('cost-basis-group');
    if (costBasisGroup) costBasisGroup.classList.toggle('d-none', !useExistingOn);
    
    const inputCostBasis = document.getElementById('inputCostBasis');
    if (inputCostBasis) inputCostBasis.value = strat.entry_price || 0;

    const consolidatedOn = strat.consolidated_reentry || false;
    document.getElementById('consolidatedReentryToggle').checked = consolidatedOn;

    const trailingBuyToggle = document.getElementById('trailingBuyToggle');
    if (trailingBuyToggle) trailingBuyToggle.checked = strat.trailing_buy_enabled || false;

    const tbSettings = document.getElementById('trailing-buy-settings');
    if (tbSettings) tbSettings.classList.toggle('d-none', !strat.trailing_buy_enabled);

    const dir = strat.direction || 'LONG';
    document.querySelectorAll('#direction-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.direction === dir));
    updateDirectionUI(dir);

    if (typeof renderTpTargets === 'function') renderTpTargets();
    updateTotalBaseUnits();
    updatePerformanceMetrics();
}


function updateOrderTypeUI(type) {
    const descEl = document.getElementById('order-type-desc');
    const condOptions = document.getElementById('cond-options');
    const priceInputGroup = document.getElementById('inputEntryPrice').closest('.mb-2');
    const ui = (allTranslations[currentLang] || {}).ui || {};

    // Default visibility
    condOptions.classList.add('d-none');
    priceInputGroup.classList.remove('d-none');

    if (['LIMIT', 'MARKET', 'CONDITIONAL'].includes(type)) {
        document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(t => t.classList.remove('active'));
        const tab = document.querySelector(`#buy-type-tabs .pillar-tab[data-type="${type}"]`);
        if (tab) tab.classList.add('active');
    }

    if (['COND_LIMIT', 'COND_MARKET'].includes(type) || type === 'CONDITIONAL') {
        const condType = type === 'CONDITIONAL' ? 'COND_LIMIT' : type;
        document.querySelectorAll('#buy-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.type === 'CONDITIONAL'));
        condOptions.classList.remove('d-none');
        document.querySelectorAll('#cond-type-tabs .pillar-tab').forEach(t => t.classList.toggle('active', t.dataset.type === condType));
    }

    switch (type) {
        case 'LIMIT': descEl.innerText = ui.desc_limit; break;
        case 'MARKET':
            descEl.innerText = ui.desc_market;
            priceInputGroup.classList.add('d-none');
            break;
        case 'CONDITIONAL':
            descEl.innerText = ui.desc_conditional;
            condOptions.classList.remove('d-none');
            break;
        case 'COND_LIMIT':
            descEl.innerText = ui.desc_cond_limit;
            condOptions.classList.remove('d-none');
            break;
        case 'COND_MARKET':
            descEl.innerText = ui.desc_cond_market;
            condOptions.classList.remove('d-none');
            priceInputGroup.classList.add('d-none');
            break;
    }
}

function updateDirectionUI(dir) {
    const isLong = dir === 'LONG';
    const buyPriceLabel = document.getElementById('label-buy_price');
    const ui = (allTranslations[currentLang] || {}).ui || {};

    if (buyPriceLabel) {
        buyPriceLabel.innerText = isLong ? (ui.buy_price || 'Buy Price') : (ui.sell_price || 'Sell Price');
    }

    // Update colors for Buy/Sell tabs
    const directionTabs = document.querySelector('#direction-tabs');
    if (directionTabs) {
        directionTabs.querySelectorAll('.pillar-tab').forEach(t => {
            if (t.dataset.direction === 'LONG') {
                t.style.borderColor = t.classList.contains('active') ? 'var(--success-color)' : '';
                t.style.color = t.classList.contains('active') ? 'var(--success-color)' : '';
            } else {
                t.style.borderColor = t.classList.contains('active') ? '#ff9f43' : ''; // Orange for Sell
                t.style.color = t.classList.contains('active') ? '#ff9f43' : '';
            }
        });
    }

    // Update TP color theme if needed
    const tpHeader = document.getElementById('label-take_profit_header');
    if (tpHeader) {
        tpHeader.style.color = isLong ? 'var(--success-color)' : '#ff9f43';
    }
}

function initSymbolPicker() {
    const select = document.getElementById('selectActiveSymbol');
    select.innerHTML = currentConfig.symbols.map(s => `<option value="${s}">${s}</option>`).join('');
    if (currentConfig.symbols.length > 0) {
        activeSymbol = currentConfig.symbols[0];
        const unit = 'USDC';
        const labels = ['asset-symbol-label', 'base-asset-label', 'entry-price-asset-label', 'tp-price-asset-label', 'sl-price-asset-label', 'sl-order-price-asset-label'];
        labels.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.innerText = unit;
        });
    }
}





function setupSocketListeners() {
    socket.on('connect', () => {
        const dot = document.getElementById('connection-status-dot');
        if (dot) {
            dot.className = 'status-dot-pulse bg-success';
        }
    });

    socket.on('disconnect', () => {
        const dot = document.getElementById('connection-status-dot');
        if (dot) {
            dot.className = 'status-dot-pulse bg-danger';
        }
    });

    socket.on('bot_status', (data) => {
        isBotRunning = data.running;
        const btn = document.getElementById('startStopBtn');
        const spinner = document.getElementById('startStopSpinner');
        const label = document.getElementById('label-start') || document.getElementById('label-stop');

        btn.disabled = false;
        if (spinner) spinner.classList.add('d-none');

        const ui = (allTranslations[currentLang] || {}).ui || {};
        const text = isBotRunning ? (ui.stop || 'Stop') : (ui.start || 'Start');

        if (label) {
            label.innerText = text;
            label.id = isBotRunning ? 'label-stop' : 'label-start';
        } else {
            btn.innerText = text; // Fallback
        }

        btn.className = isBotRunning ? 'btn btn-sm btn-danger px-4 d-flex align-items-center gap-2' : 'btn btn-sm btn-accent px-4 d-flex align-items-center gap-2';
    });

    socket.on('price_update', (prices) => {
        if (activeSymbol && prices[activeSymbol]) {
            const p = prices[activeSymbol];
            const bid = p.bid !== undefined ? p.bid : p.last || p;
            const ask = p.ask !== undefined ? p.ask : p.last || p;

            const bidEl = document.getElementById('bid-price');
            const askEl = document.getElementById('ask-price');

            let precision = 2;
            if (bid < 0.1) precision = 6;
            else if (bid < 1) precision = 5;
            else if (bid < 10) precision = 4;
            else if (bid < 100) precision = 3;

            if (bidEl && typeof bid === 'number' && bidEl.innerText !== bid.toFixed(precision)) {
                bidEl.innerText = bid.toFixed(precision);
            }
            if (askEl && typeof ask === 'number' && askEl.innerText !== ask.toFixed(precision)) {
                askEl.innerText = ask.toFixed(precision);
            }

            // Only recalculate metrics if the price has moved significantly or periodically
            // to prevent "scatter" in the UI.
            if (!window.lastUiUpdate || Date.now() - window.lastUiUpdate > 500) {
                updateTotalBaseUnits();
                updatePerformanceMetrics();
                window.lastUiUpdate = Date.now();
            }
        }
    });

    socket.on('clear_console', () => {
        const out = document.getElementById('consoleOutput');
        if (out) out.innerHTML = '';
    });

    socket.on('account_update', (data) => {
        const container = document.getElementById('individual-accounts-container');
        if (!container) return;

        // Ensure activeAccountIdx is in bounds
        if (activeAccountIdx >= data.accounts.length) activeAccountIdx = 0;

        const accountsHtml = (data.accounts || []).map((acc, idx) => {
            const ui = (allTranslations[currentLang] || {}).ui || {};
            let balanceText = 'Disconnected';
            let balanceClass = 'text-primary';

            if (acc.error) {
                balanceText = `<span class="text-danger" title="${acc.error}"><i class="bi bi-exclamation-triangle-fill"></i> ${ui.api_error || 'API Error'}</span>`;
            } else if (acc.has_client && acc.balance !== undefined) {
                balanceText = '$' + acc.balance.toFixed(2);
            }

            const isActive = (idx === activeAccountIdx);

            return `
                <div class="account-card ${acc.has_client ? '' : 'opacity-50'} ${isActive ? 'active' : ''}"
                     style="min-width: 150px"
                     onclick="setActiveAccount(${idx})">
                    <div class="d-flex justify-content-between align-items-center mb-1">
                        <span class="small fw-bold text-secondary text-uppercase">${acc.name}</span>
                        <div class="account-dot ${acc.active ? 'bg-success' : 'bg-secondary'}" style="width:6px; height:6px; border-radius:50%"></div>
                    </div>
                    <span class="${balanceClass} fw-bold">${balanceText}</span>
                </div>
            `;
        }).join('');

        // Diff the accounts HTML to prevent jitter
        if (container.innerHTML !== accountsHtml) {
            container.innerHTML = accountsHtml;
        }

        currentBalance = data.total_equity || 0;
        const totalEquityVal = document.getElementById('total-equity-val');
        if (totalEquityVal) totalEquityVal.innerText = `$${currentBalance.toFixed(2)}`;

        const posTable = document.getElementById('positionsTableBody');
        let rows = [];
        (data.positions || []).forEach(p => {
            if (p.trades && p.trades.length > 0) {
                p.trades.forEach(t => {
                    const isFilled = t.filled || t.amount > 0;
                    const displayAmount = isFilled ? t.amount : `(${t.amount || 0})`;
                    const rowClass = isFilled ? "" : "opacity-50";
                    const isExternal = t.trade_id === 'External';
                    const badgeClass = isExternal ? 'bg-warning text-dark' : 'bg-secondary';
                    const pnl = isFilled ? (t.pnl || 0) : 0;

                    rows.push(`
                        <tr class="border-0 ${rowClass}">
                            <td>${p.account}</td>
                            <td>${p.symbol} <span class="badge ${badgeClass} smaller">${t.trade_id}</span></td>
                            <td class="${p.amount > 0 ? 'text-success' : 'text-danger'}">${displayAmount}</td>
                            <td>${(parseFloat(t.entry_price || p.entryPrice) || 0).toFixed(2)}</td>
                            <td class="${pnl >= 0 ? 'text-success' : 'text-danger'}">${pnl.toFixed(2)}</td>
                            <td><button class="btn btn-xs btn-outline-danger py-0" onclick="closePosition(${p.account_idx}, '${p.symbol}', ${isExternal ? 'null' : `'${t.trade_id}'` })">Kill</button></td>
                        </tr>
                    `);
                });
            } else {
                // External or untracked position
                rows.push(`
                    <tr class="border-0">
                        <td>${p.account}</td>
                        <td>${p.symbol} <span class="badge bg-warning text-dark smaller">External</span></td>
                        <td class="${p.amount > 0 ? 'text-success' : 'text-danger'}">${p.amount}</td>
                        <td>${(parseFloat(p.entryPrice) || 0).toFixed(2)}</td>
                        <td class="${p.unrealizedProfit >= 0 ? 'text-success' : 'text-danger'}">${(parseFloat(p.unrealizedProfit) || 0).toFixed(2)}</td>
                        <td><button class="btn btn-xs btn-outline-danger py-0" onclick="closePosition(${p.account_idx}, '${p.symbol}')">Kill</button></td>
                    </tr>
                `);
            }
        });
        const posHtml = rows.join('');
        if (posTable.innerHTML !== posHtml) {
            posTable.innerHTML = posHtml;
        }

        renderOpenOrders(data.open_orders || []);
    });

    socket.on('console_log', (data) => {
        const out = document.getElementById('consoleOutput');
        if (!out) return;

        const div = document.createElement('div');
        div.className = `small mb-1 console-entry ${data.level === 'error' ? 'text-danger' : 'text-success'}`;
        div.innerText = `[${data.timestamp}] ${data.rendered || data.message}`;
        out.appendChild(div);

        // Limit number of entries to prevent UI lag
        while (out.children.length > 200) {
            out.removeChild(out.firstChild);
        }

        out.scrollTop = out.scrollHeight;
    });

    socket.on('config_data', (data) => {
        const prevSymbol = activeSymbol; // remember current selection
        currentConfig = data;
        currentLang = currentConfig.language || 'en-US';
        document.getElementById('lang-select').value = currentLang;
        applyUiTranslations();
        
        // Rebuild picker but attempt to preserve selection
        const select = document.getElementById('selectActiveSymbol');
        select.innerHTML = currentConfig.symbols.map(s => `<option value="${s}">${s}</option>`).join('');
        
        if (prevSymbol && currentConfig.symbols.includes(prevSymbol)) {
            activeSymbol = prevSymbol;
            select.value = prevSymbol;
        } else if (currentConfig.symbols.length > 0) {
            activeSymbol = currentConfig.symbols[0];
            select.value = activeSymbol;
        }

        // Skip full UI refresh if modal is open to avoid layout jumps
        const modalEl = document.getElementById('settingsModal');
        if (modalEl && (modalEl.classList.contains('show') || document.body.classList.contains('modal-open'))) {
            return;
        }

        // Only update full UI if we didn't have a stable selection or it's a first load
        // But we actually need to update fields to match config.
        // Expanded guard for active elements to prevent input resets
        const activeId = document.activeElement ? document.activeElement.id : null;
        const guardedIds = [
            'inputTradeAmountUSDC', 'inputEntryPrice', 'tpVolumeSlider',
            'inputTpPercent', 'inputTpVolume', 'stopLossPrice',
            'slOrderPrice', 'slTimeoutDuration', 'inputEntryGridPrice',
            'inputEntryGridVolume', 'trailingBuyDeviation', 'trailingDeviation',
            'inputCostBasis'
        ];

        if (!activeId || !guardedIds.includes(activeId)) {
            updateUIFromConfig();
        }
    });

    socket.on('test_api_result', (data) => {
        alert(data.message);
        // Dispatch event for UI buttons waiting for this
        window.dispatchEvent(new CustomEvent('test_api_finished', { detail: data }));
    });

    socket.on('success', (data) => {
        // Optional: show a small toast or log
        console.log("Success:", data.message);
    });

    socket.on('error', (data) => {
        alert("Error: " + data.message);
    });
}

function populateSettingsModal() {
    if (!currentConfig) return;
    document.getElementById('demoModeToggle').checked = currentConfig.is_demo || false;

    // Global Settings (from first symbol or defaults)
    const firstSym = currentConfig.symbols[0];
    const currentLev = firstSym ? (currentConfig.symbol_strategies[firstSym]?.leverage || 20) : 20;
    const currentDir = firstSym ? (currentConfig.symbol_strategies[firstSym]?.direction || 'LONG') : 'LONG';
    const currentMargin = firstSym ? (currentConfig.symbol_strategies[firstSym]?.margin_type || 'CROSSED') : 'CROSSED';

    document.getElementById('globalLeverageInput').value = currentLev;
    document.getElementById('globalDirectionSelect').value = currentDir;
    document.getElementById('globalMarginModeSelect').value = currentMargin;

    // Accounts
    const accContainer = document.getElementById('api-accounts-settings');
    const ui = (allTranslations[currentLang] || {}).ui || {};

    accContainer.innerHTML = (currentConfig.api_accounts || []).map((acc, i) => `
        <div class="row g-2 mb-2 align-items-center account-setting-row" data-idx="${i}">
            <div class="col-2"><input type="text" class="form-control form-control-sm bg-dark text-light border-secondary" placeholder="${ui.acc_name_placeholder || 'Name'}" value="${acc.name || ''}" id="acc-name-${i}"></div>
            <div class="col-3"><input type="text" class="form-control form-control-sm bg-dark text-light border-secondary" placeholder="${ui.acc_key_placeholder || 'Key'}" value="${acc.api_key || ''}" id="acc-key-${i}"></div>
            <div class="col-3"><input type="password" class="form-control form-control-sm bg-dark text-light border-secondary" placeholder="${ui.acc_secret_placeholder || 'Secret'}" value="${acc.api_secret || ''}" id="acc-secret-${i}"></div>
            <div class="col-2 text-center">
                <button class="btn btn-xs btn-outline-info" onclick="testApiKey(${i})" id="test-btn-${i}">${ui.settings_test_btn || 'Test'}</button>
            </div>
            <div class="col-2 text-end">
                <div class="form-check form-switch d-inline-block">
                    <input class="form-check-input" type="checkbox" id="acc-enabled-${i}" ${acc.enabled !== false ? 'checked' : ''}>
                </div>
            </div>
        </div>
    `).join('');

    renderSymbolsInModal();
}

function renderSymbolsInModal() {
    const list = document.getElementById('symbols-list');
    list.innerHTML = currentConfig.symbols.map(s => `
        <div class="badge bg-secondary d-flex align-items-center gap-2 p-2">
            ${s}
            <i class="bi bi-x-circle cursor-pointer text-danger" onclick="removeSymbol('${s}')"></i>
        </div>
    `).join('');
}

window.removeSymbol = (sym) => {
    currentConfig.symbols = currentConfig.symbols.filter(s => s !== sym);
    renderSymbolsInModal();
};

async function saveSettingsFromModal() {
    const isDemo = document.getElementById('demoModeToggle').checked;

    // Accounts
    const api_accounts = [];
    document.querySelectorAll('.account-setting-row').forEach(row => {
        const i = row.dataset.idx;
        api_accounts.push({
            name: document.getElementById(`acc-name-${i}`).value,
            api_key: document.getElementById(`acc-key-${i}`).value,
            api_secret: document.getElementById(`acc-secret-${i}`).value,
            enabled: document.getElementById(`acc-enabled-${i}`).checked
        });
    });

    currentConfig.is_demo = isDemo;
    currentConfig.api_accounts = api_accounts;

    // Apply Global Settings to all symbols
    const globalLeverage = parseInt(document.getElementById('globalLeverageInput').value) || 20;
    const globalDirection = document.getElementById('globalDirectionSelect').value;
    const globalMarginMode = document.getElementById('globalMarginModeSelect').value;

    for (const sym in currentConfig.symbol_strategies) {
        currentConfig.symbol_strategies[sym].leverage = globalLeverage;
        currentConfig.symbol_strategies[sym].direction = globalDirection;
        currentConfig.symbol_strategies[sym].margin_type = globalMarginMode;
    }

    await saveLiveConfig();
}

async function saveLiveConfig(extra = {}) {
    if (!currentConfig) return;
    const payload = { ...currentConfig, ...extra };
    socket.emit('update_config', payload);
}

window.testApiKey = (i) => {
    const key = document.getElementById(`acc-key-${i}`).value;
    const secret = document.getElementById(`acc-secret-${i}`).value;
    const isDemo = document.getElementById('demoModeToggle').checked;
    const btn = document.getElementById(`test-btn-${i}`);

    if (!key || !secret) {
        alert("Please enter API key and secret");
        return;
    }

    btn.disabled = true;
    const oldText = btn.innerText;
    btn.innerText = "...";

    socket.emit('test_api_key', { api_key: key, api_secret: secret, is_demo: isDemo });

    // Use a one-time listener to re-enable button
    const finishHandler = () => {
        btn.disabled = false;
        btn.innerText = oldText;
        window.removeEventListener('test_api_finished', finishHandler);
    };
    window.addEventListener('test_api_finished', finishHandler);
};

window.closePosition = (account_idx, symbol, trade_id = null) => { socket.emit('close_trade', { account_idx, symbol, trade_id }); };

window.cancelOrder = (account_idx, symbol, order_id) => { socket.emit('cancel_order', { account_idx, symbol, order_id }); };
window.refreshData = () => { socket.emit('refresh_data'); };

function renderOpenOrders(orders) {
    const tableBody = document.getElementById('openOrdersTableBody');
    if (!tableBody) return;

    const ui = (allTranslations[currentLang] || {}).ui || {};

    let rows = [];
    orders.forEach(o => {
        const sideClass = o.side === 'BUY' ? 'text-success' : 'text-danger';
        rows.push(`
            <tr class="border-0">
                <td>${o.account}</td>
                <td>${o.symbol}</td>
                <td class="${sideClass} fw-bold">${o.side}</td>
                <td>${o.type}</td>
                <td>${o.qty}</td>
                <td>${parseFloat(o.price).toFixed(2)}</td>
                <td>
                    <button class="btn btn-xs btn-outline-warning py-0" 
                            onclick="cancelOrder(${o.account_idx}, '${o.symbol}', '${o.orderId}')">
                        ${ui.cancel_btn || 'Cancel'}
                    </button>
                </td>
            </tr>
        `);
    });

    if (rows.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="7" class="text-center text-secondary small py-3">${ui.no_open_orders || 'No open orders'}</td></tr>`;
    } else {
        tableBody.innerHTML = rows.join('');
    }
}

window.setActiveAccount = (idx) => {
    activeAccountIdx = idx;
    // We don't need to re-render all accounts immediately as account_update will handle it next poll,
    // but for immediate feedback we can:
    document.querySelectorAll('.account-card').forEach((card, i) => {
        card.classList.toggle('active', i === idx);
    });
    updateUIFromConfig();
};

// TP Split Management
function renderTpTargets() {
    if (!activeSymbol || !currentConfig) return;

    const strat = currentConfig.symbol_strategies[activeSymbol] || {};
    const targets = strat.tp_targets || [];
    const miniList = document.getElementById('tp-mini-list');
    const availablePctEl = document.getElementById('tp-available-pct');
    const doughnut = document.getElementById('tpDoughnutChart');

    // 1. Update Mini List
    miniList.innerHTML = targets.map((target, i) => `
        <div class="tp-mini-row">
            <span>${parseFloat(target.percent).toFixed(2)}%</span>
            <div class="d-flex align-items-center gap-2">
                <span class="fw-bold">${parseFloat(target.volume).toFixed(2)}%</span>
                <i class="bi bi-x tp-remove-btn" onclick="removeTpTarget(${i})"></i>
            </div>
        </div>
    `).join('');

    // 2. Calculate Distribution
    const totalVolume = targets.reduce((sum, t) => sum + (parseFloat(t.volume) || 0), 0);
    const available = Math.max(0, 100 - totalVolume);
    availablePctEl.innerText = `${available.toFixed(1)}%`;

    // 3. Update Doughnut Chart (Conic Gradient)
    // We use a set of colors for the segments
    const colors = ['#00b894', '#ff9f43', '#00d2d3', '#5f27cd', '#ff7675', '#74b9ff'];
    let gradientParts = [];
    let currentPos = 0;

    targets.forEach((t, i) => {
        const vol = parseFloat(t.volume) || 0;
        const color = colors[i % colors.length];
        gradientParts.push(`${color} ${currentPos}% ${currentPos + vol}%`);
        currentPos += vol;
    });

    if (available > 0) {
        gradientParts.push(`rgba(255,255,255,0.05) ${currentPos}% 100%`);
    }

    if (gradientParts.length > 0) {
        doughnut.style.background = `conic-gradient(${gradientParts.join(', ')})`;
    } else {
        doughnut.style.background = `rgba(255,255,255,0.05)`;
    }

    updatePerformanceMetrics();
}

function updatePerformanceMetrics() {
    if (!activeSymbol || !currentConfig) return;
    const strat = currentConfig.symbol_strategies[activeSymbol] || {};

    const entryPriceInput = document.getElementById('inputEntryPrice');
    const bidPriceEl = document.getElementById('bid-price');
    const entryPrice = parseFloat(entryPriceInput.value) || (bidPriceEl ? parseFloat(bidPriceEl.innerText) : 0) || 0;

    const slPriceInput = document.getElementById('stopLossPrice');
    const slPrice = slPriceInput ? parseFloat(slPriceInput.value) : 0;

    const amountInput = document.getElementById('inputTradeAmountUSDC');
    const amountUSDC = amountInput ? parseFloat(amountInput.value) : 0;

    const leverage = strat.leverage || 20;
    const direction = strat.direction || 'LONG';
    const totalNotional = amountUSDC * leverage;

    const multiplier = (direction === 'SHORT') ? -1 : 1;

    // 1. Calculate Est. SL PnL
    let slPnL = 0;
    if (entryPrice > 0 && slPrice > 0 && amountUSDC > 0) {
        const diffP = ((slPrice - entryPrice) / entryPrice) * multiplier;
        slPnL = diffP * totalNotional;
    }
    const slEl = document.getElementById('est-sl-pnl');
    if (slEl) {
        slEl.innerText = `${slPnL >= 0 ? '+' : ''}${slPnL.toFixed(2)} USDC`;
        slEl.classList.toggle('text-success', slPnL > 0);
        slEl.classList.toggle('text-danger', slPnL < 0);
    }

    // 2. Calculate Est. TP PnL (weighted average of targets)
    let tpPnL = 0;
    const targets = strat.tp_targets || [];
    if (entryPrice > 0 && amountUSDC > 0 && targets.length > 0) {
        targets.forEach(t => {
            const tpPct = (parseFloat(t.percent) / 100);
            const tpVol = (parseFloat(t.volume) / 100);
            tpPnL += (tpPct * totalNotional * tpVol);
        });
    }
    const tpEl = document.getElementById('est-tp-pnl');
    if (tpEl) {
        tpEl.innerText = `${tpPnL >= 0 ? '+' : ''}${tpPnL.toFixed(2)} USDC`;
        tpEl.classList.toggle('text-success', tpPnL > 0);
        tpEl.classList.toggle('text-danger', tpPnL < 0);
    }

    // 3. Calculate Risk/Reward
    let rr = 0;
    const risk = Math.abs(slPnL);
    if (risk > 0) {
        rr = tpPnL / risk;
    }
    const rrEl = document.getElementById('rr-ratio');
    if (rrEl) {
        rrEl.innerText = rr > 0 ? rr.toFixed(2) : '0.00';
        rrEl.classList.toggle('text-success', rr >= 2);
        rrEl.classList.toggle('text-warning', rr > 0 && rr < 2);
    }

    // 4. Update individual TP previews (legacy support if elements exist)
    targets.forEach((t, i) => {
        const badge = document.getElementById(`tp-pnl-preview-${i}`);
        if (badge) {
            const pnl = (parseFloat(t.percent) / 100) * totalNotional * (parseFloat(t.volume) / 100);
            badge.innerText = `+${pnl.toFixed(2)} USDC`;
        }
    });

    // 5. Update SL preview (legacy support if elements exist)
    const slBadge = document.getElementById('sl-pnl-preview');
    if (slBadge && slPrice > 0) {
        slBadge.innerText = `${slPnL.toFixed(2)} USDC`;
        slBadge.className = 'badge ' + (slPnL < 0 ? 'bg-danger' : 'bg-success');
    }
}

function updateTpVolumePreviews(val) {
    // Optional: could show USDC value here if we have totalTradeAmount
    const preview = document.getElementById('tp-volume-usdc-preview');
    if (preview) {
        // Estimate based on total equity * trade_amount_pct * target_vol
        // For now just keep it clean or use a global calculate function if available
    }
}

window.addTpTarget = () => {
    if (!activeSymbol) return;

    // All accounts use the same settings (global symbol_strategies)
    if (!currentConfig.symbol_strategies[activeSymbol]) currentConfig.symbol_strategies[activeSymbol] = {};
    const strat = currentConfig.symbol_strategies[activeSymbol];

    if (!strat.tp_targets) strat.tp_targets = [];

    // Default: use 0.6% increments and 12.5% volume (matches client ladder design)
    const lastPct = strat.tp_targets.length > 0 ? parseFloat(strat.tp_targets[strat.tp_targets.length - 1].percent) : 0;
    strat.tp_targets.push({ percent: (lastPct + 0.6).toFixed(2), volume: 12.5 });
    renderTpTargets();
    saveLiveConfig();
};

window.removeTpTarget = (idx) => {
    // All accounts use the same settings (global symbol_strategies)
    const strat = currentConfig.symbol_strategies[activeSymbol];
    strat.tp_targets.splice(idx, 1);
    renderTpTargets();
    saveLiveConfig();
};

/**
 * Entry Grid (DCA) Specific Rendering
 */
function renderEntryTargets() {
    if (!activeSymbol || !currentConfig) return;
    const strat = currentConfig.symbol_strategies[activeSymbol] || {};
    const targets = strat.entry_targets || [];
    const list = document.getElementById('entry-targets-list');
    if (!list) return;

    list.innerHTML = targets.map((t, i) => `
        <div class="d-flex justify-content-between align-items-center mb-1 bg-dark-subtle p-1 rounded">
            <span>Dev: <b class="text-accent">${t.deviation}%</b></span>
            <span>Vol: <b class="text-accent">${t.volume}%</b></span>
            <i class="bi bi-x-circle text-danger cursor-pointer" onclick="removeEntryTarget(${i})"></i>
        </div>
    `).join('');
}

window.removeEntryTarget = (idx) => {
    if (!activeSymbol || !currentConfig) return;
    const strat = currentConfig.symbol_strategies[activeSymbol];
    if (strat && strat.entry_targets) {
        strat.entry_targets.splice(idx, 1);
        renderEntryTargets();
        saveLiveConfig();
    }
};

window.updateTpTarget = (idx, field, value) => {
    // All accounts use the same settings (global symbol_strategies)
    const strat = currentConfig.symbol_strategies[activeSymbol];
    strat.tp_targets[idx][field] = value;
    renderTpTargets();
    saveLiveConfig();
};

// Button handled in main setupEventListeners

window.setSlTriggerSource = (source) => {
    document.getElementById('slTriggerSourceBtn').innerText = source;
    updateStrategyField('sl_trigger_source', source);
};

window.resetTpLadder = () => {
    if (!activeSymbol) return;
    const strat = currentConfig.symbol_strategies[activeSymbol];
    strat.tp_targets = Array.from({ length: 8 }, (_, i) => ({
        percent: ((i + 1) * 0.6).toFixed(1),
        volume: 12.5
    }));
    renderTpTargets();
    saveLiveConfig();
};


