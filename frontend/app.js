document.addEventListener("DOMContentLoaded", () => {
    // ---------- Cached DOM References ----------
    const tradeBtn = document.getElementById("trade-btn");
    const stopBtn = document.getElementById("stop-btn");
    const resetBtn = document.getElementById("reset-btn");
    const logContainer = document.getElementById("log-container");
    const connectionDot = document.getElementById("connection-dot");
    const connectionText = document.getElementById("connection-text");
    const algoStatus = document.getElementById("algo-status");
    const trackedSymbolsContainer = document.getElementById("tracked-symbols-container");
    const arbitrageFlag = document.getElementById("arbitrage-flag");
    const executionTargetSymbol = document.getElementById("execution-target-symbol");
    const hiddenSymbolInput = document.getElementById("symbol");
    const newSymbolInput = document.getElementById("new-symbol-input");
    const addSymbolBtn = document.getElementById("add-symbol-btn");
    const autoTradeBtn = document.getElementById("auto-trade-btn");
    const thresholdSlider = document.getElementById("threshold-slider");
    const thresholdVal = document.getElementById("threshold-val");
    const bufferSlider = document.getElementById("buffer-slider");
    const bufferVal = document.getElementById("buffer-val");
    const allocatedFundsInput = document.getElementById("allocated-funds");
    const liveMarginVal = document.getElementById("live-margin-val");

    // Pre-cache execution config elements
    const buyExchangeSelect = document.getElementById("buy-exchange");
    const buyPriceInput = document.getElementById("buy-price");
    const sellExchangeSelect = document.getElementById("sell-exchange");
    const sellPriceInput = document.getElementById("sell-price");

    const BASE_URL = "http://localhost:8000/api";
    const WS_URL = "ws://localhost:8000/ws/prices";

    // ---------- Request Token Catcher ----------
    const urlParams = new URLSearchParams(window.location.search);
    const reqToken = urlParams.get('request_token');
    if (reqToken) {
        fetch(`http://localhost:8000/redirect?request_token=${reqToken}`)
            .then(r => r.json())
            .then(data => {
                if(data.status === "success") {
                    window.location.href = window.location.pathname;
                } else {
                    alert("Kite Login Error: " + data.message);
                }
            });
    }

    // ---------- Logger ----------
    const addLog = (message, type = "system") => {
        const entry = document.createElement("div");
        entry.className = `log-entry ${type}`;
        const timestamp = new Date().toLocaleTimeString();
        entry.innerHTML = `[${timestamp}] ${message}`;
        logContainer.appendChild(entry);
        logContainer.scrollTop = logContainer.scrollHeight;
    };

    // ---------- Status Check (slow poll — 5 second interval) ----------
    const checkStatus = async () => {
        try {
            const res = await fetch(`${BASE_URL}/status`);
            const data = await res.json();
            
            if (data.connected !== undefined) {
                if (data.connected) {
                    connectionDot.className = "dot connected";
                    connectionText.textContent = "Backend Connected";
                } else {
                    connectionDot.className = "dot disconnected";
                    connectionText.innerHTML = `Kite Disconnected <a id="login-link" href="${BASE_URL}/login" target="_blank" style="color: var(--accent-primary); margin-left:8px; text-decoration:none;">Login -></a>`;
                }

                algoStatus.textContent = data.algo_running ? "EXECUTING" : (data.stop_requested ? "SYSTEM HALTED" : "STANDBY");
                algoStatus.className = "algo-status " + (data.algo_running ? "running" : (data.stop_requested ? "stopped" : "standby"));
                
                if (data.stop_requested) {
                    if (tradeBtn) tradeBtn.disabled = true;
                    if (resetBtn) resetBtn.classList.remove("hidden");
                    if (isAutoTradeArmed) {
                        isAutoTradeArmed = false;
                        if(autoTradeBtn) {
                            autoTradeBtn.innerHTML = `<span>Auto-Trade: OFF (HALTED)</span>`;
                            autoTradeBtn.classList.remove("auto-trade-active");
                        }
                    }
                } else {
                    if (tradeBtn) tradeBtn.disabled = false;
                    if (resetBtn) resetBtn.classList.add("hidden");
                }
            }
        } catch (error) {
            connectionDot.className = "dot disconnected";
            connectionText.textContent = "Backend Offline";
        }
        
        loadMargins();
    };

    const loadMargins = async () => {
        if (!liveMarginVal) return;
        try {
            const res = await fetch(`${BASE_URL}/margins`);
            const data = await res.json();
            if (data.status === "success") {
                liveMarginVal.textContent = `₹ ${parseFloat(data.data).toFixed(2)}`;
            } else {
                liveMarginVal.textContent = "₹ --";
            }
        } catch (e) {
            liveMarginVal.textContent = "₹ --";
        }
    };

    // ---------- App State ----------
    const defaultSymbols = ["INFY", "WIPRO", "TCS", "HDFCBANK"];
    let trackedSymbols = JSON.parse(localStorage.getItem("watchlist_memory")) || defaultSymbols;
    let trackedQuantities = JSON.parse(localStorage.getItem("quantity_memory")) || {};
    let isAutoTradeArmed = false;

    // Load saved fund allocation
    const savedFunds = localStorage.getItem("allocated_funds_memory");
    if (savedFunds && allocatedFundsInput) {
        allocatedFundsInput.value = savedFunds;
    }

    if (allocatedFundsInput) {
        allocatedFundsInput.addEventListener("change", (e) => {
            const val = parseFloat(e.target.value) || 0;
            localStorage.setItem("allocated_funds_memory", val.toString());
        });
    }

    const saveWatchlist = () => {
        localStorage.setItem("watchlist_memory", JSON.stringify(trackedSymbols));
        localStorage.setItem("quantity_memory", JSON.stringify(trackedQuantities));
    };

    // ---------- Add Symbol Handler ----------
    if (addSymbolBtn) {
        addSymbolBtn.addEventListener("click", () => {
            const sym = newSymbolInput.value.trim().toUpperCase();
            if (sym && !trackedSymbols.includes(sym)) {
                trackedSymbols.push(sym);
                newSymbolInput.value = "";
                saveWatchlist();
                renderTrackedRows();
                // Tell WebSocket to subscribe to the new symbol
                sendWsSubscription();
                // Also do an immediate REST fetch as fallback
                fetchPricesREST();
            }
        });
    }

    if (newSymbolInput) {
        newSymbolInput.addEventListener("keypress", (e) => {
            if (e.key === "Enter") addSymbolBtn.click();
        });
    }

    // ---------- Render Tracked Rows ----------
    const renderTrackedRows = () => {
        if (!trackedSymbolsContainer) return;
        trackedSymbolsContainer.innerHTML = "";
        trackedSymbols.forEach(sym => {
            const row = document.createElement("div");
            row.className = "tracked-row";
            row.id = `row-${sym}`;
            
            row.innerHTML = `
                <div class="delete-btn" data-sym="${sym}" style="cursor: pointer; padding: 0.5rem; color: var(--danger); font-size: 1.2rem; display: flex; align-items: center;" title="Remove ${sym}">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2M10 11v6M14 11v6"></path>
                    </svg>
                </div>
                <div class="tracked-symbol">${sym}</div>
                <div class="tracked-price">
                    <span class="label">NSE</span>
                    <span class="val" id="nse-${sym}">₹ --</span>
                </div>
                <div class="tracked-price">
                    <span class="label">BSE</span>
                    <span class="val" id="bse-${sym}">₹ --</span>
                </div>
                <div class="tick-box" id="tick-${sym}">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" style="display:none;" id="check-${sym}">
                        <polyline points="20 6 9 17 4 12"></polyline>
                    </svg>
                </div>
                <div class="quantity-input" style="display:flex; align-items:center;">
                    <input type="number" id="qty-${sym}" data-sym="${sym}" class="row-qty" value="${trackedQuantities[sym] || 1}" min="1" style="width:60px; background:rgba(0,0,0,0.3); color:white; border:1px solid rgba(255,255,255,0.2); border-radius:4px; padding:0.25rem; text-align:center;">
                </div>
            `;
            trackedSymbolsContainer.appendChild(row);
        });

        // Attach delete listeners
        document.querySelectorAll(".delete-btn").forEach(btn => {
            btn.addEventListener("click", (e) => {
                const symToRemove = e.currentTarget.getAttribute("data-sym");
                trackedSymbols = trackedSymbols.filter(s => s !== symToRemove);
                saveWatchlist();
                renderTrackedRows();
                sendWsSubscription();
            });
        });

        // Attach quantity listeners
        document.querySelectorAll(".row-qty").forEach(inp => {
            inp.addEventListener("change", (e) => {
                const symToUpdate = e.target.getAttribute("data-sym");
                const val = parseInt(e.target.value) || 1;
                trackedQuantities[symToUpdate] = val;
                saveWatchlist();
            });
        });
    };

    renderTrackedRows();

    // ---------- Core: Arbitrage Detection Logic ----------
    // In-memory price cache updated by WebSocket or REST
    const priceCache = {};  // { "INFY": { nse: 1842.5, bse: 1843.0 }, ... }
    const depthCache = {};  // { "INFY": { nse_bid: 1842, nse_ask: 1843, bse_bid: 1843, bse_ask: 1844, ... }, ... }
    const activeExecutions = {}; // { "INFY": { startTime: ..., unlocking: false }, ... }

    const processArbitrageDetection = () => {
        const thresholdPerc = parseFloat(thresholdSlider.value) || 0.10;
        const bufferFraction = parseFloat(bufferSlider ? bufferSlider.value : 0.25);

        let opportunities = [];

        for (const sym of trackedSymbols) {
            const data = priceCache[sym];
            if (!data) continue;

            const nseEl = document.getElementById(`nse-${sym}`);
            const bseEl = document.getElementById(`bse-${sym}`);
            const tickEl = document.getElementById(`tick-${sym}`);
            const checkIcon = document.getElementById(`check-${sym}`);

            // Update price display
            if (nseEl) nseEl.textContent = data.nse > 0 ? `₹ ${data.nse.toFixed(3)}` : "₹ --";
            if (bseEl) bseEl.textContent = data.bse > 0 ? `₹ ${data.bse.toFixed(3)}` : "₹ --";

            // Arbitrage detection — use depth data if available for true actionable spread
            if (data.nse > 0 && data.bse > 0) {
                const depth = depthCache[sym];
                let actionableSpread;
                let buyFromNse;

                if (depth && depth.nse_ask > 0 && depth.bse_bid > 0 && depth.bse_ask > 0 && depth.nse_bid > 0) {
                    // True actionable spread: compare best_ask on cheap exchange vs best_bid on expensive exchange
                    const nse_buy_bse_sell = depth.bse_bid - depth.nse_ask; // profit if buy NSE, sell BSE
                    const bse_buy_nse_sell = depth.nse_bid - depth.bse_ask; // profit if buy BSE, sell NSE
                    actionableSpread = Math.max(nse_buy_bse_sell, bse_buy_nse_sell);
                    buyFromNse = nse_buy_bse_sell >= bse_buy_nse_sell;
                } else {
                    // Fallback to LTP-based spread
                    actionableSpread = Math.abs(data.nse - data.bse);
                    buyFromNse = data.nse < data.bse;
                }

                const avgPrice = (data.nse + data.bse) / 2;
                const priceDiff = Math.abs(actionableSpread);
                const threshold = (thresholdPerc / 100) * avgPrice;

                if (actionableSpread > threshold) {
                    if (tickEl) tickEl.classList.add("active");
                    if (checkIcon) checkIcon.style.display = "block";
                    
                    opportunities.push({ sym, data, depth, avgPrice, priceDiff, buyFromNse });
                } else {
                    if (tickEl) tickEl.classList.remove("active");
                    if (checkIcon) checkIcon.style.display = "none";
                }
            } else {
                if (tickEl) tickEl.classList.remove("active");
                if (checkIcon) checkIcon.style.display = "none";
            }
        }

        // Sort by spread descending
        opportunities.sort((a, b) => b.priceDiff - a.priceDiff);

        // UI handling for top opportunity (legacy manual override setup)
        if (opportunities.length > 0 && arbitrageFlag) {
            const best = opportunities[0];
            arbitrageFlag.style.display = "block";
            arbitrageFlag.textContent = `Arbitrage Opportunity Detected in ${best.sym}!`;
            if (executionTargetSymbol) executionTargetSymbol.textContent = best.sym;
            if (hiddenSymbolInput) hiddenSymbolInput.value = best.sym;

            const activeEl = document.activeElement;
            const isEditing = activeEl === buyPriceInput || activeEl === sellPriceInput || activeEl === buyExchangeSelect || activeEl === sellExchangeSelect;
            if (!isEditing) {
                const bEx = best.buyFromNse !== undefined ? (best.buyFromNse ? "NSE" : "BSE") : (best.data.nse < best.data.bse ? "NSE" : "BSE");
                const sEx = bEx === "NSE" ? "BSE" : "NSE";

                // Use depth-aware aggressive pricing if available
                let buyLimitPx, sellLimitPx;
                if (best.depth && best.depth[`${bEx.toLowerCase()}_ask`] > 0) {
                    // Aggressive: buy at best_ask + 1 tick, sell at best_bid - 1 tick
                    const cheapAsk = best.depth[`${bEx.toLowerCase()}_ask`];
                    const expensiveBid = best.depth[`${sEx.toLowerCase()}_bid`];
                    buyLimitPx = (Math.ceil((cheapAsk + 0.05) / 0.05) * 0.05);
                    sellLimitPx = (Math.floor((expensiveBid - 0.05) / 0.05) * 0.05);
                } else {
                    const buffer = bufferFraction * (thresholdPerc / 100) * best.avgPrice;
                    const lowerPrice = Math.min(best.data.nse, best.data.bse);
                    const higherPrice = Math.max(best.data.nse, best.data.bse);
                    buyLimitPx = (Math.ceil((lowerPrice + buffer) / 0.05) * 0.05);
                    sellLimitPx = (Math.floor((higherPrice - buffer) / 0.05) * 0.05);
                }
                
                buyExchangeSelect.value = bEx;
                sellExchangeSelect.value = sEx;
                buyPriceInput.value = buyLimitPx.toFixed(2);
                sellPriceInput.value = sellLimitPx.toFixed(2);
            }
        } else if (arbitrageFlag) {
            arbitrageFlag.style.display = "none";
        }

        // ---------- AUTO-TRADE AUTONOMOUS EXECUTION ----------
        if (isAutoTradeArmed && tradeBtn) {
            const currentActiveCount = Object.keys(activeExecutions).length;
            const availableSlots = 4 - currentActiveCount;

            if (availableSlots > 0) {
                // Filter out already running ops
                const validOps = opportunities.filter(op => !activeExecutions[op.sym]);
                
                // Grab the top available ops up to limit
                const opsToExecute = validOps.slice(0, availableSlots);

                opsToExecute.forEach(op => {
                    const quantity = parseInt(trackedQuantities[op.sym]) || 1;
                    if (isNaN(quantity) || quantity <= 0) {
                        addLog(`Auto-trade aborted for ${op.sym}: invalid quantity.`, "error");
                        return;
                    }

                    // Calculate exchange sides — use depth-aware direction
                    const bEx = op.buyFromNse !== undefined ? (op.buyFromNse ? "NSE" : "BSE") : (op.data.nse < op.data.bse ? "NSE" : "BSE");
                    const sEx = bEx === "NSE" ? "BSE" : "NSE";

                    // Depth-aware aggressive pricing for IOC fills
                    let bPx, sPx;
                    if (op.depth && op.depth[`${bEx.toLowerCase()}_ask`] > 0) {
                        const cheapAsk = op.depth[`${bEx.toLowerCase()}_ask`];
                        const expensiveBid = op.depth[`${sEx.toLowerCase()}_bid`];
                        bPx = parseFloat((Math.ceil((cheapAsk + 0.05) / 0.05) * 0.05).toFixed(2));
                        sPx = parseFloat((Math.floor((expensiveBid - 0.05) / 0.05) * 0.05).toFixed(2));
                    } else {
                        const buffer = bufferFraction * (thresholdPerc / 100) * op.avgPrice;
                        const lowerPrice = Math.min(op.data.nse, op.data.bse);
                        const higherPrice = Math.max(op.data.nse, op.data.bse);
                        bPx = parseFloat((Math.ceil((lowerPrice + buffer) / 0.05) * 0.05).toFixed(2));
                        sPx = parseFloat((Math.floor((higherPrice - buffer) / 0.05) * 0.05).toFixed(2));
                    }

                    // Evaluate local max allocation constraint
                    const localMaxAllocation = parseFloat(allocatedFundsInput.value) || 0;
                    const requiredMargin = ((bPx + sPx) * quantity) / 5.0; // 5x leverage assumption

                    if (requiredMargin > localMaxAllocation) {
                        addLog(`Auto-trade skipped ${op.sym}: requires ₹${requiredMargin.toFixed(2)} which exceeds your Max Allocation limit of ₹${localMaxAllocation.toFixed(2)}.`, "warning");
                        return;
                    }

                    // Mark as in-flight lock
                    activeExecutions[op.sym] = { startTime: Date.now() };
                    addLog(`[AUTO-EXECUTE] Triggered ${op.sym} at ${thresholdPerc}% spread! Locking target...`, "warning");

                    fetch(`${BASE_URL}/trade/once`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            symbol: op.sym,
                            buy_exchange: bEx,
                            buy_price: parseFloat(bPx),
                            sell_exchange: sEx,
                            sell_price: parseFloat(sPx),
                            quantity: quantity
                        })
                    }).then(r => r.json()).then(data => {
                        if (data.status === "success") {
                            addLog(data.message, "success");
                        } else {
                            addLog(`Execution failed for ${op.sym}: ${data.detail || data.message}`, "error");
                            // Unlock immediately if execution hard failed locally
                            if (data.message && data.message.includes("INSUFFICIENT FUNDS")) {
                                delete activeExecutions[op.sym];
                                isAutoTradeArmed = false;
                                autoTradeBtn.innerHTML = `<span>Auto-Trade: OFF</span>`;
                                autoTradeBtn.classList.remove("auto-trade-active");
                                addLog("Auto-trade paused due to insufficient funds.", "error");
                            } else {
                                delete activeExecutions[op.sym];
                            }
                        }
                    }).catch(error => {
                        addLog(`Network error on ${op.sym}: ${error.message}`, "error");
                        delete activeExecutions[op.sym];
                    });
                });
            }
        }
    };

    // ---------- WebSocket Connection ----------
    let ws = null;
    let wsReconnectTimer = null;

    const connectWebSocket = () => {
        if (ws && ws.readyState === WebSocket.OPEN) return;

        try {
            ws = new WebSocket(WS_URL);

            ws.onopen = () => {
                addLog("WebSocket connected — streaming real-time prices.", "success");
                sendWsSubscription();
            };

            ws.onmessage = (event) => {
                try {
                    const payload = JSON.parse(event.data);

                    // New format: { prices: { "NSE:INFY": 1842.5, ... }, depth: { "NSE:INFY": { best_bid, best_ask, ... }, ... } }
                    // Backward compat: if no "prices" key, treat entire payload as flat price map
                    const priceUpdates = payload.prices || payload;
                    const depthUpdates = payload.depth || {};

                    for (const [key, price] of Object.entries(priceUpdates)) {
                        const [exchange, sym] = key.split(":");
                        if (!priceCache[sym]) priceCache[sym] = { nse: 0, bse: 0 };
                        if (exchange === "NSE") priceCache[sym].nse = price;
                        if (exchange === "BSE") priceCache[sym].bse = price;
                    }

                    // Process depth updates
                    for (const [key, dInfo] of Object.entries(depthUpdates)) {
                        const [exchange, sym] = key.split(":");
                        if (!depthCache[sym]) depthCache[sym] = { nse_bid: 0, nse_ask: 0, bse_bid: 0, bse_ask: 0, nse_bid_qty: 0, nse_ask_qty: 0, bse_bid_qty: 0, bse_ask_qty: 0 };
                        if (exchange === "NSE") {
                            depthCache[sym].nse_bid = dInfo.best_bid || 0;
                            depthCache[sym].nse_ask = dInfo.best_ask || 0;
                            depthCache[sym].nse_bid_qty = dInfo.bid_qty || 0;
                            depthCache[sym].nse_ask_qty = dInfo.ask_qty || 0;
                        }
                        if (exchange === "BSE") {
                            depthCache[sym].bse_bid = dInfo.best_bid || 0;
                            depthCache[sym].bse_ask = dInfo.best_ask || 0;
                            depthCache[sym].bse_bid_qty = dInfo.bid_qty || 0;
                            depthCache[sym].bse_ask_qty = dInfo.ask_qty || 0;
                        }
                    }

                    // Run detection immediately on every tick
                    processArbitrageDetection();
                } catch (e) {
                    // Malformed message
                }
            };

            ws.onclose = () => {
                addLog("WebSocket disconnected. Falling back to REST polling...", "system");
                // Auto-reconnect after 3 seconds
                if (!wsReconnectTimer) {
                    wsReconnectTimer = setTimeout(() => {
                        wsReconnectTimer = null;
                        connectWebSocket();
                    }, 3000);
                }
            };

            ws.onerror = () => {
                // onclose will fire after this
            };
        } catch (e) {
            addLog("Failed to connect WebSocket. Using REST fallback.", "error");
        }
    };

    const sendWsSubscription = () => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ action: "subscribe", symbols: trackedSymbols }));
        }
    };

    // ---------- REST Fallback Fetcher ----------
    const fetchPricesREST = async () => {
        if (trackedSymbols.length === 0) return;
        try {
            const symbolsParam = trackedSymbols.map(s => `symbols=${encodeURIComponent(s)}`).join("&");
            const res = await fetch(`${BASE_URL}/quotes/bulk?${symbolsParam}`);
            const payload = await res.json();

            if (payload.status === "success") {
                const dataBlock = payload.data;
                for (const sym of trackedSymbols) {
                    const data = dataBlock[sym];
                    if (data) {
                        priceCache[sym] = { nse: data.nse, bse: data.bse };
                    }
                }
                processArbitrageDetection();
            }
        } catch (e) {
            // Silent fail
        }
    };

    // ---------- Slider Listeners ----------
    if (thresholdSlider && thresholdVal) {
        thresholdSlider.addEventListener("input", (e) => {
            thresholdVal.textContent = parseFloat(e.target.value).toFixed(2) + "%";
        });
    }
    if (bufferSlider && bufferVal) {
        bufferSlider.addEventListener("input", (e) => {
            bufferVal.textContent = parseFloat(e.target.value).toFixed(2) + "x";
        });
    }

    // ---------- Auto-Trade Toggle ----------
    if (autoTradeBtn) {
        autoTradeBtn.addEventListener("click", () => {
            isAutoTradeArmed = !isAutoTradeArmed;
            if (isAutoTradeArmed) {
                autoTradeBtn.innerHTML = `<span>Auto-Trade: ARMED</span>`;
                autoTradeBtn.classList.add("auto-trade-active");
                addLog("AUTO-TRADE SYSTEM ARMED. Awaiting opportunity...", "warning");
            } else {
                autoTradeBtn.innerHTML = `<span>Auto-Trade: OFF</span>`;
                autoTradeBtn.classList.remove("auto-trade-active");
                addLog("Auto-trade disarmed.", "system");
            }
        });
    }

    // ---------- Manual Trade Handler ----------
    if (tradeBtn) {
        tradeBtn.addEventListener("click", async () => {
            const symbol = hiddenSymbolInput ? hiddenSymbolInput.value.trim().toUpperCase() : "";
            const quantity = parseInt(trackedQuantities[symbol]) || 1;
            const buyExchange = buyExchangeSelect.value;
            const buyPrice = parseFloat(buyPriceInput.value);
            const sellExchange = sellExchangeSelect.value;
            const sellPrice = parseFloat(sellPriceInput.value);

            if (!symbol || isNaN(quantity) || quantity <= 0) {
                addLog("Invalid symbol or quantity.", "error");
                return;
            }
            if (isNaN(buyPrice) || buyPrice <= 0 || isNaN(sellPrice) || sellPrice <= 0) {
                addLog("Invalid limit prices. Must be greater than 0.", "error");
                return;
            }
            if (buyExchange === sellExchange) {
                addLog("Warning: Buy and Sell exchanges are identical.", "warning");
            }

            tradeBtn.disabled = true;
            tradeBtn.innerHTML = `<span>Executing...</span>`;
            addLog(`Placing CONCURRENT orders for ${symbol}. BUY[${buyExchange}] @ ${buyPrice} | SELL[${sellExchange}] @ ${sellPrice}`, "system");

            try {
                const res = await fetch(`${BASE_URL}/trade/once`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        symbol, buy_exchange: buyExchange, buy_price: buyPrice,
                        sell_exchange: sellExchange, sell_price: sellPrice, quantity
                    })
                });
                const data = await res.json();
                if (res.ok && data.status === "success") {
                    addLog(data.message, "success");
                    if (data.latency_ms !== undefined) {
                        const latVal = document.getElementById("latency-val");
                        if (latVal) latVal.textContent = `Latency: ${data.latency_ms} ms`;
                    }
                } else {
                    addLog(`Error: ${data.detail || data.message || "Execution failed."}`, "error");
                }
            } catch (error) {
                addLog(`Network error: ${error.message}`, "error");
            } finally {
                tradeBtn.disabled = false;
                tradeBtn.innerHTML = `
                    <span>Execute Single Trade</span>
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M5 12h14M12 5l7 7-7 7"/>
                    </svg>
                `;
                checkStatus();
            }
        });
    }

    // ---------- Emergency Controls ----------
    if (stopBtn) {
        stopBtn.addEventListener("click", async () => {
            addLog("TRIGGERING EMERGENCY STOP...", "error");
            algoStatus.textContent = "SYSTEM HALTED";
            algoStatus.className = "algo-status stopped";
            if (tradeBtn) tradeBtn.disabled = true;

            try {
                const res = await fetch(`${BASE_URL}/trade/stop`, { method: "POST" });
                const data = await res.json();
                addLog(data.message, "error");
                checkStatus();
            } catch (error) {
                addLog(`Failed to communicate stop: ${error.message}`, "error");
            }
        });
    }

    if (resetBtn) {
        resetBtn.addEventListener("click", async () => {
            try {
                const res = await fetch(`${BASE_URL}/trade/reset`, { method: "POST" });
                const data = await res.json();
                addLog(data.message, "system");
                checkStatus();
            } catch (error) {
                addLog("Failed to reset backend.", "error");
            }
        });
    }

    // ---------- Tab Navigation ----------
    const navItems = document.querySelectorAll(".nav-item");
    const views = document.querySelectorAll(".view-section");

    let currentTab = "trading-view";

    navItems.forEach(item => {
        item.addEventListener("click", () => {
            navItems.forEach(nav => nav.classList.remove("active"));
            item.classList.add("active");
            currentTab = item.getAttribute("data-target");
            
            views.forEach(view => {
                view.id === currentTab ? view.classList.remove("hidden") : view.classList.add("hidden");
            });

            if (currentTab === "investments-view") loadInvestments();
            if (currentTab === "pnl-view") loadPnL();
            if (currentTab === "orders-view") loadOrders();
        });
    });

    // Active Tab Polling Interval
    setInterval(() => {
        if (currentTab === "investments-view") loadInvestments();
        if (currentTab === "pnl-view") loadPnL();
        if (currentTab === "orders-view") loadOrders();
    }, 5000);

    // ---------- Data Fetchers ----------
    const loadInvestments = async () => {
        const tbody = document.getElementById("holdings-body");
        if (tbody.innerHTML.includes("Loading holdings...")) tbody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 2rem;">Loading holdings...</td></tr>`;
        try {
            const res = await fetch(`${BASE_URL}/investments`);
            const data = await res.json();
            if (data.status === "success" && data.data && data.data.length > 0) {
                tbody.innerHTML = "";
                data.data.forEach(h => {
                    const pnlStyle = h.pnl > 0 ? "text-success" : (h.pnl < 0 ? "text-danger" : "");
                    const row = document.createElement("tr");
                    row.innerHTML = `
                        <td style="font-weight:600;">${h.tradingsymbol}</td>
                        <td>${h.quantity}</td>
                        <td>₹ ${(h.average_price * 1).toFixed(3)}</td>
                        <td>₹ ${(h.last_price * 1).toFixed(3)}</td>
                        <td class="${pnlStyle}">₹ ${(h.pnl * 1).toFixed(3)}</td>
                    `;
                    tbody.appendChild(row);
                });
            } else if (data.status === "success") {
                tbody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 2rem; color: var(--text-secondary);">No holdings found.</td></tr>`;
            }
        } catch (e) {}
    };

    const loadPnL = async () => {
        const tbody = document.getElementById("pnl-body");
        const header = document.getElementById("total-pnl-header");
        if (tbody.innerHTML.includes("Loading PnL...")) tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 2rem;">Loading PnL...</td></tr>`;
        
        try {
            const res = await fetch(`${BASE_URL}/pnl`);
            const payload = await res.json();
            if (payload.status === "success") {
                const totalM2M = payload.data.summary.m2m;
                header.textContent = `₹ ${totalM2M.toFixed(2)}`;
                header.className = totalM2M > 0 ? "text-success" : (totalM2M < 0 ? "text-danger" : "");

                const positions = payload.data.positions;
                if (!positions || positions.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 2rem; color: var(--text-secondary);">No intraday positions found.</td></tr>`;
                    return;
                }
                
                tbody.innerHTML = "";
                positions.forEach(p => {
                    const m2mStyle = p.m2m > 0 ? "text-success" : (p.m2m < 0 ? "text-danger" : "");
                    const row = document.createElement("tr");
                    row.innerHTML = `
                        <td style="font-weight:600;">${p.tradingsymbol}</td>
                        <td>${p.quantity}</td>
                        <td>₹ ${(p.buy_price * 1).toFixed(2)}</td>
                        <td>₹ ${(p.sell_price * 1).toFixed(2)}</td>
                        <td>₹ ${(p.last_price * 1).toFixed(2)}</td>
                        <td class="${m2mStyle}" style="font-weight:bold;">₹ ${(p.m2m * 1).toFixed(2)}</td>
                    `;
                    tbody.appendChild(row);
                });
            }
        } catch (e) {}
    };

    const loadOrders = async () => {
        const tbody = document.getElementById("orders-body");
        if (tbody.innerHTML.includes("Loading orders...")) tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 2rem;">Loading orders...</td></tr>`;
        
        try {
            const res = await fetch(`${BASE_URL}/orders`);
            const payload = await res.json();
            
            if (payload.status === "success") {
                const orders = payload.data;
                if (!orders || orders.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 2rem; color: var(--text-secondary);">No orders today.</td></tr>`;
                    return;
                }
                
                // Sort by time descending (newest first)
                orders.sort((a, b) => new Date(b.order_timestamp) - new Date(a.order_timestamp));
                
                // Active Execution Cooldown Checking
                Object.keys(activeExecutions).forEach(sym => {
                    const lockInfo = activeExecutions[sym];
                    if (lockInfo.unlocking) return;

                    // Find orders for this symbol placed after the lock start time
                    const relatedOrders = orders.filter(o => o.tradingsymbol === sym && new Date(o.order_timestamp).getTime() >= lockInfo.startTime);
                    
                    const buyCompleted = relatedOrders.some(o => o.transaction_type === "BUY" && o.status === "COMPLETE");
                    const sellCompleted = relatedOrders.some(o => o.transaction_type === "SELL" && o.status === "COMPLETE");

                    if (buyCompleted && sellCompleted) {
                        lockInfo.unlocking = true;
                        addLog(`Both legs for ${sym} are COMPLETE. Initiating 5s arbitrage cooldown...`, "system");
                        setTimeout(() => {
                            delete activeExecutions[sym];
                            addLog(`[COOLDOWN CLEARED] ${sym} is re-armed for arbitrage.`, "system");
                        }, 5000);
                    }

                    // Failsafe timeout: if both legs haven't completed within 30 seconds,
                    // release the lock. The backend's _monitor_and_cleanup handles squareoff.
                    const lockAge = Date.now() - lockInfo.startTime;
                    if (lockAge > 30000 && !lockInfo.unlocking) {
                        lockInfo.unlocking = true;
                        addLog(`[TIMEOUT] ${sym} locked for >30s without both legs completing. Backend cleanup handles squareoff. Releasing lock...`, "error");
                        setTimeout(() => {
                            delete activeExecutions[sym];
                            addLog(`[TIMEOUT CLEARED] ${sym} re-armed after timeout.`, "system");
                        }, 2000);
                    }
                });

                tbody.innerHTML = "";
                orders.forEach(o => {
                    const time = new Date(o.order_timestamp).toLocaleTimeString();
                    const statusClass = `status-${o.status.replace(/ /g, '_')}`;
                    const typeColor = o.transaction_type === "BUY" ? "var(--accent-primary)" : "var(--danger)";
                    
                    const row = document.createElement("tr");
                    row.innerHTML = `
                        <td style="color: var(--text-secondary); font-size: 0.9rem;">${time}</td>
                        <td style="font-weight:600;">${o.tradingsymbol}</td>
                        <td style="color: ${typeColor}; font-weight: bold;">${o.transaction_type}</td>
                        <td>${o.filled_quantity}/${o.quantity}</td>
                        <td>₹ ${(o.average_price > 0 ? o.average_price : o.price).toFixed(2)}</td>
                        <td><span class="status-badge ${statusClass}">${o.status}</span></td>
                    `;
                    tbody.appendChild(row);
                });
            }
        } catch (e) {}
    };

    // ---------- Boot Sequence ----------

    // 1. Status check immediately + every 5 seconds (not 500ms)
    checkStatus();
    setInterval(checkStatus, 5000);

    // 2. Attempt WebSocket connection
    connectWebSocket();

    // 3. REST fallback poll — only runs if WebSocket is not connected
    setInterval(() => {
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            fetchPricesREST();
        }
    }, 500);
});
