# 🤖 Binance Scaler — Complete Documentation

A sophisticated, multi-account Binance Futures trading bot with a real-time web dashboard. It automates a grid-based "Scalping" strategy with perpetual re-entry cycles, supports multiple trading pairs simultaneously, and provides a live monitoring interface — all from a Flask web server you can host locally or in the cloud via Railway.

---

## Table of Contents

1. [How It Works — Overview](#how-it-works)
2. [Core Trading Strategy](#core-trading-strategy)
3. [Dashboard Features](#dashboard-features)
4. [Configuration Reference](#configuration-reference)
5. [Installation & Running Locally](#installation--running-locally)
6. [☁️ Deploy to Railway (Recommended)](#️-deploy-to-railway-recommended)
7. [Architecture](#architecture)

---

## How It Works

When you launch the bot (`python app.py`), a Flask web server starts on port **3000**. You open the dashboard in your browser. The bot engine runs entirely in the background as a Python process. All real-time communication between the frontend and backend happens via **Socket.IO WebSockets** — no page refreshes needed.

**Data Flow:**
```
Binance API (REST + WebSocket)
        ↓
  BinanceTradingBotEngine (bot_engine.py)
        ↓
  Flask / Socket.IO (app.py)
        ↓
  Browser Dashboard (dashboard.html + app.js)
```

A **global background worker thread** starts immediately on launch — even before you press "Start". This worker:
- Fetches live market prices for all configured symbols every ~1 second
- Fetches the maximum allowed leverage for each symbol from the exchange
- Fetches exchange precision rules (step size, tick size) per symbol for correct order formatting
- Emits live price and account data to all connected browser windows

---

## Core Trading Strategy

The bot implements a **grid-based perpetual futures scalping system**. Here is exactly what happens, step by step:

### 1. Initial Entry
When the bot is started or a new cycle begins, it places an entry order based on your configured **Entry Type**:

- **Limit (default)**: Places a limit order at your exact `entry_price`. These prices are user-defined and persist independently across all configured symbols.
- **Market**: Executes the entry immediately at current market rates.
- **Conditional (Cond. Limit / Cond. Market)**: Waits for the price to hit a "Trigger Price" before placing a Limit order (at Order Price) or executing a Market order.

The total position size is calculated as:

```
Position Size = (trade_amount_usdc × leverage) / entry_price
```

**Example:** `$100 USDC × 95x leverage / $9.00 price = ~1,055.5 LINK`

> Both the quantity and the price are automatically formatted to the exact precision required by the Binance exchange for that symbol, preventing precision-related API errors.

### 2. "Use Existing Assets" Logic
If you already have an open position for the symbol when the bot starts, it **will not open a new entry**. Instead, it reads your existing position and immediately places the Take Profit grid on top of it. This lets you attach the bot to a position you opened manually on the exchange.

- **Toggle ON (default):** Attaches to existing positions — no new entry is opened.
- **Toggle OFF:** Always places a fresh entry order at the configured price, even if a position exists.

### 3. The Take Profit (TP) Grid
Once the initial entry fills, the bot immediately places the **entire TP grid as a batch** of Limit orders on the exchange book. This ensures your exit strategy is active the moment you have a position.

- **`total_fractions`**: How many levels to split your position (e.g., `8` = 8 individual TP orders, each worth 12.5% of the total position).
- **`price_deviation`**: The percentage price gap between each level (e.g., `0.6%`).

**Example Grid — LONG at $9.00, 8 fractions, 0.6% deviation:**

| Level | Price  | % of Position Sold |
|-------|--------|---------------------|
| 1     | $9.054 | 12.50%              |
| 2     | $9.108 | 12.50%              |
| 3     | $9.162 | 12.50%              |
| 4     | $9.216 | 12.50%              |
| 5     | $9.270 | 12.50%              |
| 6     | $9.324 | 12.50%              |
| 7     | $9.378 | 12.50%              |
| 8     | $9.432 | 12.50%              |

Each order is a live `LIMIT GTC` order on the Binance order book — **not managed locally**. Your profit targets persist even if the bot crashes or goes offline.

### 4. The Perpetual Re-Entry Cycle
This is what makes the strategy "perpetual". When any TP level fills:
1. The bot detects the fill instantly via a **WebSocket User Data Stream** (no polling).
2. It immediately places a **Re-Entry Limit Order** at the price level just below the TP that just filled.
3. When that Re-Entry fills (price dips back down), it places the TP order back up at that level.
4. This cycle repeats **indefinitely**, scalping the same price range over and over.

```
Price rises:     Entry fills → TP Level 1 fills → Re-Entry placed below it
Price oscillates: Re-Entry fills → TP Level 1 re-placed → Cycle continues...
```

### 5. Trailing Take Profit
An optional overlay on top of the grid. When enabled, the bot tracks the maximum unrealized P&L for the position. If the P&L drops by your configured `trailing_deviation %` from its peak, the bot **closes the entire position at market price** to lock in that profit.

- Smaller deviation = triggers sooner, tighter lock-in
- Larger deviation = lets profits run longer before exiting

### 6. Balance & Order Safety
Before placing any re-entry order, the bot validates your available margin. If insufficient balance is detected, the order is skipped and a warning is logged to the console — preventing over-leveraged losses.

### 7. Manual Position Closing
From the dashboard's "Positions" tab, you can close any open position at any time. The bot:
1. Cancels all open orders for that symbol.
2. Sends a Market Order to flatten the full position immediately.

---

## Dashboard Features

The dashboard is a single-page real-time interface divided into three columns plus a bottom panel.

---

### Left Column — Strategic Control

#### Asset Switcher
A prominent dropdown at the top. Selecting a different symbol **switches the entire left panel** to display and control the strategy for that specific symbol. Each symbol maintains its own independent strategy settings.

#### Direction Display (`LONG/SHORT QUANTITY SYMBOL`)
Shows the currently active trading direction, the calculated base-unit quantity for the trade, and the symbol name. Updates live as you adjust the trade amount or leverage.

#### Manage Existing Assets Toggle
- **ON (default):** Bot attaches to any existing open position — no new entry is placed.
- **OFF:** Bot always places a fresh entry limit order at the configured price.

#### Leverage Controls
- **Leverage Slider:** Drag to set leverage from 1x up to the **exchange-enforced maximum** for that symbol (fetched live from Binance — e.g., 125x for BTCUSDC, 75x for LINKUSDC). The slider maximum automatically adjusts per symbol.
- **Leverage Marks:** Dynamically spaced markers (1x, 25%, 50%, 75%, Max) so you always know where you are.
- **Quick Preset Buttons** (1x, 10x, 20x, 50x, MAX): Instantly jump to a preset. The MAX button dynamically shows the actual maximum for the active symbol. Presets that exceed the symbol's max are automatically hidden.
- **Margin Mode** (Cross / Isolated): Sets the futures margin mode applied to the account when the bot starts.
- **Estimated Margin Display:** Shows how much USDC collateral the current trade setup requires at the live market price.

#### Trade Amount
- **USDC Input:** Enter the total USDC collateral to allocate. This is your margin, not the position's notional value.
- **Entry Type Tabs:** Choose between **Limit**, **Market**, or **Conditional** entry strategies.
- **Entry / Trigger Price Input:** For Limit and Conditional orders, set your specific target prices. These prices persist per-symbol and are saved automatically.
- **Live Market Price:** Displayed below — updates every second, even when the bot is not running.

#### Total Notional (Units)
The total quantity of the underlying asset your position will control:
```
Total Units = (trade_amount_usdc × leverage) / live_price
```

---

### Middle Column — Take Profit Settings

#### Take Profit Toggle
Enable or disable the TP grid for the selected symbol.

#### Availability Ring
A circular progress indicator showing what percentage of your position **has not yet been sold**. Starts at 100%. Decreases as TP levels fill and increases as re-entries fill. Gives you a live visual sense of grid activity.

#### TP Grid Table
Dynamically generated from your settings:
- **Left column:** The percentage above/below your entry price for each TP level.
- **Right column:** The quantity (fraction of total position) sold at each level.
- **Chain icon (🔗):** Indicates the level is linked to a re-entry — when this TP fills, a re-entry is placed at the level below.

#### Max TP Targets
Confirms the number of TP levels configured.

---

#### Trailing Take Profit

| Control | What It Does |
|---|---|
| **Enable Toggle** | Turns trailing P&L monitoring on/off for this symbol |
| **Estimated Peak Price** | Optional input — enter a target price to simulate expected profit before the trailing stop triggers. Leave blank to use current price. |
| **Deviation Slider (0–10%)** | How far the P&L must drop from its peak to trigger a full close |
| **Preset Buttons** (0.1%, 0.5%, 1%, 2%, 5%) | Quick deviation values |
| **Approximate Profit Display** | Shows expected `$profit`, `%gain`, and `ROE` in real time, using the Estimated Peak Price if set |

---

### Right Column — Account Summary & Symbols

#### Account Summary
- **Net Equity:** Total USDC wallet balance + unrealized PnL across all active accounts.
- **Individual Account Balances:** Each API account is listed with its name, a green/grey dot indicating whether it is actively trading, and its USDC balance. This is always visible — even when the bot is not running.
- **Active Accounts:** Count of API accounts currently connected.

#### Symbols Panel
Lists all configured symbols. Each has:
- A **trash icon** to remove the symbol from the strategy (takes effect on next start).
- A **+ button** to add a new symbol to the live config without restarting.

---

### Bottom Section — Positions & Logs

#### Positions Tab
A live table of all open futures positions across all accounts:

| Column | Description |
|---|---|
| Account | Which API account holds the position. Manual positions show a **"Manual"** badge. |
| Symbol | The trading pair |
| Size | Position size. Positive = LONG, Negative = SHORT |
| Entry Price | Average fill price of the initial entry order |
| Unrealized PnL | Live profit/loss — green if positive, red if negative |
| Action | **Close** button — immediately cancels all orders and closes position at market |

> Both bot-managed and manually opened positions appear here. Manual positions are slightly dimmed and labelled with a "Manual" badge.

#### Console Log Tab
A scrollable real-time log of all bot activity. It provides enhanced feedback, including:
- **Localized Logs**: Fully translated in Portuguese or English.
- **Account Identification**: Every log entry is prefixed with the account name (e.g., `[Account 1]`).
- **Detailed Order Tracking**: Explicitly logs order types, quantities, and prices (e.g., `Placing BUY LIMIT order for BTCUSDC: Qty 0.025 @ Price 50001`).
- **Fill Confirmation**: Real-time confirmation of order execution and subsequent grid placement.

---

## Configuration Reference

All settings live in **`config.json`**. The dashboard edits this file in real time.

```json
{
  "api_accounts": [
    {
      "name": "User 1",
      "api_key": "YOUR_BINANCE_API_KEY",
      "api_secret": "YOUR_BINANCE_API_SECRET",
      "enabled": true
    }
  ],
  "is_demo": true,
  "language": "en-US",
  "symbols": ["LINKUSDC", "BTCUSDC"],
  "symbol_strategies": {
    "LINKUSDC": {
      "direction": "LONG",
      "entry_price": 8.565,
      "leverage": 75,
      "margin_type": "CROSSED",
      "price_deviation": 0.6,
      "total_fractions": 8,
      "trade_amount_usdc": 100,
      "use_existing_assets": true,
      "trailing_enabled": true,
      "trailing_deviation": 1.0
    }
  }
}
```

### Full Settings Reference

| Field | Type | Description |
|---|---|---|
| `api_accounts[].name` | string | Display name for this account in the dashboard |
| `api_accounts[].api_key` | string | Binance Futures API Key |
| `api_accounts[].api_secret` | string | Binance Futures API Secret |
| `api_accounts[].enabled` | bool | `true` activates this account for trading |
| `is_demo` | bool | `true` = Binance Testnet (safe for testing), `false` = Live trading |
| `language` | string | Dashboard language: `"pt-BR"` (Portuguese) or `"en-US"` (English) |
| `symbols` | array | List of trading pairs (must be valid Binance Futures symbols) |
| **Per-symbol settings** | | |
| `direction` | string | `"LONG"` (buy initially) or `"SHORT"` (sell initially) |
| `entry_price` | float | Price at which the initial limit order is placed |
| `leverage` | int | Leverage multiplier — automatically capped at exchange max for that symbol |
| `margin_type` | string | `"CROSSED"` (cross margin) or `"ISOLATED"` (isolated margin) |
| `price_deviation` | float | % price gap between each TP grid level (e.g., `0.6` = 0.6%) |
| `total_fractions` | int | Number of TP levels (e.g., `8` splits position into 8 equal parts) |
| `trade_amount_usdc` | float | USDC margin to risk (collateral, not notional size) |
| `use_existing_assets` | bool | `true` = attach grid to existing position instead of placing a new entry |
| `trailing_enabled` | bool | Enable trailing P&L exit on top of the grid |
| `trailing_deviation` | float | % drop from peak P&L to trigger full position close |

> ⚠️ **Warning:** All dashboard changes save to `config.json` immediately. If the bot is running, changes are applied live without a restart.

---

## Installation & Running Locally

### Prerequisites
- Python 3.10+
- A Binance account with Futures trading enabled
- An API key with **Futures trading permissions** (and optionally **testnet** access for safe testing)

### Steps

**1. Install dependencies:**
```bash
pip install flask flask-socketio python-binance
```

**2. Configure your API keys:**

Edit `config.json` and add your API key and secret. Start with `"is_demo": true` to use the Binance Testnet and avoid risking real funds.

**3. Run the server:**
```bash
python app.py
```

**4. Open your browser:**
```
http://localhost:3000
```

**5. Configure your strategy** on the dashboard, then click **Start** when ready.

---

## ☁️ Deploy to Railway (Recommended)

[Railway](https://railway.app) is the easiest way to run this bot 24/7 in the cloud without keeping your computer on. Once deployed, you get a public URL to access the dashboard from anywhere.

### Step 1 — Prepare Your Repository

1. Create a free account at [github.com](https://github.com).
2. Create a **new private repository** (keep it private to protect your API keys!).
3. Compress the entire `NIGHT/` project folder into a `.zip` file.
4. Upload the zip or push the files to your GitHub repo.

> 💡 Make sure `config.json` is included but has your API keys already filled in, **or** use environment variables (see below).

### Step 2 — Deploy on Railway

1. Go to [railway.app](https://railway.app) and sign in with GitHub.
2. Click **"New Project"** → **"Deploy from GitHub repo"**.
3. Select your repository.
4. Railway will automatically detect it's a Python project.

### Step 3 — Configure the Start Command

In Railway's project settings, set the **Start Command** to:
```
python app.py
```

And make sure you have a `requirements.txt` file in the project root:
```
flask
flask-socketio
python-binance
gunicorn
eventlet
```

### Step 4 — Set Environment Variables (Optional but Recommended)

Instead of storing API keys in `config.json`, you can use Railway environment variables:
- In Railway → your project → **Variables** tab
- Add `PORT=3000`

Railway automatically assigns a public URL like:
```
https://your-project-name.up.railway.app
```

### Step 5 — Access Your Dashboard

Click the generated URL in Railway. Your bot dashboard will be live at that URL. You can configure and start the bot from anywhere in the world.

> 🔒 **Security tip:** Since the dashboard has no authentication by default, keep your Railway URL private or add a password layer above it (e.g., via Railway's built-in domain settings or a reverse proxy).

---

## Architecture

```
NIGHT/
├── app.py              # Flask server, Socket.IO handlers, REST API routes
├── bot_engine.py       # Core trading engine, threading, Binance API communication
├── config.json         # Live configuration (edited by dashboard in real time)
├── translations_py.py  # Backend log message translations (pt-BR / en-US)
├── binance_bot.log     # Auto-generated log file
├── requirements.txt    # Python dependencies
├── templates/
│   └── dashboard.html  # Single-page dashboard UI (HTML structure)
└── static/
    ├── css/style.css   # Dark theme + all custom styling
    └── js/app.js       # All frontend logic, Socket.IO listeners, UI updates
```

### Key Threads
| Thread | Purpose |
|---|---|
| Flask/SocketIO main thread | Serves HTTP + WebSocket to browser |
| `_global_background_worker` | Fetches prices, leverage, exchange info every ~1s |
| `ThreadedWebsocketManager` (per account) | Listens for real-time fills and account updates from Binance |
| `_symbol_logic_worker` (per account × symbol) | Manages grid state and re-entry logic per symbol |

### Key Socket.IO Events

| Event | Direction | Description |
|---|---|---|
| `price_update` | Server → Browser | Live price map for all symbols |
| `max_leverages` | Server → Browser | Exchange-enforced max leverage per symbol |
| `account_update` | Server → Browser | Balance, equity, PnL, open positions (including manual) |
| `console_log` | Server → Browser | Real-time translated log entries |
| `bot_status` | Server → Browser | Whether the bot engine is currently running |
| `start_bot` | Browser → Server | User clicked Start |
| `stop_bot` | Browser → Server | User clicked Stop |
| `close_trade` | Browser → Server | User clicked Close on a position |

### REST API Endpoints
| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Serves the dashboard HTML |
| `/api/config` | GET | Returns the current `config.json` |
| `/api/config` | POST | Saves updated config and applies live if bot is running |
| `/api/close_position` | POST | Closes a specific position at market |
| `/api/test_connection` | POST | Tests API key connectivity |

---

## Important Notes

- **Leverage is automatically clamped** to the maximum allowed by Binance for each symbol. You cannot accidentally set 100x on a symbol with a 75x max.
- **Order precision is automatically handled.** Quantity and price are formatted exactly to the exchange's `stepSize` and `tickSize` requirements.
- **All settings are per-symbol.** LINKUSDC and BTCUSDC can have completely different strategies, leverages, and deviation settings running simultaneously.
- **The bot continues working on orders even if the dashboard is closed.** Orders are on the Binance order book — the bot just listens for fill notifications via WebSocket.
- **Start with Testnet.** Set `"is_demo": true` in config.json to trade safely on the Binance Futures Testnet before going live.
