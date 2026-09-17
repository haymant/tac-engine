---
name: tradeac-alpaca
description: Guide agents to call tac-engine MCP tools for Alpaca trading and market data (news, corporate actions, screener, FX, stocks, options, realtime streams) via MCP Inspector, Cursor, or other clients.
---

# tradeac-alpaca

Use **tac-engine** MCP tools — not raw Alpaca REST — for brokerage ops and market data. Same tool surface will back TradeAC’s Next.js UI later.

## MCP-first policy

- **Prefer the MCP tools registered in this session** (`tac-engine` server, tools listed below) over writing scripts that reimplement them. If a tool exists, call it directly — do not reinvent it with curl/bash/python (raw Alpaca REST, hand-rolled pagination, own JSON-RPC clients).
- **NEVER script directly against the MCP server** (spawning the binary, talking stdio JSON-RPC, or driving it via bash/curl) unless the MCP tool surface genuinely can't do the job — and in that case **stop and ask the user to confirm first** before writing the script.
- If a direct Alpaca call is needed (e.g. an endpoint with no tool), say so and let the user confirm the approach; otherwise keep everything on the MCP surface.

## Hosts / env

| Purpose | Env | Default |
|---------|-----|---------|
| Trading REST | `APCA_BASE_URL` | `https://paper-api.alpaca.markets` |
| Market data REST | `APCA_DATA_BASE_URL` | `https://data.alpaca.markets` |
| Market data WS | `APCA_STREAM_BASE_URL` | `wss://stream.data.alpaca.markets` |
| Auth | `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY` | required |

```bash
cargo build --release
./target/release/tac-engine
```

## Secrets policy

- NEVER write secrets into files: API keys (`APCA_API_KEY_ID`/`APCA_API_SECRET_KEY`), DB passwords, OAuth tokens, or credential-bearing URLs in scripts, configs, notes or committed code.
- NEVER read `*.env` / `.env.*` directly (`cat`/`tail`/`grep`/`sed`/`head` on `.env`). That pulls secrets into this session and leaks them to any agent sharing it.
- When a tool or command needs an env var, ASK the user to set it in the environment (shell/container env, or the user-owned `.env`) and reference it by name (`$VAR`), never by value. If it's missing, report which variable is required instead of reading it yourself.
- If you find a committed secret, flag it, remove it, and replace it with a placeholder.

## MCP clients

### MCP Inspector

```bash
npx @modelcontextprotocol/inspector /absolute/path/to/tac-engine/target/release/tac-engine
```

Connect → `tools/list` → `tools/call` with JSON `arguments`.

### Cursor

```json
{
  "mcpServers": {
    "tac-engine": {
      "command": "/absolute/path/to/tac-engine/target/release/tac-engine",
      "env": {
        "APCA_API_KEY_ID": "${APCA_API_KEY_ID}",
        "APCA_API_SECRET_KEY": "${APCA_API_SECRET_KEY}"
      }
    }
  }
}
```

stdio is NDJSON JSON-RPC; logs on stderr.

## Safety

- Paper trading by default (`APCA_BASE_URL`).
- Mutating trading tools: `place_order`, `close_*`, `cancel_*`, watchlist writes.
- `subscribe_market_stream` opens a **short-lived** WebSocket (samples then disconnects). Most Alpaca plans allow **one** concurrent stream — close other clients first.

## Tool catalog

Lake tools (`get_lake_bars`, `get_lake_ta`, `get_lake_sp`, `get_lake_features`, `backfill_lake_calendar`, …) live under **`tradeac-lake`** (see `tac-engine/skills/tradeac-lake/SKILL.md`). When a lake call's purpose is **backfilling/persisting** (not reading the payload), pass `"quiet": true` so the tool returns a summary (`count`/`first_t`/`last_t`/`source`/`columns`) instead of echoing back the full bar/feature rows.

### Trading (brokerage account)

Account: `get_account`, `get_portfolio_history`, `list_account_activities`, `get_account_activities_by_type`  
Assets master: `list_assets`, `get_asset`  
Watchlists / positions / orders: `list_*`, `get_*`, `create_*`, `place_order`, `close_*`, `cancel_*`

### Market data — news & corporate actions

| Tool | Notes |
|------|------|
| `get_news` | optional `symbols`, `start`/`end`, `limit`, `include_content` |
| `get_corporate_actions` | optional `symbols`, `types`, `start`/`end`, `data_quality` |

### Screener

| Tool | Notes |
|------|------|
| `get_most_actives` | optional `by`=`volume`\|`trades`, `top` |
| `get_market_movers` | required `market_type`=`stocks`\|`crypto`, optional `top` |

### FX

| Tool | Notes |
|------|------|
| `get_forex_latest_rates` | required `currency_pairs` e.g. `USDJPY,EURUSD` |
| `get_forex_rates` | historical; optional `timeframe`, `start`, `end` |

### Stocks

| Tool | Notes |
|------|------|
| `get_stock_bars` / `get_stock_bars_single` | historical; needs `timeframe` |
| `get_stock_latest_bars` | latest minute bars |
| `get_stock_quotes` / `get_stock_latest_quotes` | quotes |
| `get_stock_trades` / `get_stock_latest_trades` | trades |
| `get_stock_snapshots` / `get_stock_snapshot` | trade+quote+bars |
| `get_stock_auctions` | auctions |

Multi-symbol tools take comma-separated `symbols`. Optional `feed` (`iex`/`sip`), `limit`, `page_token`, …

### Options

| Tool | Notes |
|------|------|
| `get_option_bars` | historical bars for contract symbols |
| `get_option_latest_quotes` / `get_option_latest_trades` | latest |
| `get_option_trades` | historical trades |
| `get_option_snapshots` | contracts |
| `get_option_chain` | underlying + filters (`type`, strikes, expiration) |
| `get_option_meta_conditions` / `get_option_meta_exchanges` | code maps |

### Realtime stream sampling

| Tool | Notes |
|------|------|
| `subscribe_market_stream` | `stream`=`stocks`\|`options`\|`news`\|`test`; optional `feed`; channels `trades`/`quotes`/`bars`/`news` as CSV symbols; `duration_secs` (1–30), `max_messages` (1–200) |

Examples:

```json
{"stream":"test","duration_secs":5,"max_messages":20}
```

```json
{"stream":"stocks","feed":"iex","quotes":"AAPL,MSFT","duration_secs":5}
```

```json
{"stream":"news","news":"*","duration_secs":8,"max_messages":30}
```

```json
{"stream":"options","feed":"indicative","quotes":"AAPL250117C00200000","duration_secs":5}
```

## Example workflows

1. **Dashboard:** `get_account` → `list_positions` → `get_stock_snapshots` (`symbols` from positions)
2. **Research:** `get_news` → `get_corporate_actions` → `get_stock_bars`
3. **Screener → trade (paper):** `get_most_actives` → `get_stock_snapshot` → `place_order`
4. **Options:** `get_option_chain` (`underlying_symbol=AAPL`) → `get_option_latest_quotes`
5. **Live sample:** `subscribe_market_stream` with `stream=test` first, then stocks/news

## Protocol

- rmcp **3.1** / MCP **2026-07-28**, stdio
- Prefer these MCP tool names/args over calling Alpaca hosts directly from agents/UI
