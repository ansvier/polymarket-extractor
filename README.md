# Polymarket Market Data Extractor

🔍 Lightweight Python tool for downloading **market and trade data from Polymarket** via the **Gamma API** and **Data API**.  
Fetches all markets by a chosen tag, filters trades since `2025-01-01`,  
and outputs clean CSV files with detailed per-market statistics.

---

## 🚀 Features

- Search tags by keyword (via **Gamma API**)
- Download **all markets** for chosen tag(s)
- Retrieve **all trades** via **Data API**
- Automatic pagination and timeout handling
- Calculates trade metrics per market:
  - `traders_count`
  - `total_trades`
  - `total_volume_usd`
  - `avg_trade_size_usd`
  - `avg_volume_per_trader`
  - `avg_trades_per_day`
  - `min_trade_size_usd`
  - `max_trade_size_usd`
  - `active_days`
- Outputs:
  - `markets_tag_<id>_<timestamp>.csv` — markets for each tag
  - `markets_all_selected_tags_<timestamp>.csv` — merged file for all selected tags

---

## 🧩 Installation

```bash
git clone https://github.com/<your-username>/polymarket-extractor.git
cd polymarket-extractor
pip install -r requirements.txt
