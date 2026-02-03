# Midocean → CSV → Dropbox

NB ogni paio di mesi modifico questo file per non far "addormentare" il repo di  github

- **stock.csv**: aggiornato ogni 6 ore
- **generale.csv**: aggiornato ogni settimana (lun 04:00 Europe/Rome)
- **print.csv**: aggiornato ogni settimana (lun 04:15 Europe/Rome)

Output in **CSV** con separatore `,`, encoding UTF-8, **virgola decimale** (it-IT) dove applicabile.

Upload su Dropbox (cartella configurabile) con **refresh token** (token long‑lived automatico).

## Segreti richiesti (GitHub → Settings → Secrets and variables → Actions → *New repository secret*)

> ⚠️ **Non committare mai i segreti nel codice.**

Obbligatori:
- `MIDOCEAN_API_KEY` – API key REST (per /gateway/*)
- `DROPBOX_APP_KEY` – App key Dropbox
- `DROPBOX_APP_SECRET` – App secret Dropbox
- `DROPBOX_REFRESH_TOKEN` – Refresh token OAuth2 (offline)

Opzionali / fallback:
- `MIDOCEAN_CUSTOMER_NUMBER` – per eventuali WS legacy
- `MIDOCEAN_LOGIN`
- `MIDOCEAN_PASSWORD`

## Variabili (GitHub → Secrets and variables → *Variables*)
- `DROPBOX_BASE_PATH` – es. `/Public/midocean` (predef.: `/Public/midocean`)
- `MIDOCEAN_BASE_URL` – predef.: `https://api.midocean.com`
- `MIDOCEAN_LANGUAGE` – predef.: `it`
- `SUPPLIER_NAME` – predef.: `Mid Ocean Brands`

## File prodotti

### `stock.csv`
Colonne (ordine fisso):
- `supplier`, `date`, `time`, `sku`, `qty`, `first_arrival_date`, `first_arrival_qty`, `next_arrival_date`, `next_arrival_qty`

> Per i **textile**: se `qty == 0` ma `first_arrival_date == oggi`, allora `qty` viene impostato a `first_arrival_qty` (quantità realmente vendibile), mantenendo comunque i campi arrival espliciti.

### `generale.csv`
Colonne allineate al tuo esempio (flatten v2.0). Alcuni alias:
- `products__product__product_number` = `sku`
- `products__product__product_base_number` = `master_code`
- `products__product__product_id` = `variant_id`
- `products__product__product_print_id` = `master_id`
- … più tutte le dimensioni, pesi, categorie, immagini, asset digitali e traduzioni come in sample.

### `print.csv`
Righe per **(master_code × colore × posizione × tecnica)**.
Colonne: `products__product__product_base_number`, `products__product__product_print_id`, `products__product__print_express_possible`, `products__product__item_color_number`, `products__product__manipulation`, `products__product__pps__pp__id`, `products__product__pps__pp__printing_size_unit`, `products__product__pps__pp__max_print_size_height`, `products__product__pps__pp__max_print_size_width`, `products__product__pps__pp__print_position_url__color`, `products__product__pps__pp__print_position_url__text`, `products__product__pps__pp__printing_technique__id`, `products__product__pps__pp__printing_technique__max_colors`, `products__product__print_position_document`.

`print_express_possible` impostato a `N` (il dato non è presente nell’API printdata 1.0; se in futuro comparirà, il campo verrà popolato automaticamente).

## Robustezza
- Retry con **exponential backoff + jitter** per 429/5xx
- Timeout e validazione payload (schema base / chiavi attese)
- Header `Accept: text/json` (fallback a `text/xml`)
- Prova automatica di key header `X-API-Key` → `x-api-key` → `apikey` (nel caso l’ambiente esiga naming diverso)
- Locale it-IT per **virgola decimale**
- Upload Dropbox con **sessioni chunked** per file grandi
- Log strutturati in stdout (Actions) + exit code coerenti

## Run locale
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MIDOCEAN_API_KEY=***
export DROPBOX_APP_KEY=***
export DROPBOX_APP_SECRET=***
export DROPBOX_REFRESH_TOKEN=***
python scripts/stock_to_csv.py        # genera e carica stock.csv
python scripts/product_general_to_csv.py  # genera e carica generale.csv
python scripts/print_to_csv.py        # genera e carica print.csv
```
