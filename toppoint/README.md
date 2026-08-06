# Toppoint V4 XML → Dropbox

Scarica i feed XML Toppoint V4 da AWS S3 e li carica nella cartella Dropbox `/Toppoint`, sovrascrivendo la versione precedente.

## Pianificazione

- `stock.xml`: ogni giorno alle 04:30 UTC
- `products.xml`, `print.xml`, `printprices.xml`: ogni lunedì alle 05:00 UTC
- avvio manuale da GitHub Actions con modalità `all`, `stock` o `weekly`

Il feed Toppoint viene cercato sotto:

- bucket: `toppoint-xml`
- prefisso: `EUR/V4`
- regione: `eu-north-1`

Lo script elenca i file presenti e riconosce automaticamente i nomi equivalenti `product/products` e `printprice/printprices`. I file vengono salvati in Dropbox con nomi stabili:

- `stock.xml`
- `products.xml`
- `print.xml`
- `printprices.xml`

## Segreti GitHub richiesti

In `Settings → Secrets and variables → Actions` aggiungere:

- `TOPPOINT_AWS_ACCESS_KEY_ID`
- `TOPPOINT_AWS_SECRET_ACCESS_KEY`

Il repository usa inoltre i segreti Dropbox già esistenti:

- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

Le credenziali non devono essere inserite nei file del repository.

## Primo test

Dopo aver aggiunto i due segreti Toppoint:

1. aprire `Actions`
2. scegliere `Toppoint XML sync`
3. premere `Run workflow`
4. scegliere `all`
5. verificare la presenza dei quattro XML nella cartella Dropbox `Toppoint`

Se i nomi reali presenti su S3 differiscono da quelli attesi, il log mostra l'elenco completo dei file disponibili senza stampare le credenziali.
