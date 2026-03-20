# Rix Credit API

Production API for Companies House + OCR financial extraction.

## Endpoints

- /healthz
- /rix-credit/company/{company_number}
- /latest-accounts-financials

## Deploy

```bash
fly launch
fly deploy
---

## 10. Environment Variables (IMPORTANT)

Set in Fly:
41119f34-8c9d-4e54-b2f9-b7b92dc9563c
---

## 11. Deployment Steps

```bash
git init
git add .
git commit -m "initial commit"

git remote add origin https://github.com/YOUR_USERNAME/rix-credit-api.git
git push -u origin main

fly auth login
fly launch
fly deploy
