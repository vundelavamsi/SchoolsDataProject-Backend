# Backend

Express API for school filtering with SQLite storage and Excel data import.

## Setup

```bash
npm install
npm run dev
```

Runs at `http://localhost:3001` by default.

## Scripts

- `npm run dev` - start backend server
- `npm run start` - start backend server

## API endpoints

- `GET /health`
- `POST /api/import` (multipart/form-data with `file=@<excel-file>.xlsx`)
- `GET /api/options`
- `GET /api/options/blocks?districtId=...`
- `GET /api/options/villages?districtId=...&blockId=...`
- `GET /api/schools?page=1&pageSize=25&search=...`

## Import data with curl

```bash
curl -X POST http://localhost:3001/api/import \
  -F "file=@../28_2811.xlsx"
```
