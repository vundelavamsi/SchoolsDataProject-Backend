# Backend

Express API for school filtering with SQLite storage and Excel data import.

## Setup

```bash
npm install
npm run import:data -- ../28_2811.xlsx
npm run dev
```

Runs at `http://localhost:3001` by default.

## Scripts

- `npm run dev` - start backend server
- `npm run start` - start backend server
- `npm run import:data -- ../28_2811.xlsx` - import Excel records into SQLite

## API endpoints

- `GET /health`
- `GET /api/options`
- `GET /api/options/blocks?districtId=...`
- `GET /api/options/villages?districtId=...&blockId=...`
- `GET /api/schools?page=1&pageSize=25&search=...`
