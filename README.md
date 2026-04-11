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
- `GET /api/options/states`
- `GET /api/options/districts?stateId=...`
- `GET /api/options/blocks?districtId=...`
- `GET /api/options/villages?districtId=...&blockId=...`
- `GET /api/options/classRanges`
- `GET /api/schools?page=1&pageSize=25&search=...`
- `GET /api/access?phone=...` (resolve mock phone permissions)
- `GET /api/access/phones` (list configured phone access profiles)
- `POST /api/access/phones` (upsert phone profile: `phone`, `role`, `blockIds`)
- `DELETE /api/access/phones/:phone`
- `POST /api/edits`
- `GET /api/edits`
- `GET /api/edits/school/:sourceKey`
- `POST /api/edits/:id/approve`
- `POST /api/edits/:id/reject`

## Mock phone access model

- Roles: `edit`, `review`
- `review` can edit + approve/reject
- `edit` can submit/re-edit and view own edit outcomes
- If `blockIds` is empty, scope is global
- If `blockIds` is set, data is restricted to those blocks

Pass phone context via query (`?phone=...`) or header:

```http
x-user-phone: 9876543210
```

## Import data with curl

```bash
curl -X POST http://localhost:3001/api/import \
  -F "file=@../28_2811.xlsx"
```
