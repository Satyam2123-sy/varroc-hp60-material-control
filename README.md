# VARROC POLYMERS • PLANT 3 • HP60 MATERIAL CONTROL

Production-ready Node.js + Express + Socket.IO material-control dashboard for Plant 3 / HP60.

## Run locally

```bash
npm install
npm start
```

The server uses `process.env.PORT` when provided and listens on `0.0.0.0`.

## Project structure

```text
server.js
package.json
README.md
public/
  index.html
  sw.js
  varroc-logo.jpg
```

## Render

Use:

- Build Command: `npm install`
- Start Command: `npm start`
- Root Directory: leave blank

The repository root must contain `server.js` and `package.json`.

## Health endpoints

- `/health`
- `/build`

## Features

- Owner/member room permissions
- Server-side room state and persistence abstraction
- Socket.IO live synchronization and reconnect/rejoin
- K2 REAR / K2 FRONT material tracking
- 7:00 AM–7:00 PM and 7:00 PM–7:00 AM shifts
- Breakdown and shift manpower tracking
- Shift-filtered printable reports
- Service worker for application-shell recovery
