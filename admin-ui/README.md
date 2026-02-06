# Go Worker Admin UI

This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Configuration

Create `admin-ui/.env.local` (or copy `admin-ui/.env.example`) to track the UI config in one place. Next.js loads `.env.local` automatically.

The admin UI now talks to the worker admin gateway (HTTP/JSON over mTLS). Configure the gateway and UI hints via env vars:

- `WORKER_ADMIN_API_URL` (e.g. `https://127.0.0.1:8081`)
- `WORKER_ADMIN_MTLS_CERT`, `WORKER_ADMIN_MTLS_KEY`, `WORKER_ADMIN_MTLS_CA` (client certs for mTLS)
- `WORKER_ADMIN_SCHEDULES_JSON` (JSON array of schedule objects)
- `WORKER_ADMIN_ALLOW_MOCK=false` (disable mock fallback in non-production)
- `NEXT_PUBLIC_WORKER_ADMIN_ORIGIN` (optional override for API fetch origin)
- `WORKER_ADMIN_PASSWORD` (required to sign into the UI)

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
