# AnySeek web console

**English** · [简体中文](README.md)

The console uses a same-origin gateway to access AnySeek. It supports collection management, record writes, search debugging, and service backup and restoration. The browser only selects a tenant namespace. It neither issues nor retains tenant keys and does not hold the AnySeek master key. Origence or the gateway handles user login and permissions. The gateway verifies the selected tenant and administrative permissions before injecting the service credential into requests to AnySeek. AnySeek exposes neither user-login nor tenant-key management endpoints.

## Run the UI locally

See the [service guide](../anyseek/src/server/README.en.md#build-and-start) to build and run the API. Run the following in `console/`.

```sh
npm ci
API_TARGET=http://your-authenticated-gateway npm run dev
```

Open the address shown in the terminal, enter a tenant namespace, and connect. `GET /api/v1/index` retrieves the collection list. Namespaces match `^[a-z0-9][a-z0-9_-]{0,62}$` (including UUIDs) and require no AnySeek registration.

For local development, connect directly to a locally bound AnySeek instance. The **Vite server** injects the service credential.

```sh
export API_TARGET=http://127.0.0.1:8080
export ANYSEEK_SERVICE_KEY='<configured service API key>'
npm run dev
```

Windows PowerShell：

```powershell
$env:API_TARGET='http://127.0.0.1:8080'
$env:ANYSEEK_SERVICE_KEY='<configured service API key>'
npm run dev
```

`ANYSEEK_SERVICE_KEY` is the output of `print_master_key=true` from the service guide. It stays in the development proxy process environment and never enters the frontend build or browser storage. By default, the dev server listens only on `127.0.0.1`. Use an authenticated gateway in deployments and keep the fully privileged development proxy off public endpoints.

## Build and deploy

```sh
npm run build
```

Static files are built into `dist/`. In production, place the UI and `/api/` behind the same authenticated gateway to preserve the login session. The gateway verifies or rewrites `X-AnySeek-Tenant` and connects to AnySeek using its own service credential. The browser-provided tenant header is a namespace selector and cannot authorize a user. The gateway must restrict service backup and restoration to administrators.

In Docker, set `API_UPSTREAM` to the authenticated gateway. This variable is used at runtime by the Nginx proxy and never enters JS. The Rust API only exposes data endpoints. The root path `/` does not host the console. Web Crypto requires HTTPS or local `http://localhost` / `http://127.0.0.1`. Connection fingerprints bind to origin and tenant.

## Retries and sessions

Tenant selection is stored in the current tab's sessionStorage. Write requests persist the original operation ID and body before sending. After timeouts or unknown outcomes, retries reuse the original request and ID. Switching tenants does not automatically replay another tenant's unconfirmed requests. Service backup and restoration also retain the original request for confirmation. During upgrades, obsolete tenant keys and issuance-flow session entries are cleared, and standalone record retry data is kept. Pending writes from old connections do not bind automatically to a new namespace.

Model credentials for the embedding provider are configured separately from AnySeek service authentication and follow the existing session and explicit save rules.

## Verification

```sh
npm test
npm run build
npx playwright test --config=playwright.bootstrap.config.ts
ANYSEEK_SERVER=/absolute/path/to/anyseek_server npx playwright test
```

Tests against a real service use an isolated temporary root. The service credential is used only by the test service and proxy. The browser uses tenant selection. The tests do not contact a running user service or any external S3 bucket.
