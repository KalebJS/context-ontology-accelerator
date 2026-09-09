# Web app runtime — build the SPA with node, serve with nginx.
# Build context: repo root (needs workspace deps).
FROM node:22-bookworm-slim AS build

WORKDIR /app

# pnpm workspace: install from the lockfile at the root
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json ./
COPY libs/ts-shared/package.json libs/ts-shared/package.json
COPY packages/web-app/package.json packages/web-app/package.json
COPY packages/mcp-proxy/package.json packages/mcp-proxy/package.json
COPY smithy-generated/control-plane-typescript-client/package.json smithy-generated/control-plane-typescript-client/package.json
COPY smithy-generated/data-layer-typescript-client/package.json smithy-generated/data-layer-typescript-client/package.json

RUN corepack enable && pnpm install --frozen-lockfile --filter coa-web-app...

# Sources needed to build
COPY libs/ts-shared libs/ts-shared
COPY packages/web-app packages/web-app
COPY packages/mcp-proxy packages/mcp-proxy
COPY smithy-generated/control-plane-typescript-client smithy-generated/control-plane-typescript-client
COPY smithy-generated/data-layer-typescript-client smithy-generated/data-layer-typescript-client

RUN pnpm --filter coa-web-app build

# ── runtime ────────────────────────────────────────────────────────────
FROM nginx:1.27-bookworm

COPY --from=build /app/packages/web-app/dist /usr/share/nginx/html
COPY docker/web-app/nginx.conf /etc/nginx/conf.d/default.conf
COPY docker/web-app/render-runtime-config.sh /docker-entrypoint.d/40-render-runtime-config.sh
RUN chmod +x /docker-entrypoint.d/40-render-runtime-config.sh

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=5 \
  CMD curl -sf http://localhost:80/ >/dev/null || exit 1
