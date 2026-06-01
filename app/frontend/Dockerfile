# syntax=docker/dockerfile:1.7
# Multi-stage build for the Agent4DA Analytics Console frontend.

FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund

FROM node:20-alpine AS builder
WORKDIR /app
ARG NEXT_PUBLIC_API_BASE_URL=http://localhost:8083
ENV NEXT_TELEMETRY_DISABLED=1
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    HOSTNAME=0.0.0.0 \
    PORT=3000

# Next.js standalone output keeps the image small.
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

RUN addgroup -S app && adduser -S app -G app
USER app

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=3s --retries=5 --start-period=15s \
  CMD wget -qO- http://127.0.0.1:3000/ask >/dev/null 2>&1 || exit 1

CMD ["node", "server.js"]
