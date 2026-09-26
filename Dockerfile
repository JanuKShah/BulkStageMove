# syntax=docker/dockerfile:1
FROM node:22-alpine AS base
WORKDIR /app
ENV NODE_ENV=production

# --- deps: install with dev deps so nest build can run ---
FROM base AS deps
COPY package.json package-lock.json* ./
RUN npm ci --include=dev

# --- build ---
FROM deps AS build
COPY tsconfig.json tsconfig.build.json nest-cli.json ./
COPY src ./src
RUN npm run build

# --- runtime: dev deps stripped, dist only ---
FROM base AS runtime
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --from=build /app/dist ./dist

EXPOSE 3000
CMD ["node", "dist/main.js"]
