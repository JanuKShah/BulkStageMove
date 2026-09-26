# syntax=docker/dockerfile:1
FROM node:22-alpine AS base
WORKDIR /app

# --- deps ---
FROM base AS deps
COPY package.json package-lock.json* ./
RUN npm ci --include=dev

# --- build all four services into one dist ---
FROM deps AS build
COPY tsconfig.json tsconfig.build.json nest-cli.json ./
COPY src ./src
RUN npm run build

# --- migration job: needs tsx, which is a devDependency, so it gets its own
# stage rather than bloating the production image with build tooling ---
FROM base AS migrate
COPY package.json package-lock.json* ./
RUN npm ci --include=dev
COPY tsconfig.json ./
COPY scripts ./scripts
COPY migrations ./migrations
CMD ["npm", "run", "migrate"]

# --- runtime: prod deps only, all four entrypoints available ---
FROM base AS runtime
ENV NODE_ENV=production
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --from=build /app/dist ./dist

# No single CMD: docker-compose overrides `command` per service.
CMD ["node", "dist/apps/opportunity-service/main.js"]
