#!/bin/bash

# Purple Sector - Development Environment Startup Script
#
# This script starts the complete development environment:
#   1. Docker infrastructure (Redpanda, gRPC Gateway, RisingWave, UDF,
#      Redis, MinIO, Trino, Postgres) via docker-compose.dev.yml
#   2. RisingWave schema initialization
#   3. Demo replay binary build (if needed)
#   4. PM2 services (Vite frontend + Django API + demo replay)
#
# Usage: ./scripts/start-dev.sh [--install]
#   --install   Run npm install before starting PM2 services

set -e

INSTALL_DEPS=false
for arg in "$@"; do
  case "$arg" in
    --install) INSTALL_DEPS=true ;;
  esac
done

echo "🚀 Starting Purple Sector Development Environment"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# ── Step 1: Docker infrastructure ──────────────────────────────────────

if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}❌ Docker is not running. Please start Docker first.${NC}"
  exit 1
fi

# Check if the stack is already running (use Redpanda container as canary)
if docker ps --format '{{.Names}}' | grep -q ps-redpanda; then
  echo -e "${GREEN}✓ Docker infrastructure already running${NC}"
else
  echo -e "${YELLOW}⏳ Starting Docker infrastructure...${NC}"
  docker compose -f docker-compose.dev.yml up -d

  echo -e "${YELLOW}⏳ Waiting for services to be healthy...${NC}"
  # Wait for Redpanda (Kafka API) — everything else depends on it
  for i in $(seq 1 30); do
    if docker exec ps-redpanda rpk cluster health > /dev/null 2>&1; then
      echo -e "${GREEN}✓ Redpanda is healthy${NC}"
      break
    fi
    [ "$i" -eq 30 ] && echo -e "${RED}⚠ Redpanda health check timed out${NC}"
    sleep 2
  done

  # Wait for Postgres
  for i in $(seq 1 15); do
    if docker exec ps-postgres pg_isready -U purplesector > /dev/null 2>&1; then
      echo -e "${GREEN}✓ Postgres is healthy${NC}"
      break
    fi
    [ "$i" -eq 15 ] && echo -e "${RED}⚠ Postgres health check timed out${NC}"
    sleep 2
  done

  # Wait for WebSocket server
  for i in $(seq 1 15); do
    if docker exec ps-ws-server node -e "const s=require('net').connect(8080,'localhost',()=>{s.end();process.exit(0)});s.on('error',()=>process.exit(1))" > /dev/null 2>&1; then
      echo -e "${GREEN}✓ WebSocket server is healthy${NC}"
      break
    fi
    [ "$i" -eq 15 ] && echo -e "${RED}⚠ WebSocket server health check timed out${NC}"
    sleep 2
  done

  echo -e "${GREEN}✓ Docker infrastructure started${NC}"

  # Create Redpanda topics before RisingWave initialization
  echo -e "${YELLOW}⏳ Creating Redpanda topics...${NC}"
  docker exec ps-redpanda rpk topic create telemetry-batches --partitions 3 --replicas 1 || true
  echo -e "${GREEN}✓ Redpanda topics ready${NC}"
  
  # Initialize Iceberg tables before RisingWave initialization
  echo -e "${YELLOW}⏳ Initializing Iceberg tables...${NC}"
  ./scripts/init-iceberg.sh
  echo -e "${GREEN}✓ Iceberg tables initialized${NC}"
  
fi

# ── Step 2: Database check ─────────────────────────────────────────────

# ── Step 2: RisingWave schema ──────────────────────────────────────────
# Always (re-)apply the schema so changes to SQL files take effect
# without needing to tear down Docker. All scripts use DROP IF EXISTS
# and CREATE ... IF NOT EXISTS so they are safe to run repeatedly.
echo -e "${YELLOW}⏳ Initializing RisingWave schema...${NC}"
./scripts/init-risingwave.sh
echo -e "${GREEN}✓ RisingWave schema initialized${NC}"

# ── Step 3: Postgres SQL migrations ───────────────────────────────────
# Apply triggers and functions that can't be expressed in the Prisma schema.
# Runs after db:push so application tables exist.
echo -e "${YELLOW}⏳ Applying Postgres SQL migrations...${NC}"
./scripts/init-postgres.sh
echo -e "${GREEN}✓ Postgres SQL migrations applied${NC}"

# ── Step 3b: Python venv setup ────────────────────────────────────────
if [ ! -f "apps/web/.venv/bin/uvicorn" ]; then
  echo -e "${YELLOW}⏳ Setting up Python virtual environment...${NC}"

  # Ensure uv is available; install it if not
  if ! command -v uv > /dev/null 2>&1; then
    echo -e "${YELLOW}  uv not found — installing via pip...${NC}"
    python3 -m pip install --user uv
    # Reload PATH so the newly installed uv is visible
    export PATH="$HOME/.local/bin:$PATH"
  fi

  if command -v uv > /dev/null 2>&1; then
    (cd apps/web && uv venv && uv pip install -e '.[ai]')
  else
    echo -e "${YELLOW}  uv still unavailable — falling back to venv + pip...${NC}"
    python3 -m venv apps/web/.venv
    apps/web/.venv/bin/pip install --quiet --upgrade pip
    apps/web/.venv/bin/pip install --quiet -e 'apps/web[ai]'
  fi

  if [ ! -f "apps/web/.venv/bin/uvicorn" ]; then
    echo -e "${RED}❌ Failed to set up Python venv — uvicorn still missing.${NC}"
    echo -e "${RED}   Try manually: cd apps/web && uv venv && uv pip install -e '.[ai]'${NC}"
    exit 1
  fi
  echo -e "${GREEN}✓ Python virtual environment ready${NC}"
else
  echo -e "${GREEN}✓ Python virtual environment already set up${NC}"
fi

# ── Step 3c: Django migrations ────────────────────────────────────────
if [ -d "apps/web/.venv" ]; then
  echo -e "${YELLOW}⏳ Running Django migrations...${NC}"
  (cd apps/web && .venv/bin/python manage.py migrate --run-syncdb 2>&1 | tail -5)
  echo -e "${GREEN}✓ Django migrations applied${NC}"

  echo -e "${YELLOW}⏳ Seeding dev users...${NC}"
  (cd apps/web && .venv/bin/python manage.py seed_dev_users)
  echo -e "${GREEN}✓ Dev users ready${NC}"

  # ── Step 3c: Create RisingWave sink to Postgres ────────────────────
  # This must run AFTER Django migrations because the sink validates that
  # the target table (laps) exists in Postgres.
  echo -e "${YELLOW}⏳ Creating RisingWave sink to Postgres...${NC}"
  (cd apps/web && .venv/bin/python ../../scripts/create-rw-sink.py 2>&1)
fi

# ── Step 6: Build demo replay binary ───────────────────────────────────

if [ ! -f "rust/target/release/ps-demo-replay" ]; then
  echo -e "${YELLOW}⏳ Building demo replay binary (first time only)...${NC}"
  cd rust && cargo build --release -p ps-demo-replay && cd ..
  echo -e "${GREEN}✓ Demo replay binary built${NC}"
else
  echo -e "${GREEN}✓ Demo replay binary exists${NC}"
fi

# ── Step 7: Node dependencies ─────────────────────────────────────────
if [ "$INSTALL_DEPS" = true ]; then
  echo -e "${YELLOW}⏳ Installing Node dependencies (npm install)...${NC}"
  npm install
  echo -e "${GREEN}✓ Node dependencies installed${NC}"
fi

# ── Step 8: PM2 services ──────────────────────────────────────────────

mkdir -p logs

echo -e "${YELLOW}⏳ Cleaning up existing PM2 processes...${NC}"
npx pm2 delete all > /dev/null 2>&1 || true

# Clean up WAL to prevent replaying old telemetry data
if [ -f "telemetry-wal.db" ]; then
  rm -f telemetry-wal.db telemetry-wal.db-shm telemetry-wal.db-wal
  echo -e "${GREEN}✓ WAL files cleaned${NC}"
fi

echo -e "${YELLOW}⏳ Starting PM2 services...${NC}"
if [ -f ".env" ]; then
  set -a
  source .env
  set +a
fi
npx pm2 start ecosystem.dev.config.js

# ── Done ──────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}✅ Development environment started successfully!${NC}"
echo ""
echo "📊 Service Status:"
npx pm2 status
echo ""
echo "📝 Useful Commands:"
echo "  npx pm2 logs                     - View all PM2 logs"
echo "  npx pm2 logs demo-replay         - View demo replay logs"
echo "  npx pm2 monit                    - Monitor PM2 services"
echo "  docker compose -f docker-compose.dev.yml logs -f  - Docker logs"
echo "  npx pm2 restart all              - Restart PM2 services"
echo "  ./scripts/stop-dev.sh            - Stop everything"
echo "  ./scripts/stop-dev.sh --keep-docker  - Stop PM2 only"
echo ""
echo "🌐 Access Points:"
echo "  Vite dev server:    http://localhost:5173"
echo "  Django API:         http://localhost:8000"
echo "  Django Admin:       http://localhost:8000/admin"
echo "  gRPC Gateway:       localhost:50051"
echo "  Redpanda Console:   http://localhost:8090"
echo "  RisingWave SQL:     localhost:4566"
echo "  RisingWave UI:      http://localhost:5691"
echo "  MinIO Console:      http://localhost:9001"
echo "  Trino:              http://localhost:8083"
echo "  Postgres:           localhost:5432"
echo "  Redis:              localhost:6379"
echo "  WebSocket:          ws://localhost:8080"
echo ""
echo "🎯 Demo Telemetry:"
echo "  Demo replay is running and streaming shared telemetry data"
echo "  Create a demo session in the UI to see live data"
echo "  All users share the same demo telemetry stream"
echo ""
echo -e "${GREEN}🎉 Happy coding!${NC}"
