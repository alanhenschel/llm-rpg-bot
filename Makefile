.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up down build logs seed-rag ps restart pull-model qr gateway-logs bot-logs clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Build (if needed) and start the whole stack
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Stack is starting. Useful URLs:"
	@echo "  Dashboard   : http://localhost:$${FRONTEND_PORT:-3000}"
	@echo "  Mgmt API    : http://localhost:$${MANAGEMENT_API_PORT:-9000}/docs"
	@echo "  LLM bot API : http://localhost:$${LLM_BOT_PORT:-8000}/docs"
	@echo "  Grafana     : http://localhost:$${GRAFANA_PORT:-3001}"
	@echo ""
	@echo "Next: run 'make pull-model' (first time) then 'make seed-rag', then 'make qr'."

down: ## Stop and remove containers (keeps volumes)
	$(COMPOSE) down

clean: ## Stop and remove containers AND volumes (full reset)
	$(COMPOSE) down -v

build: ## Build all images
	$(COMPOSE) build

logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

gateway-logs: ## Tail the WhatsApp gateway logs (where QR codes appear)
	$(COMPOSE) logs -f whatsapp-gateway

bot-logs: ## Tail the LLM bot logs
	$(COMPOSE) logs -f llm-bot

ps: ## Show service status
	$(COMPOSE) ps

restart: ## Restart all services
	$(COMPOSE) restart

pull-model: ## Pull the llama3 model into Ollama (first-time, can take minutes)
	$(COMPOSE) exec ollama ollama pull $${OLLAMA_MODEL:-llama3}

seed-rag: ## Embed the RPG seed docs into ChromaDB
	$(COMPOSE) exec llm-bot python seed_rag.py

qr: ## Show recent gateway logs filtered to QR codes to scan
	@echo "Open WhatsApp > Linked Devices > Link a device, then scan the code below:"
	$(COMPOSE) logs whatsapp-gateway | grep -i "scan this QR" || \
		echo "No QR yet. The gateway emits one when it claims an unpaired slot. Try 'make gateway-logs'."
