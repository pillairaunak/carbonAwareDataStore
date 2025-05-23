api_up: 
	@echo "Starting API server..."
	@docker compose -f docker-compose.yml up --build -d
	@echo "API server is running."

api_down:
	@echo "Stopping API server..."
	@docker compose -f docker-compose.yml down
	@echo "API server has been stopped."