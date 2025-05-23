api_up: 
	@echo "Starting API server..."
	@docker compose -f docker-compose.yml up --build -d api
	@echo "API server is running."

api_down:
	@echo "Stopping API server..."
	@docker compose -f docker-compose.yml down
	@echo "API server has been stopped."

go_app_up:
	@echo "Starting Go application..."
	@docker compose -f docker-compose.yml up --build -d go_app
	@echo "Go application is running."