.PHONY: bootstrap up down streaming simulate demo test logs clean rebuild

bootstrap:
	@echo "Running ALS bootstrap (downloads MovieLens 1M if needed)…"
	docker compose build spark
	docker compose run --rm spark python /opt/spark-jobs/bootstrap.py

streaming:
	@echo "Restarting Spark streaming (single instance managed by compose)…"
	docker compose restart spark
	@echo "Streaming restarted. Follow logs: make logs"

up:
	docker compose up -d --build
	@echo ""
	@echo "  Frontend  →  http://localhost:3000"
	@echo "  API       →  http://localhost:8000"
	@echo "  Grafana   →  http://localhost:3001  (admin/admin)"

rebuild:
	docker compose build --no-cache
	@echo "Images rebuilt from current source. Run 'make up' next."

down:
	docker compose down

simulate:
	docker compose exec event-simulator python simulator.py --users 10 --events 100

demo:
	@xdg-open http://localhost:3000 2>/dev/null || open http://localhost:3000 2>/dev/null || \
	  echo "Open http://localhost:3000"

test:
	uv run pytest tests/ -v

logs:
	docker compose logs -f spark event-simulator api

clean:
	docker compose down -v
	rm -rf data/ml-1m data/ml-1m.zip
