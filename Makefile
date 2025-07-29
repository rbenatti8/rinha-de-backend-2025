build-api:
	@docker build --target api -t rinha-backend-2025-api:latest . && docker tag rinha-backend-2025-api:latest rbenatti8/rinha-backend-2025-api:latest

push-image:
	@docker push rbenatti8/rinha-backend-2025-api:latest

remove-containers:
	@docker rm -f $$(docker ps -aq)

setup-test:
	@cd scripts/docker && docker compose up -d && cd .. && docker compose up -d
k6-run:
	cd scripts/k6 && k6 run -e MAX_REQUESTS=550 rinha.js
tools:
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-geproton-go-grpc@latest
