build-api:
	@docker build --target api -t rinha-backend-2025-api:2.0.0 . && docker tag rinha-backend-2025-api:2.0.0 rbenatti8/rinha-backend-2025-api:2.0.0

build-remote:
	@docker build --target remote -t rinha-backend-2025-remote:1.0.0 . && docker tag rinha-backend-2025-remote:1.0.0 rbenatti8/rinha-backend-2025-remote:1.0.0

push-image:
	@docker push rbenatti8/rinha-backend-2025-api:2.0.0
build-proto:
	@protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/proto/messages.proto
remove-containers:
	@docker rm -f $$(docker ps -aq)

setup-test:
	@cd scripts/docker && docker compose up -d && cd .. && docker compose up -d
k6-run:
	cd scripts/k6 && k6 run -e MAX_REQUESTS=2000 rinha.js
tools:
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-geproton-go-grpc@latest
