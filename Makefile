unit-test:
	go test ./... --cover -v

bootstrap-test-env:
	cd ./testinfra/dynamodb-go && docker compose up -d && cd ../..
	pip install -r ./testdata/requirements.txt
	python ./testdata/migration.py InvoiceAndBills
	python ./testdata/seed_data.py InvoiceAndBills ./testdata/data.chunk0.csv
	python ./testdata/seed_data.py InvoiceAndBills ./testdata/data.chunk1.csv

remove-test-env:
	cd ./testinfra/dynamodb-go && docker compose down --remove-orphans

integration-test:
	go test ./... --cover -tags=integration

generate-coverage:
	go test ./... -coverprofile coverage.out . && go tool cover -html=coverage.out

generate-integration-coverage:
	go test ./... -tags=integration -coverprofile coverage.out . && go tool cover -html=coverage.out
