test:
	@go test --v;

test_with_coverage:
	@go test -coverprofile cover.out && go tool cover -html=cover.out;