Swap the `hadolint-docker` pre-commit hook for the plain `hadolint` binary and add a guard that fails the config if a `language: docker_image` hook is ever reintroduced.
