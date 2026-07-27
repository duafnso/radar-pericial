from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_docker_build_receives_tile_provider_before_vite_build():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    build_command = dockerfile.index("RUN npm ci && npm run frontend:build")

    for variable in ("VITE_MAP_TILE_URL", "VITE_MAP_TILE_ATTRIBUTION"):
        assert dockerfile.index(f"ARG {variable}") < build_command
        assert dockerfile.index(variable, dockerfile.index("ENV ")) < build_command


def test_compose_builds_forward_public_tile_provider_pair():
    development = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    production = (ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8")

    for variable in ("VITE_MAP_TILE_URL", "VITE_MAP_TILE_ATTRIBUTION"):
        assert f"{variable}: ${{{variable}:-}}" in development
        assert f"{variable}: ${{{variable}:?{variable} is required}}" in production


def test_index_uses_the_existing_logo_as_a_svg_favicon():
    index_html = (ROOT / "index.html").read_text(encoding="utf-8")

    assert 'rel="icon"' in index_html
    assert 'type="image/svg+xml"' in index_html
    assert 'href="/frontend/src/assets/radar-pericial-logo.svg"' in index_html
    assert (ROOT / "frontend" / "src" / "assets" / "radar-pericial-logo.svg").is_file()
