import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / "interface" / "templates" / "index.html"
ASSETS_PATH = ROOT / "interface" / "static" / "assets"
CI_PATH = ROOT / ".github" / "workflows" / "ci.yml"
DOCKERFILE_PATH = ROOT / "Dockerfile"


def test_tracked_frontend_bundle_contains_map_parser_and_favicon():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    script_match = re.search(r'src="/static/assets/(index-[^"]+\.js)"', template)
    favicon_match = re.search(r'href="/static/assets/(radar-pericial-logo-[^"]+\.svg)"', template)

    assert script_match, "tracked template must reference its generated JavaScript bundle"
    assert favicon_match, "tracked template must reference its generated favicon"

    bundle = (ASSETS_PATH / script_match.group(1)).read_text(encoding="utf-8")
    assert (ASSETS_PATH / favicon_match.group(1)).is_file()
    assert "N\u00e3o foi poss\u00edvel carregar o resumo territorial." in bundle
    assert "unpkg.com" not in bundle


def test_ci_pins_linux_frontend_toolchain_and_checks_generated_diff():
    workflow = CI_PATH.read_text(encoding="utf-8")
    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")
    build_step = workflow.index("run: npm run frontend:build")
    diff_step = workflow.index(
        "run: git diff --exit-code -- interface/templates interface/static/assets"
    )

    assert dockerfile.startswith("FROM node:22.23.1-slim AS frontend-build")
    assert 'node-version: "22.23.1"' in workflow
    assert 'VITE_MAP_TILE_URL: ""' in workflow
    assert 'VITE_MAP_TILE_ATTRIBUTION: ""' in workflow
    assert 'test "$(node --version)" = "v22.23.1"' in workflow
    assert 'test "$(npm --version)" = "10.9.8"' in workflow
    assert build_step < diff_step
