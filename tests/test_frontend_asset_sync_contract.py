import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / "interface" / "templates" / "index.html"
ASSETS_PATH = ROOT / "interface" / "static" / "assets"


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
