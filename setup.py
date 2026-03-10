from plugin import *

MANUAL_FILES = [
    {"uri": "README.md", "name": "README.md"},
    {"uri": "CHANGELOG.md", "name": "CHANGELOG.md"},
]


def _ensure_manual_menu(menu_obj: dict) -> None:
    menu_list = menu_obj.setdefault("list", [])
    manual_item = None
    for item in menu_list:
        if item.get("uri") == "manual":
            manual_item = item
            break

    if manual_item is None:
        manual_item = {"uri": "manual", "name": "매뉴얼", "list": MANUAL_FILES}
        menu_list.append(manual_item)

    if not isinstance(manual_item.get("list"), list) or len(manual_item.get("list")) == 0:
        manual_item["list"] = MANUAL_FILES

    menu_obj.setdefault("sub2", {})
    if (
        "manual" not in menu_obj["sub2"]
        or not isinstance(menu_obj["sub2"]["manual"], list)
        or len(menu_obj["sub2"]["manual"]) == 0
    ):
        menu_obj["sub2"]["manual"] = [[x["uri"], x["name"]] for x in manual_item["list"]]


setting = {
    "filepath": __file__,
    "use_db": True,
    "use_default_setting": True,
    "home_module": None,
    "menu": {
        "uri": __package__,
        "name": "EBS 다운로더",
        "list": [
            {
                "uri": "basic",
                "name": "기본",
                "list": [
                    {"uri": "setting", "name": "설정"},
                    {"uri": "download", "name": "다운로드"},
                ],
            },
            {
                "uri": "auto",
                "name": "자동",
                "list": [
                    {"uri": "setting", "name": "설정"},
                    {"uri": "list", "name": "목록"},
                ],
            },
            {
                "uri": "manual",
                "name": "매뉴얼",
                "list": MANUAL_FILES,
            },
            {
                "uri": "log",
                "name": "로그",
            },
        ],
    },
    "setting_menu": None,
    "default_route": "normal",
}

_ensure_manual_menu(setting["menu"])

P = create_plugin_instance(setting)

from .mod_basic import ModuleBasic
from .mod_auto import ModuleAuto

P.set_module_list([ModuleBasic, ModuleAuto])


def _safe_get_first_manual_path() -> str:
    try:
        menu_list = P.menu.get("list", [])
        for item in menu_list:
            if item.get("uri") != "manual":
                continue
            sub_list = item.get("list") or []
            if not sub_list:
                break
            first = sub_list[0]
            if isinstance(first, dict):
                return first.get("uri") or "README.md"
            if isinstance(first, (list, tuple)) and len(first) > 0:
                return first[0] or "README.md"
            if isinstance(first, str):
                return first or "README.md"
            break
    except Exception:
        pass
    return "README.md"


P.get_first_manual_path = _safe_get_first_manual_path
_ensure_manual_menu(P.menu)
P.logger.info("[ebs_downloader] manual menu bootstrap: %s", P.menu.get("sub2", {}).get("manual"))
