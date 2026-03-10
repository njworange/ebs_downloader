import json
import logging
import os

import flask

from plugin.create_plugin import PluginBase
from plugin.logic_module_base import PluginModuleBase

from .client import AnikidsClient
from .client import logger as client_logger
from .setup import F, P

name = "basic"


class ModuleBasic(PluginModuleBase):
    def __init__(self, P: PluginBase) -> None:
        super(ModuleBasic, self).__init__(P, "setting")
        self.name = name
        self.db_default = {
            f"{self.name}_save_path": "{PATH_DATA}" + os.sep + "download",
            f"{self.name}_quality": "M50",
            f"{self.name}_user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/132.0.0.0 Safari/537.36"
            ),
            f"{self.name}_cookie": "",
            f"{self.name}_account_id": "",
            f"{self.name}_account_pw": "",
            f"{self.name}_cookie_refresh": "True",
            f"{self.name}_recent_url": "",
            f"{self.name}__migrated": "False",
        }
        self.previous_analyze = None

    def plugin_load(self) -> None:
        # Configure client logger
        if not client_logger.handlers:
            for _h in getattr(P.logger, "handlers", []):
                client_logger.addHandler(_h)
            if not client_logger.handlers:
                _sh = logging.StreamHandler()
                _sh.setFormatter(logging.Formatter("[%(name)s] %(levelname)s %(message)s"))
                client_logger.addHandler(_sh)
        client_logger.setLevel(logging.DEBUG)
        # Migrate old auto_* settings to basic_*
        try:
            self._migrate_settings()
        except Exception as e:
            P.logger.warning("[ebs_downloader] 설정 마이그레이션 중 오류 (무시): %s", e)

    def _migrate_settings(self) -> None:
        """기존 auto_* 키에서 basic_* 키로 설정값을 한 번만 마이그레이션합니다."""
        if P.ModelSetting.get_bool(f"{self.name}__migrated"):
            return
        pairs = [
            ("basic_save_path", "auto_save_path"),
            ("basic_quality", "auto_quality"),
            ("basic_user_agent", "auto_user_agent"),
            ("basic_cookie", "auto_cookie"),
            ("basic_account_id", "auto_account_id"),
            ("basic_account_pw", "auto_account_pw"),
            ("basic_cookie_refresh", "auto_auto_cookie_refresh"),
            ("basic_recent_url", "auto_recent_url"),
        ]
        migrated_any = False
        for new_key, old_key in pairs:
            try:
                old_val = (P.ModelSetting.get(old_key) or "").strip()
                if old_val:
                    P.ModelSetting.set(new_key, old_val)
                    P.logger.info("[ebs_downloader] 설정 마이그레이션: %s -> %s", old_key, new_key)
                    migrated_any = True
            except Exception:
                pass
        if migrated_any:
            P.logger.info("[ebs_downloader] 설정 마이그레이션 완료")
        P.ModelSetting.set(f"{self.name}__migrated", "True")

    def process_menu(self, page_name: str, req: flask.Request) -> flask.Response:
        arg = P.ModelSetting.to_dict()
        if page_name == "download":
            arg["url_or_code"] = req.args.get("code") or P.ModelSetting.get(
                f"{self.name}_recent_url"
            )
        return flask.render_template(f"{P.package_name}_{name}_{page_name}.html", arg=arg)

    def process_command(
        self, command: str, arg1: str, arg2: str, arg3: str, req: flask.Request
    ) -> flask.Response:
        ret = {"ret": "success"}
        match command:
            case "login_with_account":
                user_id = (arg1 or "").strip()
                password = arg2 or ""
                masked_id = (user_id[:2] + "***") if len(user_id) >= 2 else "***"
                P.logger.info("login_with_account 요청: id=%s", masked_id)
                result = AnikidsClient.login_and_get_cookie(
                    user_id=user_id,
                    password=password,
                    user_agent=P.ModelSetting.get(f"{self.name}_user_agent"),
                )
                if result.get("success"):
                    P.ModelSetting.set(f"{self.name}_cookie", result.get("cookie", ""))
                    P.ModelSetting.set(f"{self.name}_account_id", user_id)
                    P.ModelSetting.set(f"{self.name}_account_pw", password)
                    P.logger.info("login_with_account 성공: id=%s", masked_id)
                    ret["msg"] = "로그인 성공. 쿠키를 자동으로 저장했습니다."
                else:
                    P.logger.warning(
                        "login_with_account 실패: id=%s, msg=%s",
                        masked_id,
                        result.get("message", "로그인 실패"),
                    )
                    ret["ret"] = "warning"
                    ret["msg"] = result.get("message") or "로그인 실패"

            case "refresh_cookie_saved":
                ok, msg = self.refresh_cookie_with_saved_account(force=True)
                if ok:
                    ret["msg"] = msg
                else:
                    ret["ret"] = "warning"
                    ret["msg"] = msg

            case "analyze_url":
                url_or_code = (arg1 or "").strip()
                step_id = (arg2 or "").strip() or None
                if not url_or_code:
                    ret["ret"] = "warning"
                    ret["msg"] = "URL 또는 코드를 입력하세요."
                else:
                    user_agent = P.ModelSetting.get(f"{self.name}_user_agent") or "Mozilla/5.0"
                    cookie = (P.ModelSetting.get(f"{self.name}_cookie") or "").strip()
                    result = AnikidsClient.analyze_program_url(
                        url_or_code=url_or_code,
                        step_id=step_id,
                        cookie=cookie,
                        user_agent=user_agent,
                    )
                    if result.get("success"):
                        data = result.get("data") or {}
                        self.previous_analyze = data
                        P.ModelSetting.set(f"{self.name}_recent_url", url_or_code)
                        ret["data"] = data
                        ret["msg"] = result.get("message") or "분석 완료"
                    else:
                        ret["ret"] = "warning"
                        ret["msg"] = result.get("message") or "분석 실패"
                        ret["data"] = result.get("data") or {}

            case "download_manual":
                from .mod_auto import ModelEbsEpisode, ModuleAuto

                try:
                    payload = json.loads(arg1 or "[]")
                except Exception:
                    ret["ret"] = "warning"
                    ret["msg"] = "요청 데이터(JSON) 파싱 실패"
                    return flask.jsonify(ret)

                if isinstance(payload, dict):
                    episodes = [payload]
                elif isinstance(payload, list):
                    episodes = payload
                else:
                    episodes = []

                if not episodes:
                    ret["ret"] = "warning"
                    ret["msg"] = "선택된 항목이 없습니다."
                    return flask.jsonify(ret)

                added = 0
                skipped = 0
                queued = 0
                for ep in episodes:
                    if not isinstance(ep, dict):
                        continue
                    course_id = (ep.get("course_id") or "").strip()
                    lect_id = (ep.get("lect_id") or "").strip()
                    step_id_val = (ep.get("step_id") or "").strip()
                    if (not course_id) or (not lect_id) or (not step_id_val):
                        continue

                    item = ModelEbsEpisode.get_by_keys(course_id, lect_id, step_id_val)
                    if item and item.completed:
                        skipped += 1
                        continue
                    if not item:
                        item = ModelEbsEpisode(course_id, lect_id, step_id_val)

                    item.set_info(
                        program_title=(ep.get("program_title") or course_id),
                        display_title=(ep.get("display_title") or ep.get("program_title") or course_id),
                        episode_no=(ep.get("episode_no") or ""),
                        episode_title=(ep.get("episode_title") or ""),
                        release_date=(ep.get("release_date") or ""),
                        show_url=(ep.get("show_url") or ""),
                        thumbnail=(ep.get("thumbnail") or ""),
                    )
                    item.completed = False
                    item.status = "WAITING"
                    item.message = ""
                    item.save()

                    added += 1
                    if ModuleAuto.enqueue_item(item.id):
                        queued += 1

                if added == 0 and skipped > 0:
                    ret["ret"] = "warning"
                    ret["msg"] = f"추가할 항목이 없습니다. (완료로 스킵: {skipped}개)"
                else:
                    ret["msg"] = (
                        f"{added}개를 다운로드 큐에 추가했습니다. "
                        f"(큐 등록: {queued}개, 완료로 스킵: {skipped}개)"
                    )
        return flask.jsonify(ret)

    def refresh_cookie_with_saved_account(self, force: bool = False) -> tuple[bool, str]:
        """저장된 계정으로 쿠키를 갱신합니다."""
        if (not force) and (not P.ModelSetting.get_bool(f"{self.name}_cookie_refresh")):
            return False, "자동 쿠키 갱신이 꺼져 있습니다."
        user_id = (P.ModelSetting.get(f"{self.name}_account_id") or "").strip()
        password = P.ModelSetting.get(f"{self.name}_account_pw") or ""
        if (not user_id) or (not password):
            return False, "자동 갱신용 계정(ID/PW)이 저장되어 있지 않습니다."

        result = AnikidsClient.login_and_get_cookie(
            user_id=user_id,
            password=password,
            user_agent=P.ModelSetting.get(f"{self.name}_user_agent"),
        )
        if result.get("success"):
            P.ModelSetting.set(f"{self.name}_cookie", result.get("cookie", ""))
            return True, "저장된 계정으로 쿠키를 갱신했습니다."
        return False, result.get("message") or "쿠키 갱신 실패"
