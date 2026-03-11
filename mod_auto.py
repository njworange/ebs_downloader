import datetime
import json
import logging
import os
import pathlib
import queue
import re
import threading

import flask
from flask_sqlalchemy.query import Query
from sqlalchemy import desc, inspect, or_, text

from plugin.create_plugin import PluginBase
from plugin.logic_module_base import PluginModuleBase
from plugin.model_base import ModelBase
from support.expand.ffmpeg import SupportFfmpeg
from tool import ToolUtil

from .client import AnikidsClient
from .client import logger as client_logger
from .setup import F, P

# client.py 의 logger를 FlaskFarm P.logger 핸들러와 연결하여 DEBUG 로그 출력
if not client_logger.handlers:
    for _h in getattr(P.logger, "handlers", []):
        client_logger.addHandler(_h)
    if not client_logger.handlers:
        # 핸들러가 없으면 StreamHandler를 추가하여 콘솔 출력
        _sh = logging.StreamHandler()
        _sh.setFormatter(logging.Formatter("[%(name)s] %(levelname)s %(message)s"))
        client_logger.addHandler(_sh)
client_logger.setLevel(logging.DEBUG)


name = "auto"


def normalize_text(text: str) -> str:
    text = text or ""
    text = text.lower()
    text = re.sub(r"\s+", "", text)
    return text


def parse_keywords(value: str) -> list[str]:
    if not value:
        return []
    tokens = re.split(r"[\n,]+", value)
    result = []
    for token in tokens:
        parsed = normalize_text(token)
        if parsed:
            result.append(parsed)
    return result


def parse_release_date(value: str) -> datetime.date | None:
    value = (value or "").strip()
    if not value:
        return None
    match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", value)
    if not match:
        digits = re.sub(r"\D", "", value)
        if len(digits) >= 8:
            try:
                return datetime.date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
            except ValueError:
                return None
        return None
    try:
        return datetime.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def parse_collect_since(value: str) -> datetime.date | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value[:10])
    except ValueError:
        return None


class ModuleAuto(PluginModuleBase):
    download_queue = None
    download_thread = None
    queued_ids = set()
    queue_lock = threading.Lock()

    def __init__(self, P: PluginBase) -> None:
        super(ModuleAuto, self).__init__(P, "list", scheduler_desc="EBS 애니키즈 자동 다운로드")
        self.name = name
        self.db_default = {
            f"{P.package_name}_{self.name}_last_list_option": "",
            f"{self.name}_interval": "30",
            f"{self.name}_auto_start": "False",
            f"{self.name}_collect_since": "",
            f"{self.name}_download_mode": "blacklist",
            f"{self.name}_blacklist_program": "",
            f"{self.name}_blacklist_episode": "",
            f"{self.name}_whitelist_program": "",
            f"{self.name}_whitelist_episode": "",
            f"{self.name}_scan_program_limit": "120",
            f"{self.name}_scan_episode_limit": "50",
            f"{self.name}_allow_preview": "False",
            f"{self.name}_download_subtitle": "True",
            f"{self.name}_max_retry": "5",
            f"{self.name}_retry_failed": "True",
        }
        self.web_list_model = ModelEbsEpisode

    def process_menu(self, page_name: str, req: flask.Request) -> flask.Response:
        arg = P.ModelSetting.to_dict()
        if page_name == "setting":
            arg["is_include"] = F.scheduler.is_include(self.get_scheduler_id())
            arg["is_running"] = F.scheduler.is_running(self.get_scheduler_id())
        return flask.render_template(f"{P.package_name}_{name}_{page_name}.html", arg=arg)

    def process_command(
        self, command: str, arg1: str, arg2: str, arg3: str, req: flask.Request
    ) -> flask.Response:
        ret = {"ret": "success"}
        match command:
            case "collect_now":
                collected = self.collect_episodes()
                queued = self.enqueue_candidates(include_failed=True)
                ret["msg"] = f"신규 {collected}개 수집, {queued}개를 큐에 추가했습니다."
            case "retry_failed":
                reset_count = self.retry_failed()
                queued = self.enqueue_candidates(include_failed=True)
                ret["msg"] = f"실패 항목 {reset_count}개를 재시도 상태로 변경, {queued}개를 큐에 추가했습니다."
            case "queue_reset":
                reset_count = self.reset_queue()
                ret["msg"] = f"큐 초기화 완료 ({reset_count}개)."
            case "reset_status":
                item = ModelEbsEpisode.get_by_id(arg1)
                if not item:
                    ret["ret"] = "warning"
                    ret["msg"] = "항목을 찾을 수 없습니다."
                else:
                    item.completed = False
                    item.retry = 0
                    item.status = "PENDING"
                    item.message = ""
                    item.save()
                    ret["msg"] = "상태를 초기화했습니다."
            case "delete":
                result = ModelEbsEpisode.delete_by_id(arg1)
                if result:
                    ret["msg"] = "삭제했습니다."
                else:
                    ret["ret"] = "warning"
                    ret["msg"] = "삭제에 실패했습니다."
            case "add_condition":
                mode = arg1
                value = arg2
                old_list = P.ModelSetting.get_list(mode, ",")
                old_str = P.ModelSetting.get(mode)
                if value in old_list:
                    ret["msg"] = "이미 설정되어 있습니다."
                    ret["ret"] = "warning"
                else:
                    old_str += f", {value}" if old_str else value
                    P.ModelSetting.set(mode, old_str)
                    ret["msg"] = "추가했습니다."
            case "queue_status":
                with self.queue_lock:
                    queued_count = len(self.queued_ids)
                    queued_id_list = sorted(self.queued_ids)
                items = []
                for qid in queued_id_list:
                    item = ModelEbsEpisode.get_by_id(qid)
                    if item:
                        items.append({
                            "id": item.id,
                            "program_title": item.program_title or item.course_id,
                            "episode_no": item.episode_no or "",
                            "episode_title": item.episode_title or "",
                            "status": item.status or "",
                        })
                ret["msg"] = f"큐에 {queued_count}개 항목이 있습니다."
                ret["data"] = items
            case "download_item":
                item = ModelEbsEpisode.get_by_id(arg1)
                if not item:
                    ret["ret"] = "warning"
                    ret["msg"] = "항목을 찾을 수 없습니다."
                else:
                    item.completed = False
                    item.retry = 0
                    item.status = "WAITING"
                    item.message = ""
                    item.save()
                    if self.enqueue_item(item.id):
                        ret["msg"] = f"다운로드 큐에 추가했습니다. (ID: {item.id})"
                    else:
                        ret["msg"] = f"이미 큐에 있습니다. (ID: {item.id})"
            case "refresh_episode":
                item = ModelEbsEpisode.get_by_id(arg1)
                if not item:
                    ret["ret"] = "warning"
                    ret["msg"] = "항목을 찾을 수 없습니다."
                else:
                    client = self.make_client()
                    if client is None:
                        ret["ret"] = "warning"
                        ret["msg"] = "쿠키가 없어 갱신할 수 없습니다."
                    else:
                        try:
                            info = client.get_episode_play_info(
                                item.course_id, item.lect_id, item.step_id
                            )
                            item.is_login = "Y" if info["is_login"] else "N"
                            item.buy_state = info["buy_state"] or ""
                            item.updated_time = datetime.datetime.now()
                            item.save()
                            ret["msg"] = (
                                f"에피소드 정보를 갱신했습니다. "
                                f"(로그인: {item.is_login}, 화질: {len(info['qualities'])}개)"
                            )
                        except Exception as e:
                            ret["ret"] = "warning"
                            ret["msg"] = f"갱신 실패: {e}"
        return flask.jsonify(ret)

    def plugin_load(self) -> None:
        schema_ready = self.ensure_schema_columns()

        collect_since = (P.ModelSetting.get(f"{self.name}_collect_since") or "").strip()
        if not collect_since:
            collect_since = datetime.date.today().isoformat()
            P.ModelSetting.set(f"{self.name}_collect_since", collect_since)
            P.logger.info("[ebs_downloader] 자동 수집 기준일 초기화: %s", collect_since)

        if not ModuleAuto.download_queue:
            ModuleAuto.download_queue = queue.Queue()
        if not ModuleAuto.download_thread:
            ModuleAuto.download_thread = threading.Thread(target=self.download_thread_function, args=())
            ModuleAuto.download_thread.daemon = True
            ModuleAuto.download_thread.start()
        if not schema_ready:
            P.logger.warning(
                "[ebs_downloader] 스키마 확인이 완료되지 않아 이번 로드에서는 큐 상태 복구를 건너뜁니다."
            )
            return

        # Recover queue states from previous run
        for item in ModelEbsEpisode.get_queue_states():
            item.status = "PENDING"
            item.save()
        self.enqueue_candidates()

    def get_model_engine(self):
        bind_key = getattr(ModelEbsEpisode, "__bind_key__", None)
        try:
            return F.db.get_engine(bind=bind_key)
        except TypeError:
            try:
                return F.db.get_engine(F.app, bind=bind_key)
            except TypeError:
                return F.db.get_engine(F.app)

    def ensure_schema_columns(self) -> bool:
        table_name = ModelEbsEpisode.__tablename__
        with F.app.app_context():
            _engine = self.get_model_engine()
            inspector = inspect(_engine)

            try:
                if not inspector.has_table(table_name):
                    return True
            except Exception:
                P.logger.exception("[ebs_downloader] 테이블 존재 여부 확인 실패: %s", table_name)
                return False

            try:
                existing_columns = {col.get("name") for col in inspector.get_columns(table_name)}
            except Exception:
                P.logger.exception("[ebs_downloader] 컬럼 목록 조회 실패: %s", table_name)
                return False

            migrations = [
                ("thumbnail", "VARCHAR(512)"),
                ("display_title", "VARCHAR(255)"),
            ]
            for column_name, column_type in migrations:
                if column_name in existing_columns:
                    continue
                try:
                    with _engine.begin() as _conn:
                        _conn.execute(
                            text(
                                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                            )
                        )
                    P.logger.info("[ebs_downloader] DB 마이그레이션: %s 컬럼 추가 완료", column_name)
                    existing_columns.add(column_name)
                except Exception:
                    P.logger.exception(
                        "[ebs_downloader] DB 마이그레이션 실패: %s.%s",
                        table_name,
                        column_name,
                    )
                    return False
            return True

    def scheduler_function(self) -> None:
        P.logger.debug("Scheduler start")
        collected = self.collect_episodes()
        queued = self.enqueue_candidates()
        P.logger.debug("Scheduler end - collected=%s queued=%s", collected, queued)

    @property
    def filter_settings(self) -> dict:
        return {
            "mode": P.ModelSetting.get(f"{self.name}_download_mode"),
            "blacklist_program": parse_keywords(P.ModelSetting.get(f"{self.name}_blacklist_program")),
            "blacklist_episode": parse_keywords(P.ModelSetting.get(f"{self.name}_blacklist_episode")),
            "whitelist_program": parse_keywords(P.ModelSetting.get(f"{self.name}_whitelist_program")),
            "whitelist_episode": parse_keywords(P.ModelSetting.get(f"{self.name}_whitelist_episode")),
        }

    # ── 쿠키 갱신 (basic 모듈 설정 참조) ──

    def refresh_cookie_with_saved_account(self, force: bool = False) -> tuple[bool, str]:
        if (not force) and (not P.ModelSetting.get_bool("basic_cookie_refresh")):
            return False, "자동 쿠키 갱신이 꺼져 있습니다."
        user_id = (P.ModelSetting.get("basic_account_id") or "").strip()
        password = P.ModelSetting.get("basic_account_pw") or ""
        if (not user_id) or (not password):
            return False, "자동 갱신용 계정(ID/PW)이 저장되어 있지 않습니다."

        result = AnikidsClient.login_and_get_cookie(
            user_id=user_id,
            password=password,
            user_agent=P.ModelSetting.get("basic_user_agent"),
        )
        if result.get("success"):
            P.ModelSetting.set("basic_cookie", result.get("cookie", ""))
            return True, "저장된 계정으로 쿠키를 갱신했습니다."
        return False, result.get("message") or "쿠키 갱신 실패"

    def make_client(self, allow_auto_refresh: bool = True) -> AnikidsClient | None:
        cookie = (P.ModelSetting.get("basic_cookie") or "").strip()
        if (not cookie) and allow_auto_refresh:
            refreshed, refresh_msg = self.refresh_cookie_with_saved_account(force=False)
            if (not refreshed) and refresh_msg:
                P.logger.warning("쿠키 자동 갱신 실패: %s", refresh_msg)
            cookie = (P.ModelSetting.get("basic_cookie") or "").strip()
        if not cookie:
            P.logger.warning("쿠키 값이 없습니다. 전체 길이 다운로드에는 로그인 쿠키가 필요합니다.")
            return None
        user_agent = (P.ModelSetting.get("basic_user_agent") or "").strip()
        if not user_agent:
            user_agent = "Mozilla/5.0"
        return AnikidsClient(cookie=cookie, user_agent=user_agent)

    def make_public_client(self) -> AnikidsClient:
        user_agent = (P.ModelSetting.get("basic_user_agent") or "").strip()
        if not user_agent:
            user_agent = "Mozilla/5.0"
        return AnikidsClient(cookie="", user_agent=user_agent)

    # ── 에피소드 수집 (incremental) ──

    def collect_episodes(self) -> int:
        client = self.make_public_client()
        created_count = 0
        scanned_programs = 0
        empty_programs = 0
        no_new_programs = 0
        skipped_old_programs = 0
        program_limit = max(P.ModelSetting.get_int(f"{self.name}_scan_program_limit"), 0)
        episode_limit = max(P.ModelSetting.get_int(f"{self.name}_scan_episode_limit"), 1)
        collect_since = parse_collect_since(P.ModelSetting.get(f"{self.name}_collect_since"))
        try:
            course_ids = client.collect_program_ids(limit=program_limit)
        except Exception:
            P.logger.exception("코스 ID 수집 실패")
            return 0

        if not course_ids:
            P.logger.warning("프로그램 목록 수집 결과가 비어 있습니다. 공개 목록 페이지 응답을 확인하세요.")
            return 0

        P.logger.debug("프로그램 목록 수집 완료: %d개", len(course_ids))

        for course_id in course_ids:
            scanned_programs += 1
            try:
                program_title, episodes, debug = client.collect_program_episodes_resilient(course_id)
                episodes = episodes[:episode_limit]
                canonical_title = (debug.get("program_title") or program_title or course_id).strip()
                display_title = (debug.get("display_title") or program_title or canonical_title).strip()
            except Exception:
                P.logger.exception("에피소드 수집 실패: course_id=%s", course_id)
                continue

            if not episodes:
                empty_programs += 1
                P.logger.warning(
                    "에피소드 목록이 비어 있습니다: course_id=%s step_id=%s source=%s errors=%s",
                    course_id,
                    debug.get("step_id") or "",
                    debug.get("source") or "",
                    " | ".join(str(x) for x in debug.get("errors") or []),
                )
                continue

            if collect_since is not None:
                filtered_episodes = []
                skipped_old_count = 0
                for episode in episodes:
                    episode_date = parse_release_date(episode.release_date)
                    if (episode_date is not None) and (episode_date < collect_since):
                        skipped_old_count += 1
                        break
                    filtered_episodes.append(episode)
                if skipped_old_count > 0:
                    skipped_old_programs += 1
                episodes = filtered_episodes
                if not episodes:
                    P.logger.debug(
                        "프로그램 '%s': 기준일(%s) 이전 에피소드만 있어 수집 건너뜀",
                        canonical_title,
                        collect_since.isoformat(),
                    )
                    continue

            new_for_program = 0
            for episode in episodes:
                existing = ModelEbsEpisode.get_by_keys(
                    episode.course_id, episode.lect_id, episode.step_id
                )
                if existing:
                    # Incremental: 이미 DB에 존재하는 에피소드를 만나면 해당 프로그램 수집 중단
                    # (에피소드는 최신순 정렬이므로 이후는 모두 기존 항목)
                    break
                item = ModelEbsEpisode(episode.course_id, episode.lect_id, episode.step_id)
                item.set_info(
                    program_title=canonical_title,
                    display_title=display_title,
                    episode_no=episode.episode_no,
                    episode_title=episode.episode_title,
                    release_date=episode.release_date,
                    show_url=episode.show_url,
                    thumbnail=episode.thumbnail,
                )
                item.save()
                new_for_program += 1
                created_count += 1

            if new_for_program > 0:
                P.logger.info(
                    "프로그램 '%s': %d개 신규 에피소드 수집", canonical_title, new_for_program
                )
            else:
                P.logger.debug(
                    "프로그램 '%s': 신규 에피소드 없음 (episodes=%d, step_id=%s, source=%s)",
                    canonical_title,
                    len(episodes),
                    debug.get("step_id") or "",
                    debug.get("source") or "",
                )
                no_new_programs += 1
        P.logger.debug(
            "수집 요약 - scanned=%d empty=%d old_only=%d no_new=%d created=%d since=%s",
            scanned_programs,
            empty_programs,
            skipped_old_programs,
            no_new_programs,
            created_count,
            collect_since.isoformat() if collect_since else "",
        )
        return created_count

    # ── 필터링 & 큐 관리 ──

    def _is_allowed(self, item: "ModelEbsEpisode", settings: dict) -> tuple[bool, str]:
        if item.completed:
            return False, "이미 완료됨"
        program = normalize_text(item.program_title)
        episode = normalize_text(item.episode_title)
        mode = settings["mode"]

        if mode == "whitelist":
            in_whitelist = False
            for keyword in settings["whitelist_program"]:
                if keyword in program:
                    in_whitelist = True
                    break
            if not in_whitelist:
                for keyword in settings["whitelist_episode"]:
                    if keyword in episode:
                        in_whitelist = True
                        break
            if not in_whitelist:
                return False, "화이트리스트 미일치"
            return True, ""

        # blacklist mode
        for keyword in settings["blacklist_program"]:
            if keyword in program:
                return False, "블랙리스트 프로그램 키워드 일치"
        for keyword in settings["blacklist_episode"]:
            if keyword in episode:
                return False, "블랙리스트 에피소드 키워드 일치"
        return True, ""

    def enqueue_candidates(self, include_failed: bool | None = None) -> int:
        settings = self.filter_settings
        if include_failed is None:
            include_failed = P.ModelSetting.get_bool(f"{self.name}_retry_failed")
        max_retry = max(P.ModelSetting.get_int(f"{self.name}_max_retry"), 1)
        add_count = 0
        for item in ModelEbsEpisode.get_candidates(max_retry=max_retry):
            # Already queued items should not be re-filtered each scheduler tick.
            if item.status == "WAITING":
                if self.enqueue_item(item.id):
                    add_count += 1
                continue
            # FILTERED 에피소드는 건너뛰기 (Wavve/Tving 방식)
            # 화이트/블랙리스트 변경 시 기존 에피소드를 자동으로 재평가하지 않음
            # 수동으로 "초기화" 후 다음 스케줄러 실행 시 재평가됨
            if item.status == "FILTERED":
                continue
            if (item.status in ("FAILED", "PREVIEW_BLOCKED", "GIVEUP")) and (not include_failed):
                continue
            allowed, reason = self._is_allowed(item, settings)
            if not allowed:
                if not item.completed:
                    item.status = "FILTERED"
                    item.message = reason
                    item.save()
                continue

            if item.status not in ("WAITING", "DOWNLOADING"):
                item.status = "WAITING"
                item.message = ""
                item.save()
            if self.enqueue_item(item.id):
                add_count += 1
        return add_count

    @classmethod
    def enqueue_item(cls, item_id: int) -> bool:
        if cls.download_queue is None:
            return False
        with cls.queue_lock:
            if item_id in cls.queued_ids:
                return False
            cls.queued_ids.add(item_id)
            cls.download_queue.put(item_id)
            return True

    def download_thread_function(self) -> None:
        while True:
            item_id = self.download_queue.get()
            try:
                self.download_one(item_id)
            except Exception:
                P.logger.exception("다운로드 스레드 오류: id=%s", item_id)
            finally:
                with self.queue_lock:
                    self.queued_ids.discard(item_id)
                self.download_queue.task_done()

    def pick_quality(self, qualities: dict[str, str], preferred: str) -> tuple[str, str] | tuple[None, None]:
        quality_orders = {
            "M50": ["M50", "M20", "M10", "M05"],
            "M20": ["M20", "M10", "M05"],
            "M10": ["M10", "M05"],
            "M05": ["M05"],
        }
        for code in quality_orders.get(preferred, ["M50", "M20", "M10", "M05"]):
            if code in qualities:
                return code, qualities[code]
        if qualities:
            code = sorted(qualities.keys(), reverse=True)[0]
            return code, qualities[code]
        return None, None

    def make_filename(self, item: "ModelEbsEpisode", quality_code: str) -> str:
        def quality_label(code: str) -> str:
            mapping = {"M50": "1080p", "M20": "720p", "M10": "480p", "M05": "360p"}
            return mapping.get((code or "").upper(), (code or "").upper() or "NA")

        def yymmdd(release_date: str) -> str:
            digits = re.sub(r"\D", "", release_date or "")
            if len(digits) >= 8:
                return digits[2:8]
            m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", release_date or "")
            if not m:
                return "000000"
            return f"{m.group(1)[2:]}{m.group(2)}{m.group(3)}"

        def e_part(ep_no: str) -> str:
            ep_no = (ep_no or "").strip()
            if not ep_no:
                return "E00"
            if ep_no.isdigit():
                return f"E{int(ep_no):02d}"
            m = re.search(r"\d+", ep_no)
            if m:
                return f"E{int(m.group(0)):02d}"
            return f"E{ep_no}"

        title = (item.display_title or item.program_title or item.course_id or "EBS").strip()
        ep = e_part(item.episode_no)
        date = yymmdd(item.release_date)
        q = quality_label(quality_code)
        base = f"{title}.{ep}.{date}.{q}-EBS.mp4"

        # Windows-safe filename for cross-platform use.
        base = re.sub(r"[\\/:*?\"<>|]", " ", base)
        base = re.sub(r"\s+", " ", base).strip()
        if len(base) > 240:
            base = base[:240].rstrip()
        return base

    def download_one(self, item_id: int) -> None:
        item = ModelEbsEpisode.get_by_id(item_id)
        if not item:
            return
        if item.completed:
            return

        max_retry = max(P.ModelSetting.get_int(f"{self.name}_max_retry"), 1)
        if item.retry >= max_retry:
            item.status = "GIVEUP"
            item.message = "재시도 횟수 초과"
            item.save()
            return

        client = self.make_client()
        if client is None:
            item.status = "FAILED"
            item.retry += 1
            item.message = "로그인 쿠키가 비어 있습니다."
            item.save()
            return

        item.status = "DOWNLOADING"
        item.message = ""
        item.save()

        try:
            info = client.get_episode_play_info(item.course_id, item.lect_id, item.step_id)
            refresh_msg = ""
            if (not info.get("is_login")) or (not info.get("qualities")):
                refreshed, refresh_msg = self.refresh_cookie_with_saved_account(force=False)
                if refreshed:
                    P.logger.info(refresh_msg)
                    client = self.make_client(allow_auto_refresh=False)
                    if client:
                        info = client.get_episode_play_info(item.course_id, item.lect_id, item.step_id)
                else:
                    P.logger.warning("쿠키 자동 갱신 실패: %s", refresh_msg)

            item.is_login = "Y" if info["is_login"] else "N"
            item.buy_state = info["buy_state"] or ""
            if not info["is_login"]:
                msg = "로그인 상태가 아닙니다. 쿠키를 확인하세요."
                if refresh_msg:
                    msg += f" ({refresh_msg})"
                raise Exception(msg)

            preferred_quality = P.ModelSetting.get("basic_quality")
            quality_code, play_url = self.pick_quality(info["qualities"], preferred_quality)
            if not quality_code or not play_url:
                msg = "재생 가능한 화질 URL을 찾지 못했습니다. 쿠키 만료 또는 구독 상태를 확인하세요."
                if refresh_msg:
                    msg += f" ({refresh_msg})"
                raise Exception(msg)

            item.quality_code = quality_code
            item.play_url = play_url
            item.is_preview = AnikidsClient.is_preview_url(play_url)
            allow_preview = P.ModelSetting.get_bool(f"{self.name}_allow_preview")
            if item.is_preview and not allow_preview:
                item.status = "PREVIEW_BLOCKED"
                item.retry += 1
                item.message = "프리뷰 URL(end=180) 감지. 로그인/구독 쿠키를 확인하세요."
                item.save()
                return

            save_path = ToolUtil.make_path(P.ModelSetting.get("basic_save_path"))
            pathlib.Path(save_path).mkdir(parents=True, exist_ok=True)
            filename = self.make_filename(item, quality_code)
            output_path = pathlib.Path(save_path) / filename

            if output_path.exists() and output_path.stat().st_size > 0:
                item.filesize = output_path.stat().st_size
                item.filepath = output_path.as_posix()
                item.completed = True
                item.completed_time = datetime.datetime.now()
                item.status = "COMPLETED"
                item.message = "이미 파일이 존재합니다."
                item.save()
                return

            # Use FlaskFarm's ffmpeg pipeline so the job is visible in the ffmpeg plugin UI (wavve plugin style).
            cookie = (P.ModelSetting.get("basic_cookie") or "").strip()
            if cookie.lower().startswith("cookie:"):
                cookie = cookie.split(":", 1)[1].strip()
            cookie = cookie.replace("\r", " ").replace("\n", " ").strip()
            headers = {
                "User-Agent": P.ModelSetting.get("basic_user_agent") or "Mozilla/5.0",
                "Referer": info.get("show_url") or item.show_url or "https://anikids.ebs.co.kr",
            }
            if cookie:
                headers["Cookie"] = cookie

            callback_id = f"{P.package_name}_{self.name}_{item.id}"
            downloader = SupportFfmpeg(
                play_url,
                filename,
                save_path=str(pathlib.Path(save_path)),
                headers=headers,
                callback_id=callback_id,
                timeout_minute=180,
            )
            downloader.start()
            # Wait until the ffmpeg thread finishes so DB state stays consistent and we don't enqueue duplicates.
            if downloader.thread is not None:
                downloader.thread.join()

            data = downloader.get_data()
            if downloader.status != SupportFfmpeg.Status.COMPLETED:
                raise Exception(
                    f"ffmpeg 다운로드 실패: {data.get('status_kor') or data.get('status_str') or downloader.status}"
                )
            if not output_path.exists() or output_path.stat().st_size <= 0:
                raise Exception("ffmpeg 다운로드가 완료되었지만 파일이 생성되지 않았습니다.")

            item.filesize = output_path.stat().st_size
            item.filepath = output_path.as_posix()
            item.completed = True
            item.completed_time = datetime.datetime.now()
            item.status = "COMPLETED"
            item.message = "다운로드 완료"

            if P.ModelSetting.get_bool(f"{self.name}_download_subtitle"):
                for code, sub_url in info["subtitles"].items():
                    subtitle_path = output_path.with_suffix(f".{code.lower()}.smi")
                    try:
                        client.download_binary(sub_url, subtitle_path, referer=info["show_url"])
                    except Exception:
                        P.logger.exception("자막 다운로드 실패: %s", sub_url)

            item.save()
        except Exception as e:
            item.retry += 1
            item.status = "FAILED"
            item.message = str(e)
            item.save()

    def retry_failed(self) -> int:
        max_retry = max(P.ModelSetting.get_int(f"{self.name}_max_retry"), 1)
        count = 0
        for item in ModelEbsEpisode.get_failed(max_retry=max_retry):
            item.status = "PENDING"
            item.message = ""
            item.save()
            count += 1
        return count

    def reset_queue(self) -> int:
        with self.queue_lock:
            self.queued_ids.clear()
            if self.download_queue:
                self.download_queue.queue.clear()
        count = 0
        for item in ModelEbsEpisode.get_queue_states():
            item.status = "PENDING"
            item.message = "큐를 초기화했습니다."
            item.save()
            count += 1
        return count


class ModelEbsEpisode(ModelBase):
    P = P
    __tablename__ = f"{P.package_name}_auto"
    __table_args__ = {"mysql_collate": "utf8_general_ci"}
    __bind_key__ = P.package_name

    id = F.db.Column(F.db.Integer, primary_key=True)
    created_time = F.db.Column(F.db.DateTime)
    updated_time = F.db.Column(F.db.DateTime)
    completed_time = F.db.Column(F.db.DateTime)

    course_id = F.db.Column(F.db.String(64))
    lect_id = F.db.Column(F.db.String(64))
    step_id = F.db.Column(F.db.String(64))

    program_title = F.db.Column(F.db.String(255))
    episode_no = F.db.Column(F.db.String(64))
    episode_title = F.db.Column(F.db.String(255))
    release_date = F.db.Column(F.db.String(32))
    show_url = F.db.Column(F.db.String(512))
    thumbnail = F.db.Column(F.db.String(512))
    display_title = F.db.Column(F.db.String(255))

    quality_code = F.db.Column(F.db.String(32))
    play_url = F.db.Column(F.db.Text)
    is_preview = F.db.Column(F.db.Boolean)
    is_login = F.db.Column(F.db.String(1))
    buy_state = F.db.Column(F.db.String(16))

    status = F.db.Column(F.db.String(32))
    message = F.db.Column(F.db.String(1024))
    retry = F.db.Column(F.db.Integer)
    completed = F.db.Column(F.db.Boolean)
    filesize = F.db.Column(F.db.Integer)
    filepath = F.db.Column(F.db.String(512))

    def __init__(self, course_id: str, lect_id: str, step_id: str) -> None:
        now = datetime.datetime.now()
        self.created_time = now
        self.updated_time = now
        self.course_id = course_id
        self.lect_id = lect_id
        self.step_id = step_id
        self.retry = 0
        self.completed = False
        self.status = "PENDING"
        self.message = ""
        self.is_preview = False
        self.is_login = "N"
        self.buy_state = ""
        self.thumbnail = ""
        self.display_title = ""

    def set_info(
        self,
        program_title: str,
        episode_no: str,
        episode_title: str,
        release_date: str,
        show_url: str,
        thumbnail: str = "",
        display_title: str = "",
    ) -> None:
        self.program_title = program_title
        self.display_title = display_title or program_title
        self.episode_no = episode_no
        self.episode_title = episode_title
        self.release_date = release_date
        self.show_url = show_url
        if thumbnail:
            self.thumbnail = thumbnail
        self.updated_time = datetime.datetime.now()

    @classmethod
    def get_by_keys(cls, course_id: str, lect_id: str, step_id: str) -> "ModelEbsEpisode":
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter_by(course_id=course_id, lect_id=lect_id, step_id=step_id)
                .order_by(desc(cls.id))
                .first()
            )

    @classmethod
    def get_candidates(cls, max_retry: int) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter(cls.completed == False)
                .filter(cls.retry < max_retry)
                .filter(cls.status != "DOWNLOADING")
                .order_by(desc(cls.id))
                .all()
            )

    @classmethod
    def get_failed(cls, max_retry: int) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter(cls.completed == False)
                .filter(cls.retry < max_retry)
                .filter(cls.status.in_(["FAILED", "PREVIEW_BLOCKED", "GIVEUP"]))
                .order_by(desc(cls.id))
                .all()
            )

    @classmethod
    def get_queue_states(cls) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter(cls.completed == False)
                .filter(cls.status.in_(["WAITING", "DOWNLOADING"]))
                .all()
            )

    @classmethod
    def make_query(
        cls, req: flask.Request, order: str = "desc", search: str = "", option1: str = "all", option2: str = "all"
    ) -> Query:
        with F.app.app_context():
            query = F.db.session.query(cls)
            # fallback: web_list가 keyword를 search로 매핑하지 못한 경우 직접 읽기
            if not search and req:
                search = (req.form.get("keyword", "") or "").strip()
            if search:
                query = query.filter(
                    or_(
                        cls.program_title.like(f"%{search}%"),
                        cls.episode_title.like(f"%{search}%"),
                        cls.course_id.like(f"%{search}%"),
                    )
                )

            match option1:
                case "completed":
                    query = query.filter_by(completed=True)
                case "waiting":
                    query = query.filter_by(status="WAITING")
                case "downloading":
                    query = query.filter_by(status="DOWNLOADING")
                case "failed":
                    query = query.filter(cls.status.in_(["FAILED", "GIVEUP"]))
                case "filtered":
                    query = query.filter_by(status="FILTERED")
                case "preview":
                    query = query.filter_by(status="PREVIEW_BLOCKED")
                case _:
                    pass

            if order == "desc":
                query = query.order_by(desc(cls.id))
            else:
                query = query.order_by(cls.id)
            return query
