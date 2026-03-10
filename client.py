import html
import logging
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)


BASE_URL = "https://anikids.ebs.co.kr"
ALL_VODS_URL = f"{BASE_URL}/anikids/allvods"
ANIKIDS_MAIN_URL = f"{BASE_URL}/anikidsmain"
VODLIST_AJAX_URL = f"{BASE_URL}/anikids/getVodListAjax"

COURSE_ID_RE = re.compile(r"/anikids/program/show/(?P<course>[A-Z0-9]+)")
COURSE_TITLE_RE = re.compile(r'courseNm:\s*"(?P<title>[^"]+)"')
PROGRAM_REA_TITLE_RE = re.compile(
    r'<div\b[^>]*class="rea_title"[^>]*>\s*(?P<title>[^<]+)',
    re.I,
)
MPV_PROGRAM_TEXT_RE = re.compile(
    r"mpv-program-text[\"']\)\.html\(\s*[\"'](?P<title>[^\"']+)",
    re.I,
)
HTML_TITLE_RE = re.compile(r"<title>\s*(?P<title>.*?)\s*</title>", re.I | re.S)
OG_TITLE_RE = re.compile(
    r'<meta\b[^>]*property=["\']og:title["\'][^>]*content=["\'](?P<title>[^"\']+)["\']',
    re.I,
)
EPISODE_RE = re.compile(
    r"/vodCommon/show\?siteCd=AK&(?:amp;)*courseId=(?P<course>[^&\"'\s]+)"
    r"&(?:amp;)*lectId=(?P<lect>[^&\"'\s]+)&(?:amp;)*stepId=(?P<step>[^&\"'\s]+)"
)
EPISODE_META_RE = re.compile(
    r"<p>\s*(?P<no>\d+)\.\s*&nbsp;\s*(?P<title>.*?)</p>\s*<span>(?P<date>\d{4}\.\d{2}\.\d{2})</span>",
    re.S,
)
LOGIN_RE = re.compile(r'isLogin:\s*"(?P<v>[YN])"')
BUY_STATE_RE = re.compile(r'buyState:\s*"(?P<v>[^"]*)"')
QUALITY_RE = re.compile(
    r"\{code:\s*'(?P<code>M\d+)'\s*,\s*label:\s*'[^']*'\s*,\s*src:\s*'(?P<src>https://[^']+)'",
    re.S,
)
SUBTITLE_RE = re.compile(
    r"\{\s*code:\s*\"(?P<code>[A-Z0-9_]+)\"\s*,\s*src:\s*'(?P<src>[^']+)'",
    re.S,
)
KC_FORM_RE = re.compile(r'id=["\']kc-form-login["\']', re.I)
KC_FEEDBACK_RE = re.compile(r'kc-feedback-text[^>]*>\s*(?P<msg>[^<]+)\s*<', re.I)
KC_ALREADY_LOGGED_RE = re.compile(r"you are already logged in", re.I)
FORM_OPEN_RE = re.compile(r"<form\b[^>]*>", re.I)
FORM_CLOSE_RE = re.compile(r"</form>", re.I)
FORM_BLOCK_RE = re.compile(r"<form\b[^>]*>.*?</form>", re.I | re.S)
INPUT_RE = re.compile(r"<input\b[^>]*>", re.I)
ATTR_RE = re.compile(r'([a-zA-Z_:][\w:.-]*)\s*=\s*("([^"]*)"|\'([^\']*)\')')
SEASON_LINK_RE = re.compile(
    r"<a\b[^>]*changeSteps\w*\(\s*['\"]?(?P<step>[^'\"\)\s]+)['\"]?\s*\)[^>]*>(?P<name>.*?)</a>",
    re.I | re.S,
)
SELL_MENU_BLOCK_RE = re.compile(
    r'<ul\b[^>]*class=["\'][^"\']*\bsell_menu\b[^"\']*["\'][^>]*>(?P<body>.*?)</ul>',
    re.I | re.S,
)
SELL_MENU_ITEM_RE = re.compile(
    r'<a\b[^>]*\bdata-id\s*=\s*["\'](?P<step>[A-Z0-9]+)["\'][^>]*>(?P<name>.*?)</a>',
    re.I | re.S,
)
EPISODE_THUMB_RE = re.compile(r'<img\b[^>]*\bsrc="(?P<src>[^"]+)"', re.I)
EPISODE_URL_RE = re.compile(r"/vodCommon/show\?siteCd=AK[^\"'\s<]+", re.I)
AJAX_VOD_TITLE_RE = re.compile(
    r'<dt\b[^>]*class="vod_title"[^>]*>(?P<title>.*?)</dt>', re.I | re.S
)
AJAX_VOD_DATE_RE = re.compile(
    r'<li\b[^>]*class="vod_date"[^>]*>.*?<span>(?P<date>\d{4}\.\d{2}\.\d{2})</span>',
    re.I | re.S,
)
AJAX_ITEM_BLOCK_RE = re.compile(
    r'<div\b[^>]*class="item"[^>]*>.*?</div>\s*<!--\s*//item\s*-->',
    re.I | re.S,
)
STEP_ID_QUOTED_RE = re.compile(r"\bstepId\b\s*[:=]\s*['\"](?P<step>[A-Z0-9]+)['\"]", re.I)
STEP_ID_SET_RE = re.compile(r"\bstepId\b\s*=\s*['\"](?P<step>[A-Z0-9]+)['\"]", re.I)
CHANGE_STEPS_CALL_RE = re.compile(
    r"changeSteps\w*\(\s*['\"]?(?P<step>[A-Z0-9]+)['\"]?\s*\)", re.I
)


@dataclass
class ProgramEpisode:
    course_id: str
    lect_id: str
    step_id: str
    episode_no: str
    episode_title: str
    release_date: str
    show_url: str
    thumbnail: str = ""


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_program_title(text: str) -> str:
    if not text:
        return ""

    match = COURSE_TITLE_RE.search(text)
    if match:
        title = _strip_html(match.group("title") or "")
        if title:
            return title

    match = MPV_PROGRAM_TEXT_RE.search(text)
    if match:
        title = _strip_html(match.group("title") or "")
        if title:
            return title

    match = PROGRAM_REA_TITLE_RE.search(text)
    if match:
        title = _strip_html(match.group("title") or "")
        if title:
            return title

    match = OG_TITLE_RE.search(text)
    if match:
        title = _strip_html(match.group("title") or "")
        # Some pages use a generic og:title (e.g. "EBS 애니키즈"); prefer other sources.
        if title and ("EBS" not in title):
            return title

    match = HTML_TITLE_RE.search(text)
    if match:
        title = _strip_html(match.group("title") or "")
        if title:
            title = re.sub(r"\s*\|.*$", "", title).strip()
            if title in ("애니키즈", "EBS 애니키즈"):
                return ""
            return title

    return ""


def _normalize_url(url_or_path: str) -> str:
    return urllib.parse.urljoin(BASE_URL, url_or_path)


def _html_unescape_repeated(value: str, rounds: int = 3) -> str:
    value = value or ""
    for _ in range(max(rounds, 1)):
        new = html.unescape(value)
        if new == value:
            break
        value = new
    return value


def _safe_url_for_message(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url or "")
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except Exception:
        return url or ""


def _origin_for_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url or "")
        if not parsed.scheme or not parsed.netloc:
            return ""
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return ""


def _is_sso_or_login_url(url: str) -> bool:
    lower = (url or "").lower()
    return ("sso.ebs.co.kr" in lower) or ("/login" in lower)


def _looks_logged_out_html(text: str) -> bool:
    if not text:
        return False
    probes = [
        'href="/login" class="special_login"',
        'class="m_login special_login"',
        "로그인 후 이용가능 합니다",
    ]
    return any(p in text for p in probes)


def _parse_kc_feedback(text: str) -> str:
    if not text:
        return ""
    match = KC_FEEDBACK_RE.search(text)
    if not match:
        return ""
    msg = (match.group("msg") or "").strip()
    low = msg.lower()
    if "invalid username or password" in low:
        return "아이디 또는 비밀번호가 올바르지 않습니다."
    if "account is disabled" in low:
        return "계정이 비활성화 상태입니다."
    return msg


def _extract_course_and_step_from_input(value: str) -> tuple[str | None, str | None]:
    value = (value or "").strip()
    if not value:
        return None, None

    # Pure course code
    if re.fullmatch(r"[A-Z0-9]{8,}", value):
        return value, None

    # URL parsing (program/show or vodCommon/show)
    try:
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme and parsed.netloc:
            qs = urllib.parse.parse_qs(parsed.query)
            course_id = (qs.get("courseId") or [None])[0]
            step_id = (qs.get("stepId") or [None])[0]
            if not course_id:
                match = COURSE_ID_RE.search(parsed.path)
                if match:
                    course_id = match.group("course")
            return course_id, step_id
    except Exception:
        pass

    # Fallback substring search
    match = COURSE_ID_RE.search(value)
    if match:
        return match.group("course"), None
    match = re.search(r"courseId=([A-Z0-9]+)", value)
    if match:
        return match.group(1), None
    return None, None


def _pick_step_id_candidates(text: str, course_id: str = "") -> list[str]:
    text = text or ""
    seen: set[str] = set()
    candidates: list[str] = []

    def add(val: str | None) -> None:
        v = (val or "").strip()
        if not v:
            return
        if v in seen:
            return
        seen.add(v)
        candidates.append(v)

    for m in STEP_ID_QUOTED_RE.finditer(text):
        add(m.group("step"))
    for m in STEP_ID_SET_RE.finditer(text):
        add(m.group("step"))
    for m in CHANGE_STEPS_CALL_RE.finditer(text):
        add(m.group("step"))
    for m in re.finditer(r"\bdata-id\s*=\s*['\"](?P<step>[A-Z0-9]+)['\"]", text, re.I):
        add(m.group("step"))

    # Prefer values that are not the courseId itself.
    if course_id:
        filtered = [c for c in candidates if c != course_id]
        if filtered:
            return filtered
    return candidates


def _pick_step_id(text: str, course_id: str = "") -> str | None:
    candidates = _pick_step_id_candidates(text, course_id=course_id)
    return candidates[0] if candidates else None


def _parse_program_seasons(text: str) -> tuple[list[dict[str, str]], str | None]:
    seasons: list[dict[str, str]] = []
    selected: str | None = None
    seen = set()
    for match in SEASON_LINK_RE.finditer(text or ""):
        step_id = (match.group("step") or "").strip()
        name = _strip_html(match.group("name") or "").strip()
        attrs = _parse_attrs(match.group(0))
        classes = (attrs.get("class") or "").lower()
        if not step_id or not name:
            continue
        if step_id in seen:
            continue
        seen.add(step_id)
        seasons.append({"step_id": step_id, "name": name})
        if (" on " in f" {classes} ") or classes.strip() == "on":
            selected = step_id

    # Fallback: some program pages build the season list using `data-id` anchors
    # without `changeSteps*()` calls. Example: `<ul class="sell_menu"> ... <a data-id="60057279">시즌9</a>`.
    if not seasons:
        body = ""
        m = SELL_MENU_BLOCK_RE.search(text or "")
        if m:
            body = m.group("body") or ""
        else:
            body = text or ""
        for match in SELL_MENU_ITEM_RE.finditer(body):
            step_id = (match.group("step") or "").strip()
            name = _strip_html(match.group("name") or "").strip()
            attrs = _parse_attrs(match.group(0))
            classes = (attrs.get("class") or "").lower()
            if not step_id or not name:
                continue
            if step_id in seen:
                continue
            seen.add(step_id)
            seasons.append({"step_id": step_id, "name": name})
            if (" active " in f" {classes} ") or (" on " in f" {classes} ") or classes.strip() in ("active", "on"):
                selected = step_id
    return seasons, selected


def _parse_attrs(tag: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for match in ATTR_RE.finditer(tag):
        key = match.group(1).lower()
        val = match.group(3) if match.group(3) is not None else match.group(4)
        attrs[key] = html.unescape(val or "")
    return attrs


def _parse_form_block(
    form_html: str, base_url: str
) -> tuple[str | None, str | None, list[dict[str, str]], str, str]:
    form_open = FORM_OPEN_RE.search(form_html)
    if not form_open:
        return None, None, [], "", ""

    form_attrs = _parse_attrs(form_open.group(0))
    action_raw = form_attrs.get("action", "").strip()
    method = (form_attrs.get("method", "get") or "get").strip().lower()
    action = urllib.parse.urljoin(base_url, action_raw) if action_raw else ""
    form_id = (form_attrs.get("id") or "").strip().lower()

    inputs: list[dict[str, str]] = []
    for input_match in INPUT_RE.finditer(form_html):
        attrs = _parse_attrs(input_match.group(0))
        name = (attrs.get("name") or "").strip()
        if not name:
            continue
        inputs.append(
            {
                "name": name,
                "value": attrs.get("value", ""),
                "type": (attrs.get("type", "text") or "text").lower(),
            }
        )
    return action, method, inputs, action_raw, form_id


def _score_form_candidate(
    action: str | None, inputs: list[dict[str, str]], form_id: str
) -> int:
    names = {inp.get("name", "") for inp in inputs}
    action_lower = (action or "").lower()
    score = 0
    if form_id == "kc-form-login":
        score += 100
    if ("username" in names) and ("password" in names):
        score += 80
    relay_fields = {"scope", "response_type", "redirect_uri", "state", "client_id"}
    if any(key in names for key in relay_fields):
        score += 50
    if "openid-connect/auth" in action_lower:
        score += 50
    if "login-actions/authenticate" in action_lower:
        score += 50
    if action:
        score += 5
    return score


def _extract_best_form(
    text: str, base_url: str
) -> tuple[str | None, str | None, list[dict[str, str]], str]:
    best: tuple[str | None, str | None, list[dict[str, str]], str, int] | None = None
    for match in FORM_BLOCK_RE.finditer(text):
        action, method, inputs, action_raw, form_id = _parse_form_block(match.group(0), base_url)
        score = _score_form_candidate(action, inputs, form_id)
        if best is None or score > best[4]:
            best = (action, method, inputs, action_raw, score)

    if best is None:
        # Fallback for malformed HTML where closing </form> is missing.
        form_open = FORM_OPEN_RE.search(text)
        if not form_open:
            return None, None, [], ""
        form_html = text[form_open.start() :]
        action, method, inputs, action_raw, _ = _parse_form_block(form_html, base_url)
        return action, method, inputs, action_raw

    return best[0], best[1], best[2], best[3]


def _join_cookie_header(cookiejar: Any) -> str:
    values: dict[str, str] = {}
    for cookie in cookiejar:
        domain = (cookie.domain or "").lstrip(".")
        if not domain.endswith("ebs.co.kr"):
            continue
        values[cookie.name] = cookie.value or ""
    return "; ".join(f"{name}={value}" for name, value in values.items())


class AnikidsClient:
    def __init__(self, cookie: str, user_agent: str, timeout: int = 20) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                "User-Agent": user_agent,
                "Referer": BASE_URL,
            }
        )
        if cookie:
            self.session.headers["Cookie"] = cookie.strip()

    def get_text(self, url: str, referer: str = "") -> str:
        headers = {}
        if referer:
            headers["Referer"] = referer
        response = self.session.get(url, timeout=self.timeout, headers=headers)
        response.raise_for_status()
        return response.text

    def collect_program_ids(self, limit: int = 0) -> list[str]:
        text = self.get_text(ALL_VODS_URL)
        result = []
        seen = set()
        for match in COURSE_ID_RE.finditer(text):
            course_id = match.group("course")
            if course_id in seen:
                continue
            seen.add(course_id)
            result.append(course_id)
            if limit > 0 and len(result) >= limit:
                break
        return result

    def collect_program_episodes_resilient(
        self, course_id: str, step_id: str | None = None
    ) -> tuple[str, list[ProgramEpisode], dict[str, Any]]:
        requested_step_id = (step_id or "").strip() or None
        selected_step_id = requested_step_id
        debug: dict[str, Any] = {
            "program_status": 0,
            "program_final": "",
            "program_len": 0,
            "vod_status": 0,
            "vod_final": "",
            "vod_len": 0,
            "step_id": requested_step_id or "",
            "source": "",
            "seasons": [],
            "errors": [],
        }

        program_page_url = f"{BASE_URL}/anikids/program/show/{course_id}"
        program_text = ""
        base_program_title = ""
        seasons: list[dict[str, str]] = []
        try:
            resp = self.session.get(
                program_page_url,
                timeout=self.timeout,
                headers={"Referer": BASE_URL},
                allow_redirects=True,
            )
            debug["program_status"] = int(getattr(resp, "status_code", 0) or 0)
            debug["program_final"] = _safe_url_for_message(getattr(resp, "url", "") or "")
            program_text = resp.text or ""
            debug["program_len"] = len(program_text)
            resp.raise_for_status()
            base_program_title = _parse_program_title(program_text)
            seasons, selected_step = _parse_program_seasons(program_text)
            debug["seasons"] = seasons
            if not selected_step_id:
                selected_step_id = selected_step or (seasons[0]["step_id"] if seasons else None)
            if not selected_step_id:
                selected_step_id = _pick_step_id(program_text, course_id=course_id)
        except Exception as e:
            debug["errors"].append(f"program:{type(e).__name__}:{e}")

        if not selected_step_id:
            try:
                probe_url = f"{BASE_URL}/vodCommon/show?siteCd=AK&courseId={course_id}"
                probe_resp = self.session.get(
                    probe_url,
                    timeout=self.timeout,
                    headers={"Referer": program_page_url},
                    allow_redirects=True,
                )
                debug["vod_status"] = int(getattr(probe_resp, "status_code", 0) or 0)
                debug["vod_final"] = _safe_url_for_message(getattr(probe_resp, "url", "") or "")
                probe_text = probe_resp.text or ""
                debug["vod_len"] = len(probe_text)
                probe_resp.raise_for_status()
                if not base_program_title:
                    base_program_title = _parse_program_title(probe_text)
                selected_step_id = _pick_step_id(probe_text, course_id=course_id) or selected_step_id
                if not selected_step_id:
                    match = re.search(r"stepId=([A-Z0-9]+)", probe_text or "")
                    if match:
                        selected_step_id = match.group(1)
            except Exception as e:
                debug["errors"].append(f"vod:{type(e).__name__}:{e}")

        base_title = base_program_title or course_id
        display_title = ""
        episodes: list[ProgramEpisode] = []
        source = ""

        try:
            vod_title, vod_episodes = self.collect_program_episodes(
                course_id, step_id=selected_step_id
            )
            if vod_episodes:
                base_title = vod_title or base_title
                episodes = vod_episodes
                source = "vod"
        except Exception as e:
            debug["errors"].append(f"vodlist:{type(e).__name__}:{e}")

        if (not episodes) and selected_step_id:
            try:
                ajax_title, ajax_episodes = self.collect_program_episodes_ajax(
                    course_id, step_id=selected_step_id
                )
                if ajax_episodes:
                    base_title = ajax_title or base_title
                    episodes = ajax_episodes
                    source = "ajax"
            except Exception as e:
                debug["errors"].append(f"ajax:{type(e).__name__}:{e}")

        if (not episodes) and selected_step_id:
            try:
                vod_title, vod_episodes = self.collect_program_episodes(course_id, step_id=None)
                if vod_episodes:
                    base_title = vod_title or base_title
                    episodes = vod_episodes
                    source = "vod-no-step"
            except Exception as e:
                debug["errors"].append(f"vodlist2:{type(e).__name__}:{e}")

        if selected_step_id and seasons:
            for season in seasons:
                if season.get("step_id") == selected_step_id:
                    display_title = season.get("name") or ""
                    break
        if (not display_title) and seasons:
            display_title = seasons[0].get("name") or ""
        if not display_title:
            display_title = base_program_title or base_title

        debug["program_title"] = base_program_title or base_title
        debug["display_title"] = display_title
        debug["step_id"] = selected_step_id or ""
        debug["source"] = source
        debug["episode_count"] = len(episodes)
        return display_title, episodes, debug

    def collect_program_episodes(
        self, course_id: str, step_id: str | None = None
    ) -> tuple[str, list[ProgramEpisode]]:
        params = {"siteCd": "AK", "courseId": course_id}
        if step_id:
            params["stepId"] = step_id
        url = f"{BASE_URL}/vodCommon/show?{urllib.parse.urlencode(params)}"
        referer = f"{BASE_URL}/anikids/program/show/{course_id}"
        headers = {"Referer": referer}
        response = self.session.get(
            url, timeout=self.timeout, headers=headers, allow_redirects=True
        )
        response.raise_for_status()
        text = response.text or ""
        program_title = _parse_program_title(text) or course_id

        episodes: list[ProgramEpisode] = []
        seen = set()
        for match in EPISODE_RE.finditer(text):
            c_id = match.group("course")
            lect_id = match.group("lect")
            step_id_found = match.group("step")
            key = (c_id, lect_id, step_id_found)
            if key in seen:
                continue
            seen.add(key)
            chunk = text[match.start() : match.start() + 600]
            meta = EPISODE_META_RE.search(chunk)
            thumbnail = ""
            if meta:
                ep_no = _strip_html(meta.group("no"))
                ep_title = _strip_html(meta.group("title"))
                release_date = _strip_html(meta.group("date"))
            else:
                ep_no = ""
                ep_title = ""
                release_date = ""
            thumb_match = EPISODE_THUMB_RE.search(chunk)
            if thumb_match:
                thumbnail = _normalize_url(thumb_match.group("src") or "")
            show_url = (
                f"{BASE_URL}/vodCommon/show?siteCd=AK&courseId={c_id}"
                f"&lectId={lect_id}&stepId={step_id_found}"
            )
            episodes.append(
                ProgramEpisode(
                    course_id=c_id,
                    lect_id=lect_id,
                    step_id=step_id_found,
                    episode_no=ep_no,
                    episode_title=ep_title,
                    release_date=release_date,
                    show_url=show_url,
                    thumbnail=thumbnail,
                )
            )

        # Fallback: some pages escape '&' as '&amp;' or change link formatting.
        if not episodes:
            for m in EPISODE_URL_RE.finditer(text):
                raw_url = m.group(0)
                if not raw_url:
                    continue
                norm_url = _html_unescape_repeated(raw_url, rounds=3)
                parsed = urllib.parse.urlparse(norm_url)
                qs = urllib.parse.parse_qs(parsed.query)
                c_id = (qs.get("courseId") or [None])[0]
                lect_id = (qs.get("lectId") or [None])[0]
                step_id_found = (qs.get("stepId") or [None])[0]
                if not (c_id and lect_id and step_id_found):
                    continue
                key = (c_id, lect_id, step_id_found)
                if key in seen:
                    continue
                seen.add(key)

                chunk = text[m.start() : m.start() + 600]
                meta = EPISODE_META_RE.search(chunk)
                if meta:
                    ep_no = _strip_html(meta.group("no"))
                    ep_title = _strip_html(meta.group("title"))
                    release_date = _strip_html(meta.group("date"))
                else:
                    ep_no = ""
                    ep_title = ""
                    release_date = ""
                thumbnail = ""
                thumb_match = EPISODE_THUMB_RE.search(chunk)
                if thumb_match:
                    thumbnail = _normalize_url(thumb_match.group("src") or "")

                show_url = (
                    f"{BASE_URL}/vodCommon/show?siteCd=AK&courseId={c_id}"
                    f"&lectId={lect_id}&stepId={step_id_found}"
                )
                episodes.append(
                    ProgramEpisode(
                        course_id=c_id,
                        lect_id=lect_id,
                        step_id=step_id_found,
                        episode_no=ep_no,
                        episode_title=ep_title,
                        release_date=release_date,
                        show_url=show_url,
                        thumbnail=thumbnail,
                    )
                )

        # If the page doesn't provide episode numbers, assign a stable sequence.
        if episodes and not any((ep.episode_no or "").strip() for ep in episodes):
            def date_key(date_str: str) -> tuple[int, int, int]:
                m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", date_str or "")
                if not m:
                    return (9999, 99, 99)
                return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

            ordered = sorted(episodes, key=lambda ep: (date_key(ep.release_date), ep.lect_id))
            for idx, ep in enumerate(ordered, start=1):
                ep.episode_no = str(idx)
        episodes.sort(
            key=lambda item: (
                item.release_date,
                int(item.episode_no) if item.episode_no.isdigit() else -1,
            ),
            reverse=True,
        )
        return program_title, episodes

    def collect_program_episodes_ajax(
        self, course_id: str, step_id: str, max_pages: int = 50
    ) -> tuple[str, list[ProgramEpisode]]:
        """
        Program pages often load episode lists via AJAX (POST /anikids/getVodListAjax).
        This collector is used as a fallback when vodCommon/show HTML does not contain the list.
        """

        step_id = (step_id or "").strip()
        if not step_id:
            return course_id, []

        program_page_url = f"{BASE_URL}/anikids/program/show/{course_id}"
        program_title = course_id
        try:
            program_text = self.get_text(program_page_url, referer=BASE_URL)
            parsed_title = _parse_program_title(program_text or "")
            if parsed_title:
                program_title = parsed_title
        except Exception:
            pass

        episodes: list[ProgramEpisode] = []
        seen = set()

        def date_key(date_str: str) -> tuple[int, int, int]:
            m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", date_str or "")
            if not m:
                return (9999, 99, 99)
            return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

        headers = {
            "Referer": program_page_url,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html, */*; q=0.01",
        }
        for page in range(1, max_pages + 1):
            data = {
                "pageNumber": str(page),
                "courseId": course_id,
                "stepId": step_id,
                "orderby": "",
            }
            response = self.session.post(
                VODLIST_AJAX_URL, data=data, timeout=self.timeout, headers=headers
            )
            response.raise_for_status()
            text = response.text or ""

            page_found = 0
            for item_match in AJAX_ITEM_BLOCK_RE.finditer(text):
                block = item_match.group(0) or ""
                url_match = EPISODE_URL_RE.search(block)
                if not url_match:
                    continue

                raw_url = url_match.group(0)
                norm_url = _html_unescape_repeated(raw_url, rounds=3)
                parsed = urllib.parse.urlparse(norm_url)
                qs = urllib.parse.parse_qs(parsed.query)
                c_id = (qs.get("courseId") or [None])[0]
                lect_id = (qs.get("lectId") or [None])[0]
                step_id_found = (qs.get("stepId") or [None])[0]
                if not (c_id and lect_id and step_id_found):
                    continue

                key = (c_id, lect_id, step_id_found)
                if key in seen:
                    continue
                seen.add(key)
                page_found += 1

                title_match = AJAX_VOD_TITLE_RE.search(block)
                date_match = AJAX_VOD_DATE_RE.search(block)
                thumb_match = EPISODE_THUMB_RE.search(block)

                ep_title = _strip_html(title_match.group("title")) if title_match else ""
                release_date = _strip_html(date_match.group("date") or "") if date_match else ""
                thumbnail = ""
                if thumb_match:
                    thumbnail = _normalize_url(thumb_match.group("src") or "")

                show_url = _normalize_url(
                    f"/vodCommon/show?siteCd=AK&courseId={c_id}&lectId={lect_id}&stepId={step_id_found}"
                )
                episodes.append(
                    ProgramEpisode(
                        course_id=c_id,
                        lect_id=lect_id,
                        step_id=step_id_found,
                        episode_no="",
                        episode_title=ep_title,
                        release_date=release_date,
                        show_url=show_url,
                        thumbnail=thumbnail,
                    )
                )

            # No items found on this page => stop paging.
            if page_found == 0:
                break

        # Assign episode numbers if the AJAX list did not provide them.
        if episodes and not any((ep.episode_no or "").strip() for ep in episodes):
            ordered = sorted(episodes, key=lambda ep: (date_key(ep.release_date), ep.lect_id))
            for idx, ep in enumerate(ordered, start=1):
                ep.episode_no = str(idx)

        episodes.sort(
            key=lambda item: (
                item.release_date,
                int(item.episode_no) if item.episode_no.isdigit() else -1,
            ),
            reverse=True,
        )
        return program_title, episodes

    def get_episode_play_info(
        self, course_id: str, lect_id: str, step_id: str
    ) -> dict:
        url = (
            f"{BASE_URL}/vodCommon/show?siteCd=AK&courseId={course_id}"
            f"&lectId={lect_id}&stepId={step_id}"
        )
        text = self.get_text(url, referer=f"{BASE_URL}/anikids/program/show/{course_id}")

        login_match = LOGIN_RE.search(text)
        buy_state_match = BUY_STATE_RE.search(text)

        qualities = {}
        for match in QUALITY_RE.finditer(text):
            code = match.group("code")
            src = match.group("src")
            qualities[code] = src

        subtitles = {}
        for match in SUBTITLE_RE.finditer(text):
            code = match.group("code")
            src = _normalize_url(match.group("src"))
            subtitles[code] = src

        return {
            "is_login": (login_match.group("v") == "Y") if login_match else False,
            "buy_state": buy_state_match.group("v") if buy_state_match else "",
            "qualities": qualities,
            "subtitles": subtitles,
            "show_url": url,
        }

    @staticmethod
    def is_preview_url(url: str) -> bool:
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query)
        try:
            end_val = int(query.get("end", ["0"])[0])
        except Exception:
            end_val = 0
        return end_val == 180

    def download_binary(
        self, url: str, output: Path, referer: str = "", chunk_size: int = 1024 * 1024
    ) -> int:
        headers = {}
        if referer:
            headers["Referer"] = referer
        output.parent.mkdir(parents=True, exist_ok=True)
        with self.session.get(url, stream=True, timeout=self.timeout, headers=headers) as response:
            response.raise_for_status()
            total = 0
            with open(output, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    total += len(chunk)
        return total

    @staticmethod
    def login_and_get_cookie(
        user_id: str, password: str, user_agent: str, timeout: int = 20
    ) -> dict[str, Any]:
        user_id = (user_id or "").strip()
        password = password or ""
        masked_id = user_id[:2] + "***" if len(user_id) > 2 else "***"

        if (not user_id) or (not password):
            return {
                "success": False,
                "message": "아이디/비밀번호를 모두 입력하세요.",
                "cookie": "",
            }

        def _log_cookies(session: requests.Session, label: str) -> None:
            """현재 세션 쿠키를 DEBUG로 출력."""
            cookie_list = []
            for c in session.cookies:
                cookie_value = c.value if isinstance(c.value, str) else ""
                cookie_list.append(
                    f"  {c.domain} | {c.name}={cookie_value[:40]}{'...' if len(cookie_value) > 40 else ''}"
                )
            if cookie_list:
                logger.debug("[LOGIN:%s] 현재 세션 쿠키 (%d개):\n%s", label, len(cookie_list), "\n".join(cookie_list))
            else:
                logger.debug("[LOGIN:%s] 현재 세션 쿠키: 없음", label)

        def _log_response(resp: requests.Response, label: str, show_body_chars: int = 500) -> None:
            """응답 상태, URL, 리다이렉트 이력, 본문 일부를 DEBUG로 출력."""
            logger.debug("[LOGIN:%s] HTTP %s → 최종 URL: %s", label, resp.status_code, resp.url)
            if resp.history:
                chain = " → ".join(
                    f"[{r.status_code}] {r.url}" for r in resp.history
                )
                logger.debug("[LOGIN:%s] 리다이렉트 체인: %s → [%s] %s", label, chain, resp.status_code, resp.url)
            body_preview = (resp.text or "")[:show_body_chars].replace("\n", "\\n")
            logger.debug("[LOGIN:%s] 응답 본문 (%d자 중 앞 %d자): %s", label, len(resp.text or ""), show_body_chars, body_preview)

        try:
            logger.debug("=" * 70)
            logger.debug("[LOGIN] 로그인 시작: user=%s, user_agent=%s", masked_id, (user_agent or "")[:60])

            session = requests.Session()
            session.headers.update(
                {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                    "User-Agent": user_agent or "Mozilla/5.0",
                }
            )

            # ── Step 1: GET /login 페이지에서 실제 폼 필드를 동적으로 파싱 ──
            login_page_url = f"{BASE_URL}/login"
            logger.debug("[LOGIN:Step1] GET %s", login_page_url)
            login_resp = session.get(
                login_page_url, timeout=timeout, allow_redirects=True,
                headers={"Referer": BASE_URL},
            )
            login_page_text = login_resp.text or ""
            login_page_final = login_resp.url or login_page_url
            _log_response(login_resp, "Step1")
            _log_cookies(session, "Step1-after-GET")

            # frm 폼의 action과 hidden 필드를 파싱
            form_action = f"{BASE_URL}/sso/login"  # 기본값
            form_fields: dict[str, str] = {}

            frm_match = re.search(
                r'<form\b[^>]*\bid=["\']frm["\'][^>]*>(.*?)</form>',
                login_page_text, re.I | re.S,
            )
            if frm_match:
                frm_html = frm_match.group(0)
                logger.debug("[LOGIN:Step1] <form id='frm'> 발견 (길이: %d)", len(frm_html))
                # action 추출
                action_match = re.search(r'\baction=["\']([^"\']+)["\']', frm_html, re.I)
                if action_match:
                    form_action = urllib.parse.urljoin(login_page_final, action_match.group(1))
                    logger.debug("[LOGIN:Step1] 폼 action: %s (원본: %s)", form_action, action_match.group(1))
                else:
                    logger.debug("[LOGIN:Step1] 폼 action 속성 없음, 기본값 사용: %s", form_action)
                # 모든 hidden input 추출
                for inp_match in INPUT_RE.finditer(frm_html):
                    attrs = _parse_attrs(inp_match.group(0))
                    inp_type = (attrs.get("type") or "text").lower()
                    inp_name = (attrs.get("name") or "").strip()
                    inp_value = attrs.get("value", "")
                    if inp_type == "hidden" and inp_name:
                        form_fields[inp_name] = inp_value
                logger.debug("[LOGIN:Step1] 파싱된 hidden 필드: %s", {k: v[:60] for k, v in form_fields.items()})
            else:
                logger.warning("[LOGIN:Step1] <form id='frm'> 을 찾지 못했습니다! 기본값으로 진행합니다.")
                logger.debug("[LOGIN:Step1] 로그인 페이지 HTML 길이: %d, 앞 300자: %s", len(login_page_text), login_page_text[:300])

            # ── Step 2: 폼 필드에 credential 추가 후 POST ──
            payload = dict(form_fields)  # 파싱된 hidden 필드 그대로 사용
            payload["i"] = user_id
            payload["c"] = "********"  # 로그에는 비밀번호 마스킹
            payload.setdefault("r", "false")
            payload.setdefault("userId", "")
            payload.setdefault("snsSite", "")
            payload.setdefault("j_logintype", "")
            logger.debug("[LOGIN:Step2] POST payload (password masked): %s", payload)

            # 실제 전송 시에는 진짜 비밀번호 사용
            payload["c"] = password

            post_headers = {"Referer": login_page_final, "Origin": _origin_for_url(login_page_final)}
            logger.debug("[LOGIN:Step2] POST %s  headers=%s", form_action, post_headers)
            response = session.post(
                form_action,
                data=payload,
                timeout=timeout,
                allow_redirects=True,
                headers=post_headers,
            )
            final_url = response.url
            _log_response(response, "Step2-POST", show_body_chars=800)
            _log_cookies(session, "Step2-after-POST")

            # ── Step 3: SSO 자동 리다이렉트 폼 추적 (Keycloak 등) ──
            logger.debug("[LOGIN:Step3] SSO 자동 리다이렉트 폼 추적 시작")
            auto_submit_tried = False
            for loop_idx in range(15):
                current_url = response.url or ""
                current_url_lower = current_url.lower()

                logger.debug("[LOGIN:Step3:%d] 현재 URL: %s", loop_idx, current_url)

                # anikids 일반 페이지에 도착하면 중단
                if ("anikids.ebs.co.kr" in current_url_lower) and ("/sso/" not in current_url_lower) and ("/login" not in current_url_lower):
                    logger.debug("[LOGIN:Step3:%d] anikids 일반 페이지 도착 → 루프 종료", loop_idx)
                    break

                response_text = response.text or ""

                # 자동 제출 신호 확인 (JavaScript redirect 또는 meta refresh)
                has_auto_submit = (
                    "document.forms[0].submit" in response_text.lower()
                    or "document.forms['form'].submit" in response_text.lower()
                    or 'onload="document.forms' in response_text.lower()
                )
                logger.debug("[LOGIN:Step3:%d] auto_submit 신호: %s", loop_idx, has_auto_submit)

                action, method, inputs, action_raw = _extract_best_form(response_text, current_url)
                if not inputs and not has_auto_submit:
                    logger.debug("[LOGIN:Step3:%d] 폼 입력/자동제출 없음 → 루프 종료", loop_idx)
                    break

                names = {inp["name"] for inp in inputs} if inputs else set()
                has_login_fields = ("username" in names and "password" in names)
                relay_fields = {"scope", "response_type", "redirect_uri", "state", "client_id"}
                is_relay_form = any(key in names for key in relay_fields)
                action_lower = (action or "").lower()
                is_sso_action = (
                    ("sso.ebs.co.kr" in action_lower)
                    or ("openid-connect" in action_lower)
                    or ("login-actions" in action_lower)
                    or ("/sso/" in action_lower)
                )
                is_sso_page = "sso.ebs.co.kr" in current_url_lower

                logger.debug(
                    "[LOGIN:Step3:%d] 폼 분석: action=%s method=%s names=%s "
                    "has_login=%s is_relay=%s is_sso_action=%s is_sso_page=%s",
                    loop_idx, _safe_url_for_message(action or ""), method, names,
                    has_login_fields, is_relay_form, is_sso_action, is_sso_page,
                )

                # SSO 관련 폼이 아니면 중단
                if not (has_login_fields or is_relay_form or is_sso_action or is_sso_page or has_auto_submit):
                    logger.debug("[LOGIN:Step3:%d] SSO 관련 아님 → 루프 종료", loop_idx)
                    break
                if not action and not has_auto_submit:
                    logger.debug("[LOGIN:Step3:%d] action 없고 auto_submit 아님 → 루프 종료", loop_idx)
                    break

                post_data: dict[str, str] = {}
                if has_login_fields:
                    if has_auto_submit and (not auto_submit_tried):
                        logger.debug("[LOGIN:Step3:%d] Keycloak 자동 제출 폼 감지 (credential 미전송)", loop_idx)
                        for inp in inputs:
                            post_data[inp["name"]] = inp["value"]
                        post_data.setdefault("username", "")
                        post_data.setdefault("password", "")
                        auto_submit_tried = True
                    else:
                        logger.debug("[LOGIN:Step3:%d] Keycloak 로그인 폼 감지 → credential 전송", loop_idx)
                        for inp in inputs:
                            post_data[inp["name"]] = inp["value"]
                        post_data["username"] = user_id
                        post_data["password"] = password
                        if "credentialId" in names:
                            post_data["credentialId"] = ""
                        if "login" in names:
                            post_data["login"] = "Log In"
                else:
                    logger.debug("[LOGIN:Step3:%d] 릴레이/SSO 폼 → 필드 그대로 제출", loop_idx)
                    for inp in inputs:
                        post_data[inp["name"]] = inp["value"]

                # 로그용으로 password 마스킹
                log_data = {k: ("********" if k in ("password", "c") else v[:80]) for k, v in post_data.items()}
                logger.debug("[LOGIN:Step3:%d] 제출 데이터: %s", loop_idx, log_data)

                submit_headers = {"Referer": current_url}
                if method == "post" or has_auto_submit:
                    origin = _origin_for_url(current_url)
                    if origin:
                        submit_headers["Origin"] = origin
                    submit_url = action or current_url
                    logger.debug("[LOGIN:Step3:%d] POST %s", loop_idx, submit_url)
                    response = session.post(
                        submit_url,
                        data=post_data,
                        timeout=timeout,
                        allow_redirects=True,
                        headers=submit_headers,
                    )
                else:
                    logger.debug("[LOGIN:Step3:%d] GET %s", loop_idx, action or "")
                    response = session.get(
                        action or current_url,
                        params=post_data,
                        timeout=timeout,
                        allow_redirects=True,
                        headers=submit_headers,
                    )
                final_url = response.url
                _log_response(response, f"Step3:{loop_idx}", show_body_chars=500)
                _log_cookies(session, f"Step3:{loop_idx}")
            else:
                logger.warning("[LOGIN:Step3] 최대 반복 횟수(15) 도달 → 루프 강제 종료")

            # ── Step 4: 로그인 상태 확인 ──
            logger.debug("[LOGIN:Step4] 로그인 상태 확인 시작, 최종 URL: %s", final_url)
            response_text = response.text or ""
            response_text_lower = response_text.lower()
            already_logged_in_sso = KC_ALREADY_LOGGED_RE.search(response_text) is not None
            logger.debug("[LOGIN:Step4] KC already_logged_in 신호: %s", already_logged_in_sso)

            # Keycloak "already logged in" 페이지 대응
            if already_logged_in_sso:
                hydrate_url = f"{BASE_URL}/login?returnUrl={urllib.parse.quote(BASE_URL, safe=':/?=&')}"
                logger.debug("[LOGIN:Step4] 'already logged in' 감지 → 세션 hydrate 시도: GET %s", hydrate_url)
                try:
                    hydrate_resp = session.get(
                        hydrate_url,
                        timeout=timeout,
                        allow_redirects=True,
                        headers={"Referer": "https://sso.ebs.co.kr"},
                    )
                    _log_response(hydrate_resp, "Step4-hydrate")
                    _log_cookies(session, "Step4-after-hydrate")
                except Exception as e:
                    logger.debug("[LOGIN:Step4] hydrate 실패: %s", e)

            kc_form_present = KC_FORM_RE.search(response_text) is not None
            logger.debug("[LOGIN:Step4] Keycloak 로그인 폼 잔존: %s", kc_form_present)

            if kc_form_present and (not already_logged_in_sso):
                feedback = _parse_kc_feedback(response_text)
                feedback_text = f" ({feedback})" if feedback else ""
                logger.debug("[LOGIN:Step4] SSO 인증 실패, feedback: %s", feedback or "(없음)")
                return {
                    "success": False,
                    "message": (
                        "SSO 로그인 단계에서 인증에 실패했습니다."
                        f"{feedback_text} "
                        f"(최종 URL: {_safe_url_for_message(final_url)})"
                    ),
                    "cookie": "",
                }

            if "please login again through your application" in response_text_lower:
                logger.debug("[LOGIN:Step4] 'please login again' 메시지 감지")
                return {
                    "success": False,
                    "message": (
                        "로그인 세션 처리 중 오류가 발생했습니다. 다시 시도해 주세요. "
                        f"(최종 URL: {_safe_url_for_message(final_url)})"
                    ),
                    "cookie": "",
                }

            # ── Step 5: 로그인 상태 확인 ──
            cookie_header = _join_cookie_header(session.cookies)
            _log_cookies(session, "Step5-cookies")
            logger.debug("[LOGIN:Step5] 최종 쿠키 헤더 길이: %d", len(cookie_header))

            # sso.authenticated 쿠키 존재 여부 확인 (가장 강력한 신호)
            has_sso_auth = any(
                c.name == "sso.authenticated" and c.value == "1"
                for c in session.cookies
                if (c.domain or "").endswith("ebs.co.kr")
            )
            # KEYCLOAK_IDENTITY 쿠키 존재 여부 (SSO 세션 확립 증거)
            has_kc_identity = any(
                c.name == "KEYCLOAK_IDENTITY"
                for c in session.cookies
                if (c.domain or "").endswith("ebs.co.kr")
            )
            logger.debug(
                "[LOGIN:Step5] sso.authenticated=1: %s, KEYCLOAK_IDENTITY: %s",
                has_sso_auth, has_kc_identity,
            )

            # sso.authenticated=1 + KEYCLOAK_IDENTITY 쿠키가 모두 있으면
            # SSO 흐름이 완전히 완료된 것이므로 로그인 성공으로 판정.
            # (anikidsmain, allvods 페이지에는 isLogin 변수가 없고,
            #  네비게이션 바에 항상 "로그인" 링크가 있어서 _looks_logged_out_html이 오탐함)
            if has_sso_auth and has_kc_identity and cookie_header:
                logger.debug("[LOGIN] ✓ 로그인 성공 (sso.authenticated=1 + KEYCLOAK_IDENTITY 확인)")
                return {
                    "success": True,
                    "message": "로그인 성공. 쿠키를 생성했습니다.",
                    "cookie": cookie_header,
                }

            if has_sso_auth and cookie_header:
                logger.debug("[LOGIN] ✓ 로그인 성공 (sso.authenticated=1 확인)")
                return {
                    "success": True,
                    "message": "로그인 성공 (sso.authenticated 확인). 쿠키를 생성했습니다.",
                    "cookie": cookie_header,
                }

            # sso.authenticated 쿠키가 없으면 프로브 페이지로 추가 확인
            logger.debug("[LOGIN:Step5] sso 쿠키 미확인 → 프로브 페이지로 추가 검증")
            login_signal: bool | None = None
            logged_out_signal = False
            # vodCommon 페이지에 isLogin 변수가 있음 (main/allvods에는 없음)
            probe_urls = [
                f"{BASE_URL}/vodCommon/show?siteCd=AK&courseId=40049531",
                ANIKIDS_MAIN_URL,
            ]
            for probe_url in probe_urls:
                try:
                    logger.debug("[LOGIN:Step5] GET %s", probe_url)
                    probe = session.get(
                        probe_url,
                        timeout=timeout,
                        allow_redirects=True,
                        headers={"Referer": BASE_URL},
                    )
                    logger.debug("[LOGIN:Step5] 프로브 응답: HTTP %s, URL: %s, 본문 길이: %d",
                                 probe.status_code, probe.url, len(probe.text or ""))
                    login_match = LOGIN_RE.search(probe.text or "")
                    if login_match:
                        val = login_match.group("v")
                        login_signal = val == "Y"
                        logger.debug("[LOGIN:Step5] isLogin='%s' → login_signal=%s", val, login_signal)
                        if login_signal:
                            break
                    else:
                        logger.debug("[LOGIN:Step5] isLogin 미발견 (URL: %s)", probe_url)
                    if _looks_logged_out_html(probe.text or ""):
                        logged_out_signal = True
                        logger.debug("[LOGIN:Step5] 로그아웃 HTML 감지 (URL: %s)", probe_url)
                except Exception as e:
                    logger.debug("[LOGIN:Step5] 프로브 실패 (%s): %s", probe_url, e)

            logger.debug("[LOGIN:Step5] login_signal=%s, logged_out_signal=%s",
                         login_signal, logged_out_signal)

            if (login_signal is True) and cookie_header:
                logger.debug("[LOGIN] ✓ 로그인 성공 (isLogin=Y)")
                return {
                    "success": True,
                    "message": "로그인 성공. 쿠키를 생성했습니다.",
                    "cookie": cookie_header,
                }

            if cookie_header and (not _is_sso_or_login_url(final_url)) and (not logged_out_signal):
                logger.debug("[LOGIN] ✓ 로그인 추정 성공 (쿠키 존재, 비SSO URL, 로그아웃 아님)")
                return {
                    "success": True,
                    "message": (
                        "쿠키를 생성했습니다. "
                        "로그인 판별 신호가 불안정하여 쿠키 기반으로 계속 진행합니다."
                    ),
                    "cookie": cookie_header,
                }

            cookie_state = "있음" if cookie_header else "없음"
            login_state = (
                "Y" if login_signal is True else ("N" if login_signal is False else "미검출")
            )
            logged_out_state = "예" if logged_out_signal else "아니오"
            already_logged_state = "예" if already_logged_in_sso else "아니오"
            sso_auth_state = "예" if has_sso_auth else "아니오"
            logger.debug(
                "[LOGIN] ✗ 로그인 실패 요약: 최종URL=%s 쿠키=%s isLogin=%s "
                "로그인화면=%s SSO기로그인=%s sso.auth=%s",
                _safe_url_for_message(final_url), cookie_state, login_state,
                logged_out_state, already_logged_state, sso_auth_state,
            )
            return {
                "success": False,
                "message": (
                    "로그인에 실패했습니다. 아이디/비밀번호 또는 구독 상태를 확인하세요. "
                    f"(최종 URL: {_safe_url_for_message(final_url)}, 쿠키: {cookie_state}, "
                    f"isLogin: {login_state}, 로그인화면신호: {logged_out_state}, "
                    f"SSO기로그인신호: {already_logged_state}, sso.authenticated: {sso_auth_state})"
                ),
                "cookie": "",
            }
        except Exception as e:
            logger.exception("[LOGIN] 로그인 처리 중 예외 발생")
            return {
                "success": False,
                "message": f"로그인 처리 중 오류: {e}",
                "cookie": "",
            }

    @staticmethod
    def get_cookie_from_browser(
        browser: str = "auto", user_agent: str = "Mozilla/5.0", timeout: int = 20
    ) -> dict[str, Any]:
        try:
            import browser_cookie3  # type: ignore
        except Exception as e:
            return {
                "success": False,
                "message": f"browser-cookie3 로드 실패: {e}",
                "cookie": "",
            }

        browser = (browser or "auto").strip().lower()
        if browser == "":
            browser = "auto"

        browser_order_all = [
            "chrome",
            "edge",
            "firefox",
            "chromium",
            "brave",
            "opera",
            "vivaldi",
        ]
        browser_order = browser_order_all if browser == "auto" else [browser]

        errors: list[str] = []
        for browser_name in browser_order:
            getter = getattr(browser_cookie3, browser_name, None)
            if not callable(getter):
                errors.append(f"{browser_name}: 미지원 브라우저")
                continue

            try:
                try:
                    cookiejar = getter(domain_name="ebs.co.kr")
                except TypeError:
                    cookiejar = getter()
                cookie_header = _join_cookie_header(cookiejar)
                if not cookie_header:
                    errors.append(f"{browser_name}: ebs.co.kr 쿠키 없음")
                    continue

                # 브라우저에서 읽은 쿠키는 우선 수용하고, 간단 probe 정보만 메시지에 포함.
                probe_url = f"{BASE_URL}/vodCommon/show?siteCd=AK&courseId=40049531"
                final_url = probe_url
                login_state = "미검출"
                try:
                    probe_session = requests.Session()
                    probe_session.headers.update(
                        {
                            "User-Agent": user_agent or "Mozilla/5.0",
                            "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                            "Referer": BASE_URL,
                            "Cookie": cookie_header,
                        }
                    )
                    probe = probe_session.get(probe_url, timeout=timeout, allow_redirects=True)
                    final_url = probe.url
                    login_match = LOGIN_RE.search(probe.text or "")
                    if login_match:
                        login_state = login_match.group("v")
                except Exception:
                    pass

                return {
                    "success": (login_state != "N"),
                    "message": (
                        (
                            f"{browser_name} 브라우저에서 쿠키를 가져왔습니다. "
                            f"(최종 URL: {_safe_url_for_message(final_url)}, isLogin: {login_state})"
                        )
                        if login_state != "N"
                        else (
                            f"{browser_name} 브라우저 쿠키를 읽었지만 로그인 상태가 아닙니다. "
                            f"브라우저에서 로그인이 유지되는지 확인하고 다시 시도하세요. "
                            f"(최종 URL: {_safe_url_for_message(final_url)}, isLogin: {login_state})"
                        )
                    ),
                    "cookie": cookie_header if login_state != "N" else "",
                }
            except Exception as e:
                errors.append(f"{browser_name}: {type(e).__name__} - {e}")

        return {
            "success": False,
            "message": (
                "브라우저에서 쿠키를 가져오지 못했습니다. "
                "FlaskFarm 실행 환경이 브라우저 사용자 프로필에 접근 가능한지 확인하세요. "
                f"(상세: {' | '.join(errors[:4])})"
                + (
                    " (참고: 리눅스 서비스/도커 환경에서는 Chrome/Edge 쿠키 복호화에 필요한 DBus/키링이 없어 실패할 수 있습니다.)"
                    if any("DBUS_SESSION_BUS_ADDRESS" in e for e in errors)
                    else ""
                )
            ),
            "cookie": "",
        }

    @staticmethod
    def get_cookie_from_file(
        path: str, user_agent: str = "Mozilla/5.0", timeout: int = 20
    ) -> dict[str, Any]:
        path = (path or "").strip()
        if not path:
            return {"success": False, "message": "쿠키 파일 경로가 비어 있습니다.", "cookie": ""}

        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                raw = f.read()
        except FileNotFoundError:
            return {
                "success": False,
                "message": f"쿠키 파일을 찾을 수 없습니다. (path: {path})",
                "cookie": "",
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"쿠키 파일 읽기 실패: {type(e).__name__} - {e}",
                "cookie": "",
            }

        cookie_header = ""
        raw_stripped = raw.strip()
        if not raw_stripped:
            return {"success": False, "message": "쿠키 파일이 비어 있습니다.", "cookie": ""}

        # Support either:
        # 1) Cookie header string: "a=b; c=d"
        # 2) Netscape cookies.txt format (Get cookies.txt 등)
        if ("# Netscape HTTP Cookie File" in raw_stripped) or ("\t" in raw_stripped):
            values: dict[str, str] = {}
            now = int(__import__("time").time())
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#HttpOnly_"):
                    line = line[1:]
                elif line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 7:
                    continue
                domain = (parts[0] or "").strip()
                if domain.startswith("HttpOnly_"):
                    domain = domain[len("HttpOnly_") :]
                domain = domain.lstrip(".")
                if not domain.endswith("ebs.co.kr"):
                    continue
                expiry_raw = (parts[4] or "").strip()
                try:
                    expiry = int(expiry_raw) if expiry_raw else 0
                except Exception:
                    expiry = 0
                if expiry and expiry < now:
                    continue
                name = (parts[5] or "").strip()
                value = parts[6] if len(parts) >= 7 else ""
                if not name:
                    continue
                values[name] = value
            cookie_header = "; ".join(f"{k}={v}" for k, v in values.items())
        else:
            # If user copied "Cookie: xxx", accept it.
            if raw_stripped.lower().startswith("cookie:"):
                raw_stripped = raw_stripped.split(":", 1)[1].strip()
            cookie_header = raw_stripped

        if not cookie_header:
            return {
                "success": False,
                "message": "쿠키 파일에서 ebs.co.kr 쿠키를 추출하지 못했습니다.",
                "cookie": "",
            }

        # Quick probe (best-effort)
        probe_url = f"{BASE_URL}/vodCommon/show?siteCd=AK&courseId=40049531"
        final_url = probe_url
        login_state = "미검출"
        try:
            probe_session = requests.Session()
            probe_session.headers.update(
                {
                    "User-Agent": user_agent or "Mozilla/5.0",
                    "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                    "Referer": BASE_URL,
                    "Cookie": cookie_header,
                }
            )
            probe = probe_session.get(probe_url, timeout=timeout, allow_redirects=True)
            final_url = probe.url
            login_match = LOGIN_RE.search(probe.text or "")
            if login_match:
                login_state = login_match.group("v")
        except Exception:
            pass

        return {
            "success": (login_state != "N"),
            "message": (
                f"쿠키 파일에서 쿠키를 가져왔습니다. (최종 URL: {_safe_url_for_message(final_url)}, isLogin: {login_state})"
                if login_state != "N"
                else (
                    "쿠키 파일에서 쿠키를 읽었지만 로그인 상태가 아닙니다. "
                    "cookies.txt가 최신인지(브라우저에서 다시 로그인 후 내보내기) 확인하세요. "
                    f"(최종 URL: {_safe_url_for_message(final_url)}, isLogin: {login_state})"
                )
            ),
            "cookie": cookie_header if login_state != "N" else "",
        }

    @staticmethod
    def analyze_program_url(
        url_or_code: str,
        step_id: str | None,
        cookie: str,
        user_agent: str,
        timeout: int = 20,
    ) -> dict[str, Any]:
        course_id, step_from_input = _extract_course_and_step_from_input(url_or_code)
        if not course_id:
            return {
                "success": False,
                "message": "코스 ID를 찾지 못했습니다. 프로그램 URL 또는 courseId 또는 코스 ID를 입력하세요.",
                "data": {},
            }

        step_id = (step_id or "").strip() or (step_from_input or "").strip() or None
        # Guard against stale/wrong manual stepId (e.g. from a different course).
        if step_id:
            step_course = re.search(r"(BA[A-Z0-9]{8,})", step_id)
            if step_course and (step_course.group(1) != course_id):
                step_id = None

        try:
            # Episode listing (program page + ajax list) is public. Using a cookie can sometimes
            # break requests in container environments (oversized/invalid header), so prefer
            # a cookie-less client for analysis.
            public_client = AnikidsClient(
                cookie="",
                user_agent=(user_agent or "Mozilla/5.0"),
                timeout=timeout,
            )
            display_title, episodes, debug = public_client.collect_program_episodes_resilient(
                course_id, step_id=step_id
            )
            step_id = debug.get("step_id") or step_id
            seasons = debug.get("seasons") or []

            ep_dicts = []
            for ep in episodes:
                ep_dicts.append(
                    {
                        "course_id": ep.course_id,
                        "lect_id": ep.lect_id,
                        "step_id": ep.step_id,
                        "program_title": debug.get("program_title") or display_title,
                        "display_title": debug.get("display_title") or display_title,
                        "episode_no": ep.episode_no,
                        "episode_title": ep.episode_title,
                        "release_date": ep.release_date,
                        "show_url": ep.show_url,
                        "thumbnail": ep.thumbnail,
                    }
                )

            if not ep_dicts:
                program_text = ""
                try:
                    program_text = public_client.get_text(
                        f"{BASE_URL}/anikids/program/show/{course_id}", referer=BASE_URL
                    )
                except Exception:
                    pass
                step_candidates = _pick_step_id_candidates(program_text, course_id=course_id)[:5]
                errors = debug.get("errors") or []
                extra = (
                    f"stepId={step_id or '없음'}, seasons={len(seasons)}, "
                    f"program_status={debug.get('program_status')}, vod_status={debug.get('vod_status')}"
                )
                if step_candidates:
                    extra += f", stepId후보={','.join(step_candidates)}"
                if errors:
                    extra += f", errors={' | '.join(str(x) for x in errors[:3])}"
                return {
                    "success": False,
                    "message": f"에피소드 목록을 찾지 못했습니다. (courseId={course_id}, {extra})",
                    "data": {
                        "input": url_or_code,
                        "course_id": course_id,
                        "step_id": step_id or "",
                        "program_title": debug.get("program_title") or display_title,
                        "display_title": debug.get("display_title") or display_title,
                        "seasons": seasons,
                        "episodes": [],
                        "debug": debug,
                    },
                }

            return {
                "success": True,
                "message": f"분석 완료 (courseId={course_id}, 시즌={step_id or '자동'}, 에피소드={len(ep_dicts)}개)",
                "data": {
                    "input": url_or_code,
                    "course_id": course_id,
                    "step_id": step_id or "",
                    "program_title": debug.get("program_title") or display_title,
                    "display_title": debug.get("display_title") or display_title,
                    "seasons": seasons,
                    "episodes": ep_dicts,
                    "debug": debug,
                },
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"분석 중 오류: {type(e).__name__} - {e}",
                "data": {"course_id": course_id, "step_id": step_id or ""},
            }
