from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime, timezone
from typing import Dict, List, Optional
import re
import unicodedata

from app.services.base import TransfermarktBase
from app.utils.utils import extract_from_url, trim


@dataclass
class TransfermarktLeagueInjuries(TransfermarktBase):
    """
    Scrapes a Transfermarkt *league-wide* injuries page (URL is provided by the client).
    Example URL (more data): https://www.transfermarkt.com/championship/verletztespieler/wettbewerb/GB2/plus/1
    """
    URL: str
    # season: Optional[str] = None
    # Future option: max_pages: int = 1

    def _ensure_plus_variant(self, url: str) -> str:
        """
        If the URL doesn't already include '/plus/1', append it to the path.
        Keeps query/fragment intact. If already present, returns url unchanged.
        """
        parts = urlsplit(url)
        path = parts.path or ""
        if "/plus/" in path or path.endswith("/plus/1"):
            return url
        new_path = path.rstrip("/") + "/plus/1"
        return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))


    def __post_init__(self) -> None:
        # Try the richer '/plus/1' version first; fallback to the original URL if needed
        primary = self._ensure_plus_variant(self.URL)
        candidates = [primary] if primary == self.URL else [primary, self.URL]

        last_error = None
        for url in candidates:
            self.URL = url
            self.page = self.request_url_page()
            # guard: table with headers exists
            has_table = self.page.xpath(
                "//table[(contains(@class,'items') or contains(@class,'responsive')) and .//thead//th]"
            )
            if has_table:
                return
            last_error = "Injuries table not found"

        # If neither candidate had a table, raise your standard guard error
        self.raise_exception_if_not_found(xpath="//table")
        # (Optionally: raise ValueError(last_error))


    # ---------- Public orchestrator ----------
    def get_injuries(self) -> dict:
        table = self._get_injuries_table()
        col_map = self._build_column_map(table)
        rows = table.xpath(".//tbody/tr[td]")

        items: List[dict] = []
        for tr in rows:
            parsed = self._parse_row(tr, col_map)
            if parsed:
                items.append(parsed)

        league_name = self._guess_league_name()
        canonical_url = self._canonical_url()

        self.response.update({
            "league": {
                "name": league_name,
                "url": canonical_url or self.URL,
                # "season": self.season,
            },
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "rows": items,
        })
        return self.response

    # ---------- Core parsing ----------
    def _get_injuries_table(self):
        """
        Locate the injuries table in a tolerant way.
        """
        candidates = self.page.xpath(
            "//table[contains(@class,'items') or contains(@class,'responsive')][.//thead//th]"
        )
        if not candidates:
            # Try a fallback if layout differs
            candidates = self.page.xpath("//table[.//thead//th]")
        if not candidates:
            raise ValueError("Injuries table not found on the page.")
        return candidates[0]

    def _build_column_map(self, table) -> Dict[str, int]:
        """
        Build a map {canonical_key: 1-based column index} from the table header.
        Header text is normalized and matched against known variants (EN/DE and likely others).
        """
        th_nodes = table.xpath(".//thead//th")
        headers = [self._norm_text(" ".join(h.xpath(".//text()"))) for h in th_nodes]

        # Known variants -> canonical keys
        variants = {
            "player": {"player", "spieler"},
            "club": {"club", "verein", "team", "mannschaft"},
            "injury": {"injury", "verletzung"},
            "since": {"since", "seit", "from", "injury since", "date of injury", "out since"},
            "until": {"until", "bis", "to", "return date", "back on", "out until"},
            "expectedreturn": {"expected return", "erwartete rückkehr", "voraussichtliche rückkehr", "expected back"},
            "daysabsent": {"days", "tage", "fehltage"},
            "gamesmissed": {"games missed", "spiele verpasst", "spiele"},
            "notes": {"note", "notes", "bemerkung", "anmerkung"},
            "position": {"position", "pos."},
        }

        def canon(h: str) -> Optional[str]:
            for key, alts in variants.items():
                if h in alts:
                    return key
            return None

        col_map: Dict[str, int] = {}
        for idx, raw in enumerate(headers, start=1):
            key = canon(raw)
            if key:
                col_map[key] = idx

        # Player and injury should exist at minimum
        if "player" not in col_map or "injury" not in col_map:
            # We keep going, but parsing may be limited
            pass

        return col_map

    def _parse_row(self, tr, col_map: Dict[str, int]) -> Optional[dict]:
        tds = tr.xpath("./td")
        if not tds:
            return None

        def cell_text(col_key: str) -> Optional[str]:
            idx = col_map.get(col_key)
            if not idx or idx > len(tds):
                return None
            return trim(" ".join(tds[idx - 1].xpath(".//text()")))

        def cell_link(col_key: str) -> Optional[str]:
            idx = col_map.get(col_key)
            if not idx or idx > len(tds):
                return None
            hrefs = tds[idx - 1].xpath(".//a/@href")
            return trim(hrefs[0]) if hrefs else None

        # --- Player (name/url/id)
        # Prefer explicit player column; if missing, try first <a> in row
        player_name = cell_text("player")
        player_url = cell_link("player")
        if not player_name or not player_url:
            link = tr.xpath(".//a[contains(@class,'spielprofil') or contains(@href,'profil/spieler')]/@href")
            if link:
                player_url = trim(link[0])
                player_name = trim(" ".join(tr.xpath(".//a[contains(@class,'spielprofil') or contains(@href,'profil/spieler')]//text()")))

        # --- Club (optional)
        club_name, club_url = None, None
        if "club" in col_map:
            club_name = cell_text("club")
            club_url = cell_link("club")
            if not club_name:
                idx = col_map["club"]
                cell = tds[idx - 1]
                # try <a title> or <img alt>
                t = cell.xpath(".//a/@title | .//img/@alt")
                if t:
                    club_name = trim(t[0])
                if not club_url:
                    href = cell.xpath(".//a/@href")
                    club_url = trim(href[0]) if href else None

        # --- Other fields
        injury = cell_text("injury")
        since = self._normalize_date(cell_text("since"))
        until = self._normalize_date(cell_text("until"))
        # expected = self._normalize_date(cell_text("expectedreturn"))
        # days_absent = self._parse_int(cell_text("daysabsent"))
        # games_missed = self._parse_int(cell_text("gamesmissed"))
        # notes = cell_text("notes")

        if (since is None or until is None):
            injury_td = tr.xpath(".//td[contains(@class,'links')][1]")
            if injury_td:
                # walk following siblings: since -> until -> (often) days/games
                sibs = injury_td[0].itersiblings(tag="td")
                try:
                    since_td = next(sibs)
                    until_td = next(sibs)
                    s_text = trim(" ".join(since_td.xpath(".//text()")))
                    u_text = trim(" ".join(until_td.xpath(".//text()")))
                    if since is None:
                        since = self._normalize_date(s_text)
                    if until is None:
                        until = self._normalize_date(u_text)
                except StopIteration:
                    pass

        # Minimal requirement: player + injury
        if not (player_name and injury):
            return None

        return {
            "player": {
                "id": extract_from_url(player_url),
                "name": player_name,
                "url": player_url,
                "club": {
                    "id": extract_from_url(club_url),
                    "name": club_name,
                } if club_name or club_url else None,
            },
            "injury": injury,
            "since": since,
            "until": until,
            # "expectedReturn": expected,
            # "daysAbsent": days_absent,
            # "gamesMissed": games_missed,
            # "notes": notes,
        }

    # ---------- Utilities ----------
    def _norm_text(self, s: Optional[str]) -> str:
        """
        Lowercase, strip, collapse spaces/punct, remove accents.
        """
        if not s:
            return ""
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.strip().lower()
        s = re.sub(r"\s+", " ", s)
        s = re.sub(r"[^\w ]+", "", s)
        return s

    def _parse_int(self, s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        digits = re.findall(r"\d+", s)
        return int(digits[0]) if digits else None

    def _normalize_date(self, s: Optional[str]) -> Optional[str]:
        """
        Try common TM date formats; return YYYY-MM-DD or None.
        Accepts: 'Oct 11, 2025', '11.10.2025', '01/09/2025', '2025-10-11', '-', '' -> None
        """
        if not s or s in {"-", "?", "unknown", "Unknown"}:
            return None
        s = s.strip()

        # ISO already?
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return s

        # 11.10.2025 -> 2025-10-11
        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", s)
        if m:
            d, mo, y = m.groups()
            return f"{y}-{int(mo):02d}-{int(d):02d}"

        # 01/09/2025 or 1/9/25 -> 2025-09-01
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
        if m:
            d, mo, y = m.groups()
            if len(y) == 2:
                y = ("20" + y) if int(y) <= 69 else ("19" + y)
            return f"{y}-{int(mo):02d}-{int(d):02d}"

        # Oct 11, 2025
        try:
            from datetime import datetime as dt
            return dt.strptime(s, "%b %d, %Y").strftime("%Y-%m-%d")
        except Exception:
            pass

        # e.g. "Sat, Oct 11, 2025"
        s2 = re.sub(r"^[A-Za-z]{3},\s*", "", s)
        try:
            from datetime import datetime as dt
            return dt.strptime(s2, "%b %d, %Y").strftime("%Y-%m-%d")
        except Exception:
            return None

    def _canonical_url(self) -> Optional[str]:
        url = self.page.xpath("//link[@rel='canonical']/@href | //meta[@property='og:url']/@content")
        return trim(url[0]) if url else None

    def _guess_league_name(self) -> Optional[str]:
        # Try breadcrumb or H1
        h1 = self.page.xpath("//h1//text()")
        if h1:
            return trim(" ".join(h1))
        crumb = self.page.xpath("//nav//ol//li[last()]//a//text() | //nav//ol//li[last()]//text()")
        if crumb:
            return trim(" ".join(crumb))
        # Fallback: title
        t = self.page.xpath("//title//text()")
        return trim(" ".join(t)) if t else None
