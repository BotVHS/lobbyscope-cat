"""Feed RSS per a alertes i novetats."""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from fastapi.responses import Response
from sqlalchemy import text
from db.session import get_db_fastapi

router = APIRouter()

_RSS_HEADER = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>{title}</title>
    <link>https://lobbyscope.cat</link>
    <description>{description}</description>
    <language>ca</language>
    <lastBuildDate>{now}</lastBuildDate>
{items}
  </channel>
</rss>"""

_RSS_ITEM = """    <item>
      <title>{title}</title>
      <link>{link}</link>
      <description>{description}</description>
      <pubDate>{date}</pubDate>
      <guid>{guid}</guid>
    </item>"""


@router.get("/alertes.xml", response_class=Response)
def rss_alertes(db=Depends(get_db_fastapi)):
    rows = db.execute(text("""
        SELECT a.id, a.descripcio, a.creat_at, g.nom AS nom_grup
        FROM alertes a
        LEFT JOIN grups g ON g.id = a.grup_id
        ORDER BY a.creat_at DESC
        LIMIT 50
    """)).fetchall()

    items = []
    for r in rows:
        nom = r.nom_grup or "lobbyscope.cat"
        items.append(_RSS_ITEM.format(
            title=_escape(f"[{nom}] {r.descripcio[:80]}"),
            link=f"https://lobbyscope.cat/alertes",
            description=_escape(r.descripcio),
            date=_fmt_rss_date(r.creat_at),
            guid=f"alerta-{r.id}",
        ))

    xml = _RSS_HEADER.format(
        title="lobbyscope.cat — Alertes",
        description="Novetats del tracker de lobbisme a la Generalitat de Catalunya",
        now=_fmt_rss_date(datetime.now(timezone.utc)),
        items="\n".join(items),
    )
    return Response(content=xml, media_type="application/rss+xml")


@router.get("/grups/{grup_id}.xml", response_class=Response)
def rss_grup(grup_id: int, db=Depends(get_db_fastapi)):
    grup = db.execute(text(
        "SELECT nom FROM grups WHERE id = :id"
    ), {"id": grup_id}).fetchone()

    if not grup:
        return Response(content="<error>Grup no trobat</error>", status_code=404)

    rows = db.execute(text("""
        SELECT
            cx.id, cx.connexio_score, cx.explicacio_ca, cx.creat_at,
            r.data_reunio, r.tema_original,
            nd.titol AS titol_decisio
        FROM connexions cx
        JOIN reunions r ON r.id = cx.reunio_id
        LEFT JOIN normativa_dogc nd ON nd.id = cx.decisio_normativa_id
        WHERE r.grup_id = :grup_id
        ORDER BY cx.creat_at DESC
        LIMIT 30
    """), {"grup_id": grup_id}).fetchall()

    items = []
    for r in rows:
        title = f"Connexió {r.connexio_score:.0f}/100: {(r.titol_decisio or '')[:60]}"
        desc = r.explicacio_ca or f"Reunió sobre: {r.tema_original[:100]}"
        items.append(_RSS_ITEM.format(
            title=_escape(title),
            link=f"https://lobbyscope.cat/connexio/{r.id}",
            description=_escape(desc),
            date=_fmt_rss_date(r.creat_at),
            guid=f"connexio-{r.id}",
        ))

    xml = _RSS_HEADER.format(
        title=f"lobbyscope.cat — {grup.nom}",
        description=f"Connexions detectades per al lobby {grup.nom}",
        now=_fmt_rss_date(datetime.now(timezone.utc)),
        items="\n".join(items),
    )
    return Response(content=xml, media_type="application/rss+xml")


def _escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt_rss_date(dt) -> str:
    if not dt:
        return ""
    if not hasattr(dt, "strftime"):
        return str(dt)
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")
