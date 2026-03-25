"""
xml_service.py — Processamento de XMLs NF-e (importação DUIMP/F5).

Regras aplicadas:
  1. EAN: aplica da base de dados; se ausente, preserva o existente no XML
  2. cFabricante: igual a cProd (SKU) dentro de <adi>
  3. ICMS: zera todos os valores monetários e limpa infAdic/infCpl
  4. IPI CST → 03 (quando sem alíquota)
  5. PIS/COFINS CST → 50
"""

import io
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Dict, List, Optional, Tuple

from lxml import etree
from openpyxl import Workbook


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _ns(tag: str) -> Optional[str]:
    if tag.startswith("{") and "}" in tag:
        return tag[1:].split("}")[0]
    return None


def _first(xpath_result: List) -> Optional[str]:
    if not xpath_result:
        return None
    v = xpath_result[0]
    return str(v).strip() if v is not None else None


def _dec(text) -> Decimal:
    if text is None:
        return Decimal("0")
    t = str(text).strip().replace(",", ".")
    if not t:
        return Decimal("0")
    try:
        return Decimal(t)
    except InvalidOperation:
        return Decimal("0")


def _q2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _limpar_digitos(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())


def _mk(ns_uri: Optional[str], local: str, text: str) -> etree._Element:
    tag = f"{{{ns_uri}}}{local}" if ns_uri else local
    e = etree.Element(tag)
    e.text = text
    return e


def _get_text(elem: etree._Element, local: str) -> Optional[str]:
    r = elem.xpath(f"./*[local-name()='{local}']/text()")
    return str(r[0]).strip() if r else None


def _novo_stats() -> Dict:
    return {
        # EAN
        "ean_criados": 0,
        "ean_atualizados": 0,
        "ean_preservados": 0,     # sem base, mas XML já tinha valor
        "ean_ausentes": 0,        # sem base e sem valor no XML
        "faltantes_detalhado": [],

        # cFabricante
        "cfabricante_inseridos": 0,
        "cfabricante_atualizados": 0,
        "cfabricante_reposicionados": 0,
        "cfabricante_deduplicados": 0,

        # ICMS
        "icms_zerados": 0,
        "infcpl_limpa": 0,

        # IPI / PIS / COFINS
        "ipi_cst_alterados": 0,
        "ipi_cst_criados": 0,
        "pis_cst_alterados": 0,
        "cofins_cst_alterados": 0,

        "avisos": [],
        "erros": [],
    }


# ---------------------------------------------------------------------------
# 1. EAN — aplica da base; preserva se ausente (nunca destrói)
# ---------------------------------------------------------------------------

def _aplicar_ean(root: etree._Element, stats: Dict, mapa_ean: Dict[str, str], nome_arquivo: str) -> None:
    for det in root.xpath(".//*[local-name()='det']"):
        prod_nodes = det.xpath("./*[local-name()='prod']")
        if not prod_nodes:
            continue
        prod = prod_nodes[0]

        nitem = det.get("nItem", "")
        cprod_nodes = prod.xpath("./*[local-name()='cProd']")
        if not cprod_nodes:
            continue
        sku = (cprod_nodes[0].text or "").strip()
        if not sku:
            continue

        xprod = _first(prod.xpath("./*[local-name()='xProd']/text()")) or ""
        cean_orig = _first(prod.xpath("./*[local-name()='cEAN']/text()")) or ""
        ceantrib_orig = _first(prod.xpath("./*[local-name()='cEANTrib']/text()")) or ""

        gtin = _limpar_digitos(mapa_ean.get(sku, "") or "")

        ns_uri = _ns(prod.tag)
        tag_cean = f"{{{ns_uri}}}cEAN" if ns_uri else "cEAN"
        tag_ceantrib = f"{{{ns_uri}}}cEANTrib" if ns_uri else "cEANTrib"

        cean_nodes = prod.xpath("./*[local-name()='cEAN']")
        ceantrib_nodes = prod.xpath("./*[local-name()='cEANTrib']")
        idx_cprod = prod.index(cprod_nodes[0])

        if not gtin:
            # Sem EAN na base: PRESERVAR o que há no XML, apenas sinalizar
            stats["ean_ausentes"] += 1
            if cean_orig or ceantrib_orig:
                stats["ean_preservados"] += 1

            stats["faltantes_detalhado"].append({
                "Arquivo XML": nome_arquivo,
                "nItem": nitem,
                "SKU": sku,
                "Descrição": xprod,
                "cEAN no XML": cean_orig,
                "cEANTrib no XML": ceantrib_orig,
            })
            continue

        # Atualiza ou cria cEAN
        if cean_nodes:
            if (cean_nodes[0].text or "").strip() != gtin:
                cean_nodes[0].text = gtin
                stats["ean_atualizados"] += 1
        else:
            new_cean = etree.Element(tag_cean)
            new_cean.text = gtin
            prod.insert(idx_cprod + 1, new_cean)
            stats["ean_criados"] += 1

        # Atualiza ou cria cEANTrib
        cean_nodes = prod.xpath("./*[local-name()='cEAN']")
        if ceantrib_nodes:
            if (ceantrib_nodes[0].text or "").strip() != gtin:
                ceantrib_nodes[0].text = gtin
        else:
            new_t = etree.Element(tag_ceantrib)
            new_t.text = gtin
            ref = cean_nodes[0] if cean_nodes else None
            if ref is not None:
                prod.insert(prod.index(ref) + 1, new_t)
            else:
                prod.insert(idx_cprod + 2, new_t)


# ---------------------------------------------------------------------------
# 2. cFabricante = cProd dentro de <adi>
# ---------------------------------------------------------------------------

def _inserir_cfabricante(root: etree._Element, stats: Dict) -> None:
    for adi in root.xpath(".//*[local-name()='adi']"):
        det_anc = adi.xpath("ancestor::*[local-name()='det'][1]")
        if not det_anc:
            continue
        det = det_anc[0]
        cprod = det.xpath("string(./*[local-name()='prod']/*[local-name()='cProd'])").strip()
        if not cprod:
            stats["avisos"].append("adi encontrado, mas det não possui cProd.")
            continue

        ns_uri = _ns(adi.tag)
        tag_cfab = f"{{{ns_uri}}}cFabricante" if ns_uri else "cFabricante"

        nseq = next(iter(adi.xpath("./*[local-name()='nSeqAdic']")), None)

        existentes = [ch for ch in adi if isinstance(ch.tag, str) and etree.QName(ch).localname == "cFabricante"]
        if len(existentes) > 1:
            for extra in existentes[1:]:
                adi.remove(extra)
            existentes = existentes[:1]
            stats["cfabricante_deduplicados"] += 1

        if existentes:
            cf = existentes[0]
            if (cf.text or "").strip() != cprod:
                cf.text = cprod
                stats["cfabricante_atualizados"] += 1
        else:
            cf = etree.Element(tag_cfab)
            cf.text = cprod
            stats["cfabricante_inseridos"] += 1

        # Posicionar após nSeqAdic (ou no início)
        if nseq is not None:
            idx_nseq = adi.index(nseq)
            if cf in adi:
                if adi.index(cf) != idx_nseq + 1:
                    adi.remove(cf)
                    adi.insert(idx_nseq + 1, cf)
                    stats["cfabricante_reposicionados"] += 1
            else:
                adi.insert(idx_nseq + 1, cf)
        else:
            if cf in adi:
                if adi.index(cf) != 0:
                    adi.remove(cf)
                    adi.insert(0, cf)
                    stats["cfabricante_reposicionados"] += 1
            else:
                adi.insert(0, cf)


# ---------------------------------------------------------------------------
# 3. ICMS — zera todos os valores monetários; limpa infCpl
# ---------------------------------------------------------------------------

_CAMPOS_MONETARIOS_ICMS = [
    "vBC", "vICMS", "vICMSOp", "vICMSDif",
    "vBCSTRet", "vICMSSTRet", "vICMSST",
    "vBCST", "vBCFCPST",
    "vFCPST", "vFCP", "vFCPSTRet",
    "pDif",
]

_ICMS_REGEX = re.compile(
    r"[^;.]*\bICMS\b[^;.]*[;.]?",
    flags=re.IGNORECASE,
)


def _zerar_icms(root: etree._Element, stats: Dict) -> None:
    """
    Para cada item det:
      - Garante que o grupo ICMS seja ICMS51 com CST=51
      - Zera todos os campos monetários (vBC, vICMS, etc.)
      - Preserva orig, modBC, pICMS
      - Remove referências a ICMS no infCpl
    """
    for det in root.xpath(".//*[local-name()='det']"):
        icms_wrapper = det.xpath(".//*[local-name()='imposto']//*[local-name()='ICMS']")
        if not icms_wrapper:
            continue
        icms = icms_wrapper[0]

        grupos = [ch for ch in icms if isinstance(ch.tag, str)]
        if not grupos:
            continue
        old = grupos[0]
        old_name = etree.QName(old).localname

        # Simples Nacional: não mexe
        if old_name.startswith("ICMSSN"):
            stats["avisos"].append(f"ICMS Simples Nacional ({old_name}) — não alterado.")
            continue

        ns_uri = _ns(old.tag) or _ns(icms.tag)

        # Lê campos que devem ser preservados
        orig   = _get_text(old, "orig")   or "0"
        modBC  = _get_text(old, "modBC")  or "3"
        pICMS  = _get_text(old, "pICMS")  or "0.00"
        pRedBC = _get_text(old, "pRedBC")

        # Monta novo nó ICMS51 com valores zerados
        tag_icms51 = f"{{{ns_uri}}}ICMS51" if ns_uri else "ICMS51"
        novo = etree.Element(tag_icms51)
        novo.append(_mk(ns_uri, "orig",      orig))
        novo.append(_mk(ns_uri, "CST",       "51"))
        novo.append(_mk(ns_uri, "modBC",     modBC))
        if pRedBC is not None:
            novo.append(_mk(ns_uri, "pRedBC", pRedBC))
        novo.append(_mk(ns_uri, "vBC",       "0.00"))
        novo.append(_mk(ns_uri, "pICMS",     "0.00"))
        novo.append(_mk(ns_uri, "vICMSOp",   "0.00"))
        novo.append(_mk(ns_uri, "pDif",      "0.00"))
        novo.append(_mk(ns_uri, "vICMSDif",  "0.00"))
        novo.append(_mk(ns_uri, "vICMS",     "0.00"))

        idx = icms.index(old)
        icms.remove(old)
        icms.insert(idx, novo)
        stats["icms_zerados"] += 1

    # Limpa infCpl de referências a ICMS
    for infcpl in root.xpath(".//*[local-name()='infCpl']"):
        if infcpl.text:
            original = infcpl.text
            limpo = _ICMS_REGEX.sub("", original)
            limpo = re.sub(r"\s{2,}", " ", limpo).strip()
            if limpo != original:
                infcpl.text = limpo
                stats["infcpl_limpa"] += 1


# ---------------------------------------------------------------------------
# 4. IPI — CST → 03 quando sem alíquota
# ---------------------------------------------------------------------------

def _ajustar_ipi(root: etree._Element, stats: Dict) -> None:
    for ipi in root.xpath(".//*[local-name()='IPI']"):
        p_ipi = _first(ipi.xpath(".//*[local-name()='pIPI']/text()"))
        aliquota = _dec(p_ipi)

        if aliquota == 0:
            cst_nodes = ipi.xpath(".//*[local-name()='CST']")
            if cst_nodes:
                for cst in cst_nodes:
                    if (cst.text or "").strip() != "03":
                        cst.text = "03"
                        stats["ipi_cst_alterados"] += 1
            else:
                alvo = next(iter(
                    ipi.xpath("./*[local-name()='IPITrib']") or
                    ipi.xpath("./*[local-name()='IPINT']")
                ), None)
                if alvo is None:
                    stats["avisos"].append("IPI sem CST e sem IPITrib/IPINT: não foi possível inserir CST=03.")
                    continue
                ns_uri = _ns(alvo.tag)
                tag_cst = f"{{{ns_uri}}}CST" if ns_uri else "CST"
                cst_new = etree.Element(tag_cst)
                cst_new.text = "03"
                alvo.insert(0, cst_new)
                stats["ipi_cst_criados"] += 1


# ---------------------------------------------------------------------------
# 5. PIS / COFINS — CST → 50
# ---------------------------------------------------------------------------

def _ajustar_pis_cofins(root: etree._Element, stats: Dict) -> None:
    for pis in root.xpath(".//*[local-name()='PIS']"):
        for cst in pis.xpath(".//*[local-name()='CST']"):
            if (cst.text or "").strip() != "50":
                cst.text = "50"
                stats["pis_cst_alterados"] += 1

    for cofins in root.xpath(".//*[local-name()='COFINS']"):
        for cst in cofins.xpath(".//*[local-name()='CST']"):
            if (cst.text or "").strip() != "50":
                cst.text = "50"
                stats["cofins_cst_alterados"] += 1


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def processar_xml(
    xml_bytes: bytes,
    mapa_ean: Dict[str, str],
    nome_arquivo: str,
) -> Tuple[Optional[bytes], Dict]:
    """
    Processa um único XML NF-e aplicando todas as regras.
    Retorna (bytes_processado, stats).
    """
    stats = _novo_stats()

    try:
        parser = etree.XMLParser(remove_blank_text=False, recover=True, huge_tree=True)
        tree = etree.parse(io.BytesIO(xml_bytes), parser)
        root = tree.getroot()
    except etree.XMLSyntaxError as e:
        stats["erros"].append(f"XML inválido: {e}")
        return None, stats
    except Exception as e:
        stats["erros"].append(f"Erro ao ler o XML: {e}")
        return None, stats

    # 1. EAN
    try:
        _aplicar_ean(root, stats, mapa_ean, nome_arquivo)
    except Exception as e:
        stats["avisos"].append(f"Falha ao aplicar EAN (ignorado): {e}")

    # 2. cFabricante
    try:
        _inserir_cfabricante(root, stats)
    except Exception as e:
        stats["avisos"].append(f"Falha ao inserir cFabricante (ignorado): {e}")

    # 3. ICMS zerado
    try:
        _zerar_icms(root, stats)
    except Exception as e:
        stats["avisos"].append(f"Falha ao zerar ICMS (ignorado): {e}")

    # 4. IPI
    try:
        _ajustar_ipi(root, stats)
    except Exception as e:
        stats["avisos"].append(f"Falha ao ajustar IPI (ignorado): {e}")

    # 5. PIS/COFINS
    try:
        _ajustar_pis_cofins(root, stats)
    except Exception as e:
        stats["avisos"].append(f"Falha ao ajustar PIS/COFINS (ignorado): {e}")

    # Serializar
    try:
        encoding = tree.docinfo.encoding or "UTF-8"
        out = io.BytesIO()
        tree.write(out, encoding=encoding, xml_declaration=True, pretty_print=False)
        return out.getvalue(), stats
    except Exception as e:
        stats["erros"].append(f"Erro ao serializar XML: {e}")
        return None, stats


# ---------------------------------------------------------------------------
# Relatório Excel — SKUs sem EAN
# ---------------------------------------------------------------------------

def gerar_relatorio_faltantes(resultados: List[Dict]) -> bytes:
    wb = Workbook()
    ws_res = wb.active
    ws_res.title = "Resumo"
    ws_res.append(["SKU", "Qtd. itens sem EAN"])

    contagem: Dict[str, int] = {}
    detalhes: List[Dict] = []
    for r in resultados:
        if r["stats"].get("erros"):
            continue
        for item in r["stats"].get("faltantes_detalhado", []):
            sku = item["SKU"]
            contagem[sku] = contagem.get(sku, 0) + 1
            detalhes.append(item)

    for sku, qtd in sorted(contagem.items(), key=lambda x: (-x[1], x[0])):
        ws_res.append([sku, qtd])

    ws_det = wb.create_sheet("Detalhado")
    ws_det.append(["Arquivo XML", "nItem", "SKU", "Descrição", "cEAN no XML", "cEANTrib no XML"])
    for row in detalhes:
        ws_det.append([
            row.get("Arquivo XML", ""),
            row.get("nItem", ""),
            row.get("SKU", ""),
            row.get("Descrição", ""),
            row.get("cEAN no XML", ""),
            row.get("cEANTrib no XML", ""),
        ])

    for ws in (ws_res, ws_det):
        for col in ws.columns:
            maxlen = max((len(str(c.value or "")) for c in col), default=0)
            ws.column_dimensions[col[0].column_letter].width = min(maxlen + 3, 60)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()
