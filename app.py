"""
app.py — NF-e Editor: Importação DUIMP / XML F5 → Bling
Streamlit UI principal.

Execute:
    streamlit run app.py
"""

import io
import zipfile
import pandas as pd
import streamlit as st

from database import (
    init_db, get_all_eans, upsert_eans, salvar_ean_manual,
    get_db_stats, listar_todos,
)
from excel_loader import carregar_planilha_ean, carregar_planilha_xmlf5
from xml_service import processar_xml, gerar_relatorio_faltantes
from decimal import Decimal


# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="NF-e Editor · Importação",
    layout="wide",
    initial_sidebar_state="expanded",
)

init_db()

# ---------------------------------------------------------------------------
# CSS customizado
# ---------------------------------------------------------------------------

st.markdown("""
<style>
/* Fonte e fundo geral */
html, body, [class*="css"] { font-family: 'Inter', 'Segoe UI', sans-serif; }

/* Remove padding topo */
.block-container { padding-top: 1.5rem !important; padding-bottom: 2rem !important; }

/* Header principal */
.app-header {
    background: linear-gradient(135deg, #0f2244 0%, #1a3a6e 100%);
    border-radius: 12px;
    padding: 1.4rem 2rem;
    margin-bottom: 1.5rem;
    color: white;
}
.app-header h1 { margin: 0; font-size: 1.5rem; font-weight: 700; letter-spacing: -0.3px; }
.app-header p  { margin: 0.25rem 0 0; font-size: 0.85rem; opacity: 0.75; }

/* Cards de stat */
.stat-card {
    background: #f8f9fb;
    border: 1px solid #e4e8ef;
    border-radius: 10px;
    padding: 0.9rem 1.1rem;
    text-align: center;
}
.stat-card .num  { font-size: 2rem; font-weight: 700; color: #1a3a6e; line-height: 1; }
.stat-card .lbl  { font-size: 0.72rem; color: #6b7280; margin-top: 0.2rem; text-transform: uppercase; letter-spacing: 0.5px; }

/* Seção de etapa */
.step-badge {
    display: inline-block;
    background: #e8eef8;
    color: #1a3a6e;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    padding: 0.25rem 0.7rem;
    border-radius: 20px;
    margin-bottom: 0.5rem;
}

/* Aviso EAN */
.ean-warning {
    background: #fff8e1;
    border-left: 4px solid #f59e0b;
    border-radius: 6px;
    padding: 0.8rem 1.1rem;
    margin-bottom: 1rem;
}

/* Botão primário extra */
div.stButton > button[kind="primary"] {
    background: #1a3a6e !important;
    color: white !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}

/* Sidebar */
[data-testid="stSidebar"] { background: #f5f7fb; }
[data-testid="stSidebar"] .block-container { padding-top: 1rem !important; }

/* Expander header */
.streamlit-expanderHeader { font-weight: 600; }

/* Divider suave */
hr { border: none; border-top: 1px solid #e4e8ef; margin: 1.2rem 0; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

def _init_state():
    defaults = {
        "stage": "input",          # input | fill_ean | results
        "xmls_carregados": [],     # [(nome, bytes), ...]
        "faltantes": [],           # lista de dicts
        "resultados": [],          # lista processada
        "mapa_fiscal": {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# ---------------------------------------------------------------------------
# Helpers UI
# ---------------------------------------------------------------------------

def _stat_card(num, label):
    return f"""<div class="stat-card"><div class="num">{num}</div><div class="lbl">{label}</div></div>"""


def _step_badge(text):
    return f'<span class="step-badge">{text}</span>'


def _limpar_digitos(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())


# ---------------------------------------------------------------------------
# SIDEBAR — Base de EANs
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("## 🗄️ Base de EANs")

    db_stats = get_db_stats()
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        st.metric("SKUs cadastrados", db_stats["total"])
    with col_s2:
        ult = (db_stats["ultima_atualizacao"] or "—")[:10]
        st.metric("Última atualização", ult)

    st.divider()

    # Upload para atualizar base de EAN
    st.markdown("**Atualizar base por planilha**")
    st.caption("Colunas esperadas: SKU · EAN (GTIN)")
    plan_ean_file = st.file_uploader(
        "Planilha EAN (.xlsx, .xls, .csv)",
        type=["xlsx", "xls", "csv"],
        key="plan_ean_upload",
        label_visibility="collapsed",
    )

    if plan_ean_file:
        if st.button("⬆️ Importar para a base", use_container_width=True):
            try:
                registros = carregar_planilha_ean(plan_ean_file.getvalue(), plan_ean_file.name)
                if not registros:
                    st.warning("Nenhum SKU/EAN encontrado na planilha.")
                else:
                    ins, upd, err = upsert_eans(registros)
                    st.success(
                        f"✅ **{ins}** inseridos · **{upd}** atualizados"
                        + (f" · **{err}** erros" if err else "")
                    )
                    st.rerun()
            except Exception as e:
                st.error(f"Erro ao ler planilha: {e}")

    st.divider()

    # Visualizar base
    with st.expander("🔍 Ver base de EANs", expanded=False):
        registros_db = listar_todos()
        if registros_db:
            df_db = pd.DataFrame(registros_db)
            st.dataframe(df_db, use_container_width=True, height=250)
        else:
            st.info("Base vazia. Importe uma planilha de EANs.")




# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------

st.markdown("""
<div class="app-header">
    <h1>NF-e Editor · Importação DUIMP / XML F5</h1>
    <p>Ajustes automáticos para emissão no Bling · EAN · ICMS zerado · IPI · PIS/COFINS</p>
</div>
""", unsafe_allow_html=True)


# ===========================================================================
# ETAPA 1 — INPUT
# ===========================================================================

if st.session_state.stage == "input":

    st.markdown(_step_badge("Etapa 1 de 3 · Carregar arquivos"), unsafe_allow_html=True)
    st.markdown("### Carregue os XMLs e a Planilha XML F5")

    col1, col2 = st.columns([2, 1])

    with col1:
        uploaded_xmls = st.file_uploader(
            "XMLs NF-e (um ou mais)",
            type=["xml"],
            accept_multiple_files=True,
            help="Selecione todos os XMLs que deseja processar de uma vez.",
        )

    with col2:
        plan_fiscal = st.file_uploader(
            "Planilha XML F5",
            type=["xlsx", "xls", "csv"],
            help="Planilha F5 ou espelho HAGN008 com SKU/Part Number, Siscomex, AFRMM e, se houver, Base/Alq/Valor de IBS e CBS.",
        )

    st.divider()

    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        analisar = st.button(
            "🔍 Analisar XMLs",
            use_container_width=True,
            type="primary",
            disabled=(not uploaded_xmls or not plan_fiscal),
        )

    with col_info:
        if not uploaded_xmls:
            st.info("Aguardando XMLs...")
        elif not plan_fiscal:
            st.info("Aguardando Planilha XML F5...")
        else:
            st.success(f"✅ {len(uploaded_xmls)} XML(s) · Planilha XML F5 pronta")

    if analisar:
        # Carrega planilha fiscal
        try:
            mapa_fiscal = carregar_planilha_xmlf5(plan_fiscal.getvalue(), plan_fiscal.name)
        except Exception as e:
            st.error(f"Erro ao ler Planilha XML F5: {e}")
            st.stop()

        # Carrega EANs da base
        mapa_ean = get_all_eans()

        # Pré-analisa XMLs para encontrar itens sem EAN
        xmls_carregados = [(f.name, f.getvalue()) for f in uploaded_xmls]
        faltantes = []

        for nome, xml_bytes in xmls_carregados:
            try:
                from lxml import etree
                parser = etree.XMLParser(recover=True, huge_tree=True)
                tree = etree.parse(io.BytesIO(xml_bytes), parser)
                root = tree.getroot()
                for det in root.xpath(".//*[local-name()='det']"):
                    prod = next(iter(det.xpath("./*[local-name()='prod']")), None)
                    if prod is None:
                        continue
                    cprod = next(iter(prod.xpath("./*[local-name()='cProd']/text()")), "").strip()
                    if not cprod:
                        continue
                    if not mapa_ean.get(cprod):
                        xprod = next(iter(prod.xpath("./*[local-name()='xProd']/text()")), "").strip()
                        cean = next(iter(prod.xpath("./*[local-name()='cEAN']/text()")), "").strip()
                        faltantes.append({
                            "Arquivo XML": nome,
                            "nItem": det.get("nItem", ""),
                            "SKU": cprod,
                            "Descrição": xprod,
                            "cEAN no XML": cean,
                            "EAN (preencher)": "",
                        })
            except Exception as e:
                st.warning(f"Não foi possível pré-analisar {nome}: {e}")

        st.session_state.xmls_carregados = xmls_carregados
        st.session_state.mapa_fiscal = mapa_fiscal
        st.session_state.faltantes = faltantes

        if faltantes:
            st.session_state.stage = "fill_ean"
        else:
            # Sem faltantes: processa direto
            resultados = []
            with st.spinner("Processando XMLs..."):
                mapa_ean = get_all_eans()
                for nome, xml_bytes in xmls_carregados:
                    xml_out, stats = processar_xml(xml_bytes, mapa_ean, nome, st.session_state.mapa_fiscal)
                    resultados.append({"nome_original": nome, "xml_processado": xml_out, "stats": stats})
            st.session_state.resultados = resultados
            st.session_state.stage = "results"

        st.rerun()


# ===========================================================================
# ETAPA 2 — PREENCHIMENTO MANUAL DE EAN
# ===========================================================================

elif st.session_state.stage == "fill_ean":

    faltantes = st.session_state.faltantes

    st.markdown(_step_badge("Etapa 2 de 3 · Preencher EANs ausentes"), unsafe_allow_html=True)
    st.markdown("### Itens sem EAN na base")

    st.markdown(f"""
    <div class="ean-warning">
        ⚠️ <strong>{len(faltantes)} item(ns)</strong> não encontrados na base de EANs.
        Preencha os EANs abaixo e eles serão salvos automaticamente na base.
        Campos deixados em branco serão preservados como estão no XML.
    </div>
    """, unsafe_allow_html=True)

    df_faltantes = pd.DataFrame(faltantes)

    edited = st.data_editor(
        df_faltantes,
        column_config={
            "EAN (preencher)": st.column_config.TextColumn(
                "EAN (preencher)",
                help="Digite o EAN/GTIN de 8, 12, 13 ou 14 dígitos.",
                width="medium",
            ),
            "Arquivo XML": st.column_config.TextColumn(width="medium"),
            "nItem": st.column_config.TextColumn(width="small"),
            "SKU": st.column_config.TextColumn(width="medium"),
            "Descrição": st.column_config.TextColumn(width="large"),
            "cEAN no XML": st.column_config.TextColumn(width="medium"),
        },
        disabled=["Arquivo XML", "nItem", "SKU", "Descrição", "cEAN no XML"],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
    )

    st.divider()

    col_b1, col_b2, _ = st.columns([1, 1, 2])

    with col_b1:
        processar_btn = st.button("✅ Salvar EANs e Processar", type="primary", use_container_width=True)
    with col_b2:
        if st.button("⬅️ Voltar", use_container_width=True):
            st.session_state.stage = "input"
            st.rerun()

    if processar_btn:
        # Salva EANs preenchidos manualmente
        salvos = 0
        for _, row in edited.iterrows():
            ean_raw = str(row.get("EAN (preencher)", "")).strip()
            if ean_raw:
                ean_digits = _limpar_digitos(ean_raw)
                if ean_digits:
                    salvar_ean_manual(row["SKU"], ean_digits, row.get("Descrição", ""))
                    salvos += 1

        if salvos:
            st.toast(f"{salvos} EAN(s) salvos na base!", icon="✅")

        # Processa XMLs
        mapa_ean = get_all_eans()
        resultados = []
        with st.spinner("Processando XMLs..."):
            for nome, xml_bytes in st.session_state.xmls_carregados:
                xml_out, stats = processar_xml(xml_bytes, mapa_ean, nome, st.session_state.mapa_fiscal)
                resultados.append({"nome_original": nome, "xml_processado": xml_out, "stats": stats})

        st.session_state.resultados = resultados
        st.session_state.stage = "results"
        st.rerun()


# ===========================================================================
# ETAPA 3 — RESULTADOS
# ===========================================================================

elif st.session_state.stage == "results":

    resultados = st.session_state.resultados

    st.markdown(_step_badge("Etapa 3 de 3 · Resultados"), unsafe_allow_html=True)
    st.markdown("### Processamento concluído")

    # Totalizadores rápidos
    total_xml = len(resultados)
    total_ok  = sum(1 for r in resultados if not r["stats"]["erros"] and r["xml_processado"])
    total_ean_criados = sum(r["stats"].get("ean_criados", 0) for r in resultados)
    total_ean_ausentes = sum(r["stats"].get("ean_ausentes", 0) for r in resultados)
    total_icms = sum(r["stats"].get("icms_zerados", 0) for r in resultados)
    total_ibscbs = sum(r["stats"].get("ibscbs_itens_gerados", 0) for r in resultados)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    for col, num, lbl in [
        (c1, total_xml, "XMLs processados"),
        (c2, total_ok, "Com sucesso"),
        (c3, total_ean_criados, "EANs inseridos"),
        (c4, total_ean_ausentes, "Itens sem EAN"),
        (c5, total_icms, "ICMS zerados"),
        (c6, total_ibscbs, "IBS/CBS gerados"),
    ]:
        col.markdown(_stat_card(num, lbl), unsafe_allow_html=True)

    st.divider()

    # Alerta se ainda há itens sem EAN
    if total_ean_ausentes > 0:
        st.markdown(f"""
        <div class="ean-warning">
            ⚠️ <strong>{total_ean_ausentes} item(ns)</strong> ainda sem EAN na base.
            O XML foi preservado (cEAN/cEANTrib originais mantidos).
            Baixe o relatório Excel abaixo para ver o detalhe.
        </div>
        """, unsafe_allow_html=True)

    # Por arquivo
    st.markdown("#### Detalhes por arquivo")

    for r in resultados:
        nome  = r["nome_original"]
        stats = r["stats"]
        xml_out = r["xml_processado"]

        with st.container(border=True):
            col_nome, col_dl = st.columns([4, 1])
            with col_nome:
                status_icon = "🔴" if stats["erros"] else ("🟡" if stats["ean_ausentes"] else "🟢")
                st.markdown(f"**{status_icon} {nome}**")

            with col_dl:
                if xml_out:
                    base = nome.rsplit(".", 1)[0]
                    st.download_button(
                        label="⬇️ Download XML",
                        data=xml_out,
                        file_name=f"{base}_processado.xml",
                        mime="application/xml",
                        use_container_width=True,
                        key=f"dl_{nome}",
                    )

            if stats["erros"]:
                for e in stats["erros"]:
                    st.error(e)
                continue

            # Stats em colunas compactas
            s = stats
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.markdown("**EAN**")
                st.write(f"Criados: {s['ean_criados']} · Atualizados: {s['ean_atualizados']}")
                st.write(f"Preservados (sem base): {s['ean_preservados']} · Ausentes: {s['ean_ausentes']}")
            with col_b:
                st.markdown("**ICMS / Fiscal**")
                st.write(f"CFOP ajustados: {s.get('cfop_ajustados', 0)} · ICMS zerados: {s['icms_zerados']}")
                st.write(f"infCpl limpos: {s['infcpl_limpa']} · cFabricante: {s['cfabricante_inseridos']} ins / {s['cfabricante_atualizados']} upd")
            with col_c:
                st.markdown("**IPI / PIS / COFINS / IBS-CBS**")
                st.write(f"IPI CST→03: {s['ipi_cst_alterados']+s['ipi_cst_criados']}")
                st.write(f"PIS CST→50: {s['pis_cst_alterados']} · COFINS: {s['cofins_cst_alterados']}")
                st.write(f"IBS/CBS: {s.get('ibscbs_itens_gerados', 0)} item(ns) · total: {s.get('ibscbs_totais_gerados', 0)}")

            # Itens sem EAN deste arquivo
            falt = s.get("faltantes_detalhado", [])
            if falt:
                with st.expander(f"⚠️ {len(falt)} item(ns) sem EAN neste arquivo"):
                    st.dataframe(pd.DataFrame(falt), use_container_width=True, hide_index=True)

            # Avisos
            if s["avisos"]:
                with st.expander(f"💬 {len(s['avisos'])} aviso(s)"):
                    for a in s["avisos"][:50]:
                        st.write(f"– {a}")
                    if len(s["avisos"]) > 50:
                        st.write(f"... +{len(s['avisos'])-50} avisos omitidos")

    st.divider()

    # Downloads globais
    col_z1, col_z2, col_z3 = st.columns(3)

    with col_z1:
        if len(resultados) > 1:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for r in resultados:
                    if r["xml_processado"]:
                        base = r["nome_original"].rsplit(".", 1)[0]
                        zf.writestr(f"{base}_processado.xml", r["xml_processado"])
            zip_buf.seek(0)
            st.download_button(
                label="⬇️ Baixar todos os XMLs (.zip)",
                data=zip_buf,
                file_name="xmls_processados.zip",
                mime="application/zip",
                use_container_width=True,
            )

    with col_z2:
        rel_bytes = gerar_relatorio_faltantes(resultados)
        st.download_button(
            label="📊 Relatório Excel (SKUs sem EAN)",
            data=rel_bytes,
            file_name="relatorio_skus_sem_ean.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    with col_z3:
        if st.button("🔄 Processar novos XMLs", use_container_width=True):
            st.session_state.stage = "input"
            st.session_state.xmls_carregados = []
            st.session_state.faltantes = []
            st.session_state.resultados = []
            st.rerun()
