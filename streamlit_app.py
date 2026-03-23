import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
from io import BytesIO

# ─── Page Config ───
st.set_page_config(
    page_title="IFEMA Liquidator",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&display=swap');
    html, body, [class*="css"] { font-family: 'Nunito', sans-serif; }
    .main-header {
        background: linear-gradient(135deg, #0d7377, #0a5c5f);
        padding: 1.5rem 2rem; border-radius: 12px; color: white; margin-bottom: 1.5rem;
    }
    .main-header h1 { margin: 0; font-size: 1.4rem; font-weight: 900; }
    .main-header p { margin: 0; font-size: 0.75rem; opacity: 0.7; }
    .metric-card {
        background: white; border: 1px solid #e8ecf1; border-radius: 14px;
        padding: 1.2rem; text-align: center;
    }
    .metric-card .label { font-size: 0.65rem; font-weight: 700; color: #9ca3af; text-transform: uppercase; letter-spacing: 0.08em; }
    .metric-card .value { font-size: 2rem; font-weight: 900; margin-top: 0.3rem; }
    .metric-accent {
        background: linear-gradient(135deg, #0d7377, #0a5c5f);
        border: none; color: white; box-shadow: 0 8px 24px rgba(13, 115, 119, 0.25);
    }
    .metric-accent .label { color: rgba(255,255,255,0.7); }
    .metric-accent .value { color: white; }
    .concept-card {
        background: white; border: 1.5px solid #e8ecf1; border-radius: 12px; padding: 1rem;
    }
    .concept-card.active { border-color: #b2e0e0; }
    .concept-card .clabel { font-size: 0.55rem; font-weight: 700; color: #9ca3af; text-transform: uppercase; }
    .concept-card .cvalue { font-size: 1.3rem; font-weight: 900; color: #0d7377; margin-top: 0.2rem; }
    .concept-card .ctip { font-size: 0.55rem; color: #c4c9d2; margin-top: 0.2rem; }
    .concept-card.inactive { opacity: 0.35; }
    .concept-card.inactive .cvalue { color: #d1d5db; }
    .badge-fin { background: #fef2f2; color: #dc2626; padding: 2px 8px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; }
    .badge-activo { background: #f0fdf4; color: #16a34a; padding: 2px 8px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; }
    .mode-banner {
        padding: 0.6rem 1rem; border-radius: 8px; font-size: 0.75rem; font-weight: 700;
        margin-bottom: 1rem; display: flex; align-items: center; gap: 8px;
    }
    .mode-liq { background: #fef2f2; color: #dc2626; border: 1px solid #fee2e2; }
    .mode-mens { background: #eff6ff; color: #2563eb; border: 1px solid #dbeafe; }
    div[data-testid="stSidebar"] { background: white; }
    .stDownloadButton button {
        background: #059669 !important; color: white !important;
        border: none !important; font-weight: 700 !important;
    }
</style>
""", unsafe_allow_html=True)


# ─── Helpers ───
def parse_date(val):
    if pd.isna(val) or val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime.combine(val, datetime.min.time())
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    s = str(val).strip().strip('"')
    if not s:
        return None
    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"]:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        return pd.to_datetime(s).to_pydatetime()
    except Exception:
        return None


def parse_time_to_hours(val):
    """Parse time string like '23:30' or '06:15' to decimal hours (23.5, 6.25)"""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip().strip('"')
    if not s:
        return None
    import re
    m = re.match(r'(\d{1,2}):(\d{2})', s)
    if m:
        return int(m.group(1)) + int(m.group(2)) / 60
    try:
        return float(s.replace(",", "."))
    except (ValueError, TypeError):
        return None


def find_col(df, keywords):
    if df is None or df.empty:
        return None
    cols = df.columns.tolist()
    for kw in keywords:
        for c in cols:
            if c.lower().strip().strip('"') == kw.lower().strip():
                return c
    for kw in keywords:
        for c in cols:
            if kw.lower().strip() in c.lower().strip().strip('"'):
                return c
    return None


def normalize(val):
    return str(val or "").replace('"', '').replace(',', '').replace(' ', '').upper()


def parse_float(val):
    if pd.isna(val) or val is None:
        return 0.0
    try:
        return float(str(val).replace(",", "."))
    except (ValueError, TypeError):
        return 0.0


def read_file(uploaded):
    if uploaded is None:
        return None
    name = uploaded.name.lower()
    try:
        if name.endswith(".csv") or name.endswith(".tsv") or name.endswith(".txt"):
            content = uploaded.getvalue().decode("utf-8", errors="replace")
            first_line = content.split("\n")[0]
            if "\t" in first_line:
                return pd.read_csv(uploaded, sep="\t")
            elif ";" in first_line:
                uploaded.seek(0)
                return pd.read_csv(uploaded, sep=";")
            else:
                uploaded.seek(0)
                return pd.read_csv(uploaded)
        else:
            return pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"Error leyendo {uploaded.name}: {e}")
        return None


def get_employees_with_status(df_contratos, df_jornadas, df_tramos, df_absentismos):
    keywords = ["Empleado", "Nombre", "Persona", "Trabajador"]
    employees = {}

    for df in [df_contratos, df_jornadas, df_tramos, df_absentismos]:
        if df is None or df.empty:
            continue
        col = find_col(df, keywords)
        if not col:
            continue
        for val in df[col].dropna().unique():
            name = str(val).strip().strip('"')
            if name and name.upper() != "UNDEFINED":
                n = normalize(name)
                if n not in employees:
                    employees[n] = (name, False, None)

    if df_contratos is not None and not df_contratos.empty:
        col_emp = find_col(df_contratos, keywords)
        col_fin = find_col(df_contratos, ["Fecha fin", "Hasta"])
        col_fin_prev = find_col(df_contratos, ["Fecha fin prevista"])

        if col_emp:
            for _, row in df_contratos.iterrows():
                name = str(row[col_emp]).strip().strip('"')
                n = normalize(name)
                f_fin = None
                if col_fin:
                    f_fin = parse_date(row[col_fin])
                if not f_fin and col_fin_prev:
                    f_fin = parse_date(row[col_fin_prev])
                if n in employees:
                    if f_fin:
                        employees[n] = (employees[n][0], True, f_fin.strftime("%d/%m/%Y"))
                    else:
                        employees[n] = (employees[n][0], False, None)

    return sorted(employees.values(), key=lambda x: x[0])


def calc_night_hours(h_ini, h_fin):
    """Calculate complete hours between 22:00-06:00 given start/end decimal hours.
    Returns (plus_noct_hours, is_comp_nocturno)
    - plus_noct_hours: complete hours in 22:00-06:00 range
    - is_comp_nocturno: True if end >= 23:00 (or next day)
    """
    if h_ini is None or h_fin is None:
        return 0, False

    # Handle overnight: if end <= start, assume next day
    overnight = h_fin <= h_ini
    comp_nocturno = h_fin >= 23 or overnight  # fin >= 23:00 or crosses midnight

    # Calculate hours in 22:00-06:00 band
    night_h = 0.0

    if overnight:
        # e.g. 20:00 to 02:00
        # Part 1: from start to midnight in 22-24 range
        if h_ini < 22:
            night_h += min(24, 24) - 22  # 2h from 22 to 00
        else:
            night_h += 24 - h_ini  # from start to midnight
        # Part 2: from midnight to end in 00-06 range
        night_h += min(h_fin, 6)
    else:
        # Same day
        # 22:00-24:00 portion
        if h_fin > 22:
            night_h += min(h_fin, 24) - max(h_ini, 22)
        # 00:00-06:00 portion
        if h_ini < 6:
            night_h += min(h_fin, 6) - h_ini

    # Only complete hours
    night_h = max(0, int(night_h))
    return night_h, comp_nocturno


def export_to_excel(result, mode):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        wb = writer.book
        ws = wb.add_worksheet("Liquidación")
        writer.sheets["Liquidación"] = ws

        title_fmt = wb.add_format({"bold": True, "font_size": 14, "font_color": "#0d7377"})
        num_fmt = wb.add_format({"num_format": "0.00", "font_size": 11})
        num3_fmt = wb.add_format({"num_format": "0.000", "font_size": 11})
        bold_fmt = wb.add_format({"bold": True, "font_size": 11})
        accent_fmt = wb.add_format({"bold": True, "font_size": 14, "font_color": "#0d7377", "num_format": "0.00"})

        r = 0
        titulo = "LIQUIDACIÓN ETT — IFEMA" if mode == "liquidacion" else "CÁLCULO MENSUAL HORAS — IFEMA"
        ws.write(r, 0, titulo, title_fmt); r += 2
        ws.write(r, 0, "Empleado", bold_fmt); ws.write(r, 1, result["empleado"]); r += 1
        if mode == "liquidacion":
            ws.write(r, 0, "Fecha fin contrato", bold_fmt); ws.write(r, 1, result.get("f_fin", "")); r += 1
        ws.write(r, 0, "Fecha informe", bold_fmt); ws.write(r, 1, datetime.now().strftime("%d/%m/%Y")); r += 2

        if mode == "liquidacion":
            ws.write(r, 0, "VACACIONES", title_fmt); r += 1
            ws.write(r, 0, "Periodo devengo", bold_fmt); ws.write(r, 1, result["per_dev"]); r += 1
            ws.write(r, 0, "Días devengo", bold_fmt); ws.write(r, 1, result["dias_dev"]); r += 1
            ws.write(r, 0, "Devengadas", bold_fmt); ws.write(r, 1, result["dev"], num3_fmt); r += 1
            ws.write(r, 0, "Disfrutadas", bold_fmt); ws.write(r, 1, result["dis"], num_fmt); r += 1
            ws.write(r, 0, "SALDO VACACIONES", bold_fmt); ws.write(r, 1, result["saldo"], accent_fmt); r += 2

        ws.write(r, 0, "CONCEPTOS HORA", title_fmt)
        ws.write(r, 1, "Periodo: " + result["per_rev"]); r += 1
        for label, key in [
            ("Complementarias L-V", "compLV"),
            ("Plus SDF", "plusSDF"),
            ("Festivo H.Comp.", "festHComp"),
            ("Horas Especiales", "hEspeciales"),
            ("Complemento Festivo", "compFestivo"),
            ("Comp. Nocturnos", "compNocturno"),
            ("Plus Nocturnidad", "plusNoct"),
        ]:
            ws.write(r, 0, label, bold_fmt)
            ws.write(r, 1, result[key], num_fmt)
            r += 1
        r += 1

        if result["detalle"]:
            df_det = pd.DataFrame(result["detalle"])
            df_det.to_excel(writer, sheet_name="Detalle Jornadas", index=False)

        ws.set_column(0, 0, 24)
        ws.set_column(1, 1, 18)

    return output.getvalue()


# ─── SIDEBAR ───
with st.sidebar:
    st.markdown("""
    <div style="display:flex; align-items:center; gap:10px; margin-bottom:1.5rem;">
        <div style="width:34px; height:34px; border-radius:9px; background:#0d7377; display:flex; align-items:center; justify-content:center; color:white; font-size:12px; font-weight:900;">IF</div>
        <div>
            <div style="font-size:14px; font-weight:800;">IFEMA <span style="color:#0d7377;">Liquidator</span></div>
            <div style="font-size:10px; color:#9ca3af;">Finiquitos ETT · Endalia HR</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("##### 📂 Informes Endalia")
    f_contratos = st.file_uploader("Contratos", type=["xlsx", "xls", "csv"], key="contratos")
    f_jornadas = st.file_uploader("Jornadas", type=["xlsx", "xls", "csv"], key="jornadas")
    f_tramos = st.file_uploader("Tramos", type=["xlsx", "xls", "csv"], key="tramos")
    f_absentismos = st.file_uploader("Absentismos (opcional)", type=["xlsx", "xls", "csv"], key="absentismos")

    df_contratos = read_file(f_contratos)
    df_jornadas = read_file(f_jornadas)
    df_tramos = read_file(f_tramos)
    df_absentismos = read_file(f_absentismos)

    st.markdown("---")

    # Employee selector with status
    emp_list = get_employees_with_status(df_contratos, df_jornadas, df_tramos, df_absentismos)
    st.markdown("##### 👤 Empleado")

    if emp_list:
        options = []
        for name, has_end, end_str in emp_list:
            if has_end:
                options.append(f"🔴 {name}  (Fin: {end_str})")
            else:
                options.append(f"🟢 {name}  (Activo)")

        selected_idx = st.selectbox(
            "Seleccionar", range(len(options)),
            format_func=lambda i: options[i], label_visibility="collapsed",
        )
        selected_emp = emp_list[selected_idx][0]
        emp_has_end = emp_list[selected_idx][1]
    else:
        selected_emp = None
        emp_has_end = False
        st.info("Sube un archivo para ver empleados")

    st.markdown("---")

    # Config
    st.markdown("##### ⚙️ Configuración")
    fecha_corte = st.date_input("Fecha de corte (último día pagado)",
                                 value=datetime(2026, 3, 1),
                                 help="Horas calculadas desde el día siguiente")

    if selected_emp and not emp_has_end:
        fecha_hasta = st.date_input("Fecha hasta (cálculo horas)",
                                     value=datetime.now().date(),
                                     help="Para empleados activos: calcular horas hasta esta fecha")
    else:
        fecha_hasta = None

    with st.expander("Configuración avanzada"):
        col1, col2 = st.columns(2)
        with col1:
            inicio_ciclo = st.date_input("Inicio devengo", value=datetime(2026, 1, 1))
        with col2:
            fin_ciclo = st.date_input("Fin devengo", value=datetime(2026, 12, 31))
        max_vac = st.number_input("Días vacaciones/año", value=22, min_value=1, max_value=40)
        periodo_filtro = st.text_input("Periodo filtro absentismos", value="2026")

    st.markdown("---")

    if selected_emp and emp_has_end:
        btn_label = "🔴 Calcular LIQUIDACIÓN"
    elif selected_emp:
        btn_label = "🟢 Calcular HORAS MENSUALES"
    else:
        btn_label = "Calcular"

    btn_calculate = st.button(
        btn_label, type="primary", use_container_width=True,
        disabled=not (df_contratos is not None and df_jornadas is not None and selected_emp),
    )


# ─── MAIN ───
st.markdown("""
<div class="main-header">
    <h1>📋 IFEMA Liquidator</h1>
    <p>Calculadora de liquidación ETT desde Endalia HR</p>
</div>
""", unsafe_allow_html=True)


if btn_calculate and selected_emp:
    try:
        # ─── Determine mode ───
        mode = "liquidacion" if emp_has_end else "mensual"

        # ─── Find contract ───
        col_emp_ct = find_col(df_contratos, ["Empleado", "Nombre", "Persona"])
        ct_match = df_contratos[df_contratos[col_emp_ct].apply(lambda x: normalize(x) == normalize(selected_emp))]

        if ct_match.empty:
            st.error(f"No se encontró contrato para {selected_emp}")
            st.stop()

        ct = ct_match.iloc[0]
        col_ini = find_col(df_contratos, ["Fecha inicio", "Desde", "Fecha de inicio"])
        col_fin_ct = find_col(df_contratos, ["Fecha fin", "Hasta"])
        col_fin_prev = find_col(df_contratos, ["Fecha fin prevista"])

        f_inicio = parse_date(ct[col_ini]) if col_ini else None
        f_fin = None
        if col_fin_ct:
            f_fin = parse_date(ct[col_fin_ct])
        if not f_fin and col_fin_prev:
            f_fin = parse_date(ct[col_fin_prev])

        # ─── End date for hours ───
        if mode == "liquidacion" and f_fin:
            horas_hasta = f_fin
        elif fecha_hasta:
            horas_hasta = datetime.combine(fecha_hasta, datetime.min.time())
        else:
            horas_hasta = datetime.now()

        # ─── JORNADAS: detect columns ───
        d_corte = datetime.combine(fecha_corte, datetime.min.time())
        rev_ini = d_corte + timedelta(days=1)

        col_emp_j = find_col(df_jornadas, ["Empleado", "Nombre", "Persona"])
        col_fecha_j = find_col(df_jornadas, ["Día registro", "Fecha", "Día de registro"])
        col_horas_j = find_col(df_jornadas, ["Tiempo trabajado", "Horas"])
        col_tipo_dia = find_col(df_jornadas, ["Tipo de día", "Tipo dia"])
        col_tipo_fes = find_col(df_jornadas, ["Tipo de festivo"])
        col_dif = find_col(df_jornadas, ["Diferencia con horas especiales", "Diferencia"])

        jornadas_emp = df_jornadas[
            df_jornadas[col_emp_j].apply(lambda x: normalize(x) == normalize(selected_emp))
        ].copy()

        # ─── TRAMOS: detect columns ───
        tramos_emp = None
        col_emp_t = col_fecha_t = col_hini_t = col_hfin_t = None
        if df_tramos is not None and not df_tramos.empty:
            col_emp_t = find_col(df_tramos, ["Empleado", "Nombre", "Persona"])
            col_fecha_t = find_col(df_tramos, ["Día de registro", "Día registro", "Fecha"])
            col_hini_t = find_col(df_tramos, ["Hora inicio"])
            col_hfin_t = find_col(df_tramos, ["Hora fin"])
            if col_emp_t:
                tramos_emp = df_tramos[
                    df_tramos[col_emp_t].apply(lambda x: normalize(x) == normalize(selected_emp))
                ].copy()

        # ─── Initialize accumulators ───
        compLV = 0.0       # 1. Complementarias L-V
        plusSDF = 0.0       # 2. Plus SDF (min(H,7) en sáb/dom/fest)
        festHComp = 0.0     # 3. Festivo H.Comp (exceso >7h)
        hEspeciales = 0.0   # 4. Horas Especiales (H>11 → min(H-11,1))
        compFestivo = 0.0   # 5. Complemento Festivo (sáb/dom, H≥4 → H-4)
        compNocturno = 0    # 6. Comp. Nocturnos (count of days)
        plusNoct = 0.0       # 7. Plus Nocturnidad (complete hours 22-06)
        detalle = []

        # ─── Track nocturno days to avoid double-counting ───
        nocturno_days = set()

        # ─── Process JORNADAS ───
        for _, row in jornadas_emp.iterrows():
            f = parse_date(row[col_fecha_j])
            if not f or f < rev_ini or f > horas_hasta:
                continue
            h = parse_float(row[col_horas_j])
            if h <= 0:
                continue

            # Detect festivo/SDF
            es_sdf = False
            if col_tipo_dia:
                td = str(row[col_tipo_dia]).lower()
                es_sdf = "festivo" in td or "semana" in td
            if not es_sdf and col_tipo_fes:
                tf = str(row.get(col_tipo_fes, "")).strip()
                es_sdf = tf != "" and tf.lower() != "nan"

            # Detect sáb/dom specifically (for Comp. Festivo)
            es_sabdom = f.weekday() in (5, 6)  # 5=Sat, 6=Sun

            dif_lab = 0.0
            dia_plus_sdf = 0.0
            dia_fest_hcomp = 0.0
            dia_h_especiales = 0.0
            dia_comp_festivo = 0.0

            if es_sdf:
                # 2. Plus SDF: first 7h
                dia_plus_sdf = min(h, 7)
                plusSDF += dia_plus_sdf

                # 3. Festivo H.Comp: excess over 7h
                if h > 7:
                    dia_fest_hcomp = h - 7
                    festHComp += dia_fest_hcomp

                # 5. Comp. Festivo: only sáb/dom, H ≥ 4h → H-4
                if es_sabdom and h >= 4:
                    dia_comp_festivo = h - 4
                    compFestivo += dia_comp_festivo
            else:
                # 1. Complementarias L-V: use Diferencia column
                if col_dif:
                    dif_lab = parse_float(row[col_dif])
                    compLV += dif_lab
                else:
                    col_plan = find_col(df_jornadas, ["Tiempo planificado", "Planificado"])
                    plan = parse_float(row[col_plan]) if col_plan else 0.0
                    dif_lab = h - plan
                    compLV += dif_lab

            # 4. Horas Especiales: any day, H > 11h → min(H-11, 1)
            if h > 11:
                dia_h_especiales = min(h - 11, 1)
                hEspeciales += dia_h_especiales

            detalle.append({
                "Fecha": f.strftime("%d/%m/%Y"),
                "Horas": round(h, 2),
                "Tipo": "Festivo" if es_sdf else "Laborable",
                "Dif.L-V": round(dif_lab, 2) if not es_sdf else None,
                "Plus SDF": round(dia_plus_sdf, 2) if es_sdf else None,
                "Fest.HComp": round(dia_fest_hcomp, 2) if dia_fest_hcomp > 0 else None,
                "H.Esp.": round(dia_h_especiales, 2) if dia_h_especiales > 0 else None,
                "Comp.Fest.": round(dia_comp_festivo, 2) if dia_comp_festivo > 0 else None,
            })

        detalle.sort(key=lambda x: x["Fecha"])

        # ─── Process TRAMOS for nocturnidad ───
        if tramos_emp is not None and not tramos_emp.empty and col_fecha_t and col_hfin_t:
            for _, row in tramos_emp.iterrows():
                f = parse_date(row[col_fecha_t])
                if not f or f < rev_ini or f > horas_hasta:
                    continue

                h_ini = parse_time_to_hours(row[col_hini_t]) if col_hini_t else None
                h_fin = parse_time_to_hours(row[col_hfin_t])

                if h_fin is None:
                    continue

                night_h, is_comp = calc_night_hours(h_ini, h_fin)

                # 7. Plus Nocturnidad: complete hours in 22-06
                plusNoct += night_h

                # 6. Comp. Nocturno: 1 unit/day if end >= 23:00
                if is_comp:
                    day_key = f.strftime("%Y-%m-%d")
                    if day_key not in nocturno_days:
                        nocturno_days.add(day_key)
                        compNocturno += 1

        # ─── Round everything ───
        compLV = round(compLV, 2)
        plusSDF = round(plusSDF, 2)
        festHComp = round(festHComp, 2)
        hEspeciales = round(hEspeciales, 2)
        compFestivo = round(compFestivo, 2)
        plusNoct = round(plusNoct, 2)

        # ─── Vacation calc (only liquidacion mode) ───
        devengadas = 0.0
        disfrutadas = 0.0
        saldo = 0.0
        dias_dev = 0
        per_dev = ""

        if mode == "liquidacion" and f_fin:
            d_ini_ciclo = datetime.combine(inicio_ciclo, datetime.min.time())
            d_fin_ciclo = datetime.combine(fin_ciclo, datetime.min.time())
            ciclo_total = (d_fin_ciclo - d_ini_ciclo).days + 1

            dev_ini = max(f_inicio, d_ini_ciclo) if f_inicio else d_ini_ciclo
            dias_dev = max(0, (f_fin - dev_ini).days + 1)
            devengadas = (dias_dev * max_vac) / ciclo_total
            per_dev = f"{dev_ini.strftime('%d/%m/%Y')} – {f_fin.strftime('%d/%m/%Y')}"

            if df_absentismos is not None and not df_absentismos.empty:
                col_emp_a = find_col(df_absentismos, ["Empleado", "Nombre"])
                col_dur_a = find_col(df_absentismos, ["Duración", "Valor"])
                col_uni_a = find_col(df_absentismos, ["Unidad"])
                col_per_a = find_col(df_absentismos, ["Periodo", "Año"])

                abs_emp = df_absentismos[
                    df_absentismos[col_emp_a].apply(lambda x: normalize(x) == normalize(selected_emp))
                ]
                if periodo_filtro and col_per_a:
                    abs_emp = abs_emp[abs_emp[col_per_a].astype(str).str.strip() == periodo_filtro]

                for _, row in abs_emp.iterrows():
                    val = parse_float(row[col_dur_a])
                    if col_uni_a and "hora" in str(row[col_uni_a]).lower():
                        val = val / 7
                    disfrutadas += val

            saldo = devengadas - disfrutadas

        # ─── Build result ───
        result = {
            "mode": mode,
            "empleado": selected_emp,
            "f_fin": f_fin.strftime("%d/%m/%Y") if f_fin else None,
            "per_rev": f"{rev_ini.strftime('%d/%m/%Y')} – {horas_hasta.strftime('%d/%m/%Y')}",
            "per_dev": per_dev,
            "dias_dev": dias_dev,
            "dev": round(devengadas, 3),
            "dis": round(disfrutadas, 2),
            "saldo": round(saldo, 2),
            "compLV": compLV,
            "plusSDF": plusSDF,
            "festHComp": festHComp,
            "hEspeciales": hEspeciales,
            "compFestivo": compFestivo,
            "compNocturno": compNocturno,
            "plusNoct": plusNoct,
            "detalle": detalle,
        }
        st.session_state["result"] = result

    except Exception as e:
        st.error(f"Error en cálculo: {e}")
        import traceback
        st.code(traceback.format_exc())


# ─── Display Results ───
if "result" in st.session_state:
    r = st.session_state["result"]
    is_liq = r["mode"] == "liquidacion"

    # Mode banner
    if is_liq:
        st.markdown(f'<div class="mode-banner mode-liq">🔴 LIQUIDACIÓN — Fin de contrato: {r["f_fin"]}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="mode-banner mode-mens">🟢 CÁLCULO MENSUAL — Empleado activo</div>', unsafe_allow_html=True)

    # Header + export
    col_head, col_export = st.columns([3, 1])
    with col_head:
        badges = ""
        if is_liq:
            badges += f'<span style="padding:3px 10px; border-radius:6px; background:#fef2f2; font-size:0.65rem; font-weight:700; color:#dc2626;">Baja: {r["f_fin"]}</span> '
            badges += f'<span style="padding:3px 10px; border-radius:6px; background:#e8f6f6; font-size:0.65rem; font-weight:700; color:#0d7377;">Devengo: {r["dias_dev"]}d</span> '
        badges += f'<span style="padding:3px 10px; border-radius:6px; background:#f4f5f8; font-size:0.65rem; font-weight:700; color:#9ca3af;">Horas: {r["per_rev"]}</span>'

        st.markdown(f"""
        <div style="background:white; border:1px solid #e8ecf1; border-radius:14px; padding:1.2rem 1.5rem; margin-bottom:1rem;">
            <div style="font-size:0.65rem; font-weight:700; color:#0d7377; text-transform:uppercase; letter-spacing:0.1em;">
                {"Liquidación" if is_liq else "Cálculo mensual"}
            </div>
            <div style="font-size:1.4rem; font-weight:900; color:#111827; margin-top:0.2rem;">{r['empleado']}</div>
            <div style="display:flex; gap:6px; margin-top:0.5rem; flex-wrap:wrap;">{badges}</div>
        </div>
        """, unsafe_allow_html=True)
    with col_export:
        st.markdown("<br>", unsafe_allow_html=True)
        xlsx_data = export_to_excel(r, r["mode"])
        filename = "Liquidacion" if is_liq else "Horas_Mensual"
        st.download_button(
            "📥 Descargar .xlsx", data=xlsx_data,
            file_name=f"{filename}_{r['empleado'].replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # ─── Vacation cards (only liquidacion) ───
    if is_liq:
        st.markdown("<div style='font-size:0.65rem; font-weight:700; color:#9ca3af; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.5rem;'>Vacaciones en finiquito</div>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f"""<div class="metric-card">
                <div class="label">Devengadas</div>
                <div class="value" style="color:#374151;">{r['dev']:.3f}</div>
                <div style="font-size:0.65rem; color:#c4c9d2;">días</div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class="metric-card">
                <div class="label">Disfrutadas</div>
                <div class="value" style="color:#dc2626;">{r['dis']:.2f}</div>
                <div style="font-size:0.65rem; color:#c4c9d2;">días</div>
            </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(f"""<div class="metric-card metric-accent">
                <div class="label">Saldo Vacaciones</div>
                <div class="value">{r['saldo']:.2f}</div>
                <div style="font-size:0.65rem; color:rgba(255,255,255,0.6);">días a liquidar</div>
            </div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # ─── 7 Concept cards ───
    concepts = [
        ("Complem. L-V", r["compLV"], "Σ dif. plan vs reg en laborables", "h"),
        ("Plus SDF", r["plusSDF"], "min(H,7) en sáb/dom/fest", "h"),
        ("Festivo H.Comp.", r["festHComp"], "Exceso >7h en sáb/dom/fest", "h"),
        ("Horas Especiales", r["hEspeciales"], "H>11h → min(H-11, 1)", "h"),
        ("Comp. Festivo", r["compFestivo"], "Sáb/Dom, H≥4h → H-4", "h"),
        ("Comp. Nocturnos", r["compNocturno"], "1 ud/día si fin ≥ 23:00", "ud"),
        ("Plus Nocturnidad", r["plusNoct"], "Horas completas 22:00-06:00", "h"),
    ]
    st.markdown(f"<div style='font-size:0.65rem; font-weight:700; color:#9ca3af; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.5rem;'>Conceptos hora · {r['per_rev']}</div>", unsafe_allow_html=True)

    # Row 1: 4 concepts
    cols1 = st.columns(4)
    for i in range(4):
        label, val, tip, unit = concepts[i]
        active = "active" if val != 0 else "inactive"
        color = "#0d7377" if val > 0 else ("#dc2626" if val < 0 else "#d1d5db")
        with cols1[i]:
            st.markdown(f"""<div class="concept-card {active}">
                <div class="clabel">{label}</div>
                <div class="cvalue" style="color:{color};">{val:.2f}<span style="font-size:0.7rem; opacity:0.5; margin-left:2px;">{unit}</span></div>
                <div class="ctip">{tip}</div>
            </div>""", unsafe_allow_html=True)

    # Row 2: 3 concepts
    cols2 = st.columns(4)
    for i in range(3):
        label, val, tip, unit = concepts[4 + i]
        active = "active" if val != 0 else "inactive"
        color = "#0d7377" if val > 0 else ("#dc2626" if val < 0 else "#d1d5db")
        fmt_val = f"{val}" if unit == "ud" else f"{val:.2f}"
        with cols2[i]:
            st.markdown(f"""<div class="concept-card {active}">
                <div class="clabel">{label}</div>
                <div class="cvalue" style="color:{color};">{fmt_val}<span style="font-size:0.7rem; opacity:0.5; margin-left:2px;">{unit}</span></div>
                <div class="ctip">{tip}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─── Detail table ───
    if r["detalle"]:
        st.markdown(f"<div style='font-size:0.65rem; font-weight:700; color:#9ca3af; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.5rem;'>Detalle jornadas · {len(r['detalle'])} días</div>", unsafe_allow_html=True)
        df_det = pd.DataFrame(r["detalle"])
        st.dataframe(
            df_det, use_container_width=True, hide_index=True,
            column_config={
                "Fecha": st.column_config.TextColumn("Fecha", width="small"),
                "Horas": st.column_config.NumberColumn("Horas", format="%.2f"),
                "Tipo": st.column_config.TextColumn("Tipo", width="small"),
                "Dif.L-V": st.column_config.NumberColumn("Dif. L-V", format="%.2f", help="Diferencia planificado vs registrado"),
                "Plus SDF": st.column_config.NumberColumn("Plus SDF", format="%.2f"),
                "Fest.HComp": st.column_config.NumberColumn("Fest.HComp", format="%.2f"),
                "H.Esp.": st.column_config.NumberColumn("H. Especiales", format="%.2f"),
                "Comp.Fest.": st.column_config.NumberColumn("Comp. Fest.", format="%.2f"),
            },
        )
    else:
        st.info("No se encontraron jornadas en el periodo indicado.")

    # Nocturno summary
    if r["compNocturno"] > 0 or r["plusNoct"] > 0:
        st.markdown(f"""
        <div style="background:#eff6ff; border:1px solid #dbeafe; border-radius:10px; padding:0.8rem 1.2rem; margin-top:0.5rem;">
            <div style="font-size:0.65rem; font-weight:700; color:#2563eb; text-transform:uppercase; margin-bottom:0.3rem;">Resumen Nocturnidad (desde Tramos)</div>
            <div style="font-size:0.8rem; color:#1e40af;">
                <strong>{r['compNocturno']}</strong> días con comp. nocturno (fin ≥ 23:00) · 
                <strong>{r['plusNoct']:.2f}h</strong> plus nocturnidad (horas completas 22:00-06:00)
            </div>
        </div>
        """, unsafe_allow_html=True)

elif not btn_calculate:
    st.markdown("""
    <div style="display:flex; flex-direction:column; align-items:center; justify-content:center; padding:4rem 0; color:#9ca3af;">
        <div style="width:80px; height:80px; border-radius:20px; background:#e8f6f6; display:flex; align-items:center; justify-content:center; margin-bottom:1.2rem; font-size:2rem;">📋</div>
        <div style="font-size:0.9rem; font-weight:800; color:#374151;">Panel de liquidación</div>
        <div style="font-size:0.75rem; color:#9ca3af; max-width:320px; text-align:center; margin-top:0.3rem; line-height:1.6;">
            Carga los informes de Endalia y selecciona un empleado.<br>
            <strong style="color:#dc2626;">🔴 Con fecha fin</strong> = liquidación completa (horas + vacaciones)<br>
            <strong style="color:#16a34a;">🟢 Sin fecha fin</strong> = solo horas mensuales
        </div>
    </div>
    """, unsafe_allow_html=True)
