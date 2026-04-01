import streamlit as st
import pandas as pd
import math
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
    """Parse time string like '23:30' or '06:15' to decimal hours. Also handles '(+1)' suffix."""
    if pd.isna(val) or val is None:
        return None, False
    s = str(val).strip().strip('"')
    if not s:
        return None, False
    next_day = "(+1)" in s
    s_clean = s.replace("(+1)", "").strip()
    import re
    m = re.match(r'(\d{1,2}):(\d{2})', s_clean)
    if m:
        h = int(m.group(1)) + int(m.group(2)) / 60
        return h, next_day
    try:
        return float(s_clean.replace(",", ".")), next_day
    except (ValueError, TypeError):
        return None, next_day


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


def get_employees_with_status(df_contratos, df_jornadas, df_absentismos):
    keywords = ["Empleado", "Nombre", "Persona", "Trabajador"]
    employees = {}

    for df in [df_contratos, df_jornadas, df_absentismos]:
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


def calc_night_hours_from_jornada(h_ini_val, h_fin_val):
    """Calculate night hours from Jornadas cols N (hora inicio) and O (hora fin).
    Returns (plus_noct_hours, is_comp_nocturno)
    """
    h_ini, _ = parse_time_to_hours(h_ini_val)
    h_fin, is_next_day = parse_time_to_hours(h_fin_val)

    if h_ini is None or h_fin is None:
        # Check if at least h_fin exists for comp nocturno
        is_comp = False
        if h_fin is not None:
            is_comp = h_fin >= 23 or is_next_day
        return 0, is_comp

    # Comp nocturno: fin >= 23:00 or next day marker
    is_comp = h_fin >= 23 or is_next_day

    # Plus nocturnidad: complete hours in 22:00-06:00
    night_h = 0.0

    if is_next_day or h_fin < h_ini:
        # Overnight shift: e.g. 14:00 to 02:00(+1) or 22:00 to 06:00
        # Part 1: from start to midnight, count 22:00-24:00
        if h_ini < 22:
            night_h += 24 - 22  # 2h from 22 to 00
        elif h_ini < 24:
            night_h += 24 - h_ini  # from start to midnight
        # Part 2: from midnight to end, count 00:00-06:00
        h_fin_effective = h_fin  # already in 0-24 range since next day
        night_h += min(h_fin_effective, 6)
    else:
        # Same day
        # Check 00:00-06:00 portion (early morning shifts)
        if h_ini < 6:
            night_h += min(h_fin, 6) - h_ini
        # Check 22:00-24:00 portion
        if h_fin > 22:
            night_h += min(h_fin, 24) - max(h_ini, 22)

    # Only complete hours
    night_h = max(0, int(night_h))
    return night_h, is_comp


def get_contract_start_for_employee(df_contratos, emp_name, corte_date, horas_hasta_date):
    """Get the most recent contract start date within the period.
    - If contract started in the calculation period → exclude jornadas before that date
    - If contract started before the period → no exclusion (return corte+1)
    - Multiple contracts → use most recent start date within the period
    """
    col_emp = find_col(df_contratos, ["Empleado", "Nombre", "Persona"])
    col_ini = find_col(df_contratos, ["Fecha inicio", "Desde", "Fecha de inicio"])

    if not col_emp or not col_ini:
        return corte_date + timedelta(days=1)

    ct_match = df_contratos[df_contratos[col_emp].apply(lambda x: normalize(x) == normalize(emp_name))]

    if ct_match.empty:
        return corte_date + timedelta(days=1)

    rev_ini = corte_date + timedelta(days=1)
    latest_start_in_period = None

    for _, row in ct_match.iterrows():
        f_ini = parse_date(row[col_ini])
        if f_ini and f_ini >= rev_ini and f_ini <= horas_hasta_date:
            if latest_start_in_period is None or f_ini > latest_start_in_period:
                latest_start_in_period = f_ini

    # If a contract started in the period, use that date
    if latest_start_in_period:
        return latest_start_in_period

    # Otherwise, no exclusion
    return rev_ini


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
            ("Complementarias L-V (h)", "compLV"),
            ("Plus SDF (h)", "plusSDF"),
            ("Festivo H.Comp. (h)", "festHComp"),
            ("Horas Especiales (h)", "hEspeciales"),
            ("Complemento Festivo (uds)", "compFestivo"),
            ("Comp. Nocturnos (uds)", "compNocturno"),
            ("Plus Nocturnidad (h)", "plusNoct"),
        ]:
            ws.write(r, 0, label, bold_fmt)
            ws.write(r, 1, result[key], num_fmt)
            r += 1
        r += 1

        if result["detalle"]:
            df_det = pd.DataFrame(result["detalle"])
            df_det.to_excel(writer, sheet_name="Detalle Jornadas", index=False)

        ws.set_column(0, 0, 26)
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
    f_absentismos = st.file_uploader("Absentismos (opcional)", type=["xlsx", "xls", "csv"], key="absentismos")

    df_contratos = read_file(f_contratos)
    df_jornadas = read_file(f_jornadas)
    df_absentismos = read_file(f_absentismos)

    st.markdown("---")

    # Employee selector
    emp_list = get_employees_with_status(df_contratos, df_jornadas, df_absentismos)
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

        # ─── Dates ───
        d_corte = datetime.combine(fecha_corte, datetime.min.time())

        # ─── Contract filter: most recent start in period ───
        rev_ini = get_contract_start_for_employee(df_contratos, selected_emp, d_corte, horas_hasta)

        # ─── JORNADAS: detect columns ───
        col_emp_j = find_col(df_jornadas, ["Empleado", "Nombre", "Persona"])
        col_fecha_j = find_col(df_jornadas, ["Día registro", "Fecha", "Día de registro"])
        col_H = find_col(df_jornadas, ["Tiempo trabajado", "Horas"])  # Col H
        col_dif = find_col(df_jornadas, ["Diferencia con horas especiales", "Diferencia"])  # Col for Comp L-V
        col_tipo_fes = find_col(df_jornadas, ["Tipo de festivo"])  # Col E
        col_hora_ini = find_col(df_jornadas, ["Hora inicio jornada", "Hora inicio"])  # Col N
        col_hora_fin = find_col(df_jornadas, ["Hora fin jornada", "Hora fin"])  # Col O

        if not col_dif:
            st.warning("⚠ No se encontró la columna 'Diferencia con horas especiales'. Comp. L-V no se calculará.")

        jornadas_emp = df_jornadas[
            df_jornadas[col_emp_j].apply(lambda x: normalize(x) == normalize(selected_emp))
        ].copy()

        # Deduplicate: same employee + same day → keep last
        if col_fecha_j:
            jornadas_emp = jornadas_emp.drop_duplicates(subset=[col_fecha_j], keep="last")

        # ─── Initialize accumulators ───
        compLV = 0.0       # 1. Complementarias L-V: H - S
        plusSDF = 0.0       # 2. Plus SDF: min(H, 7) en sáb/dom/fest
        festHComp = 0.0     # 3. Festivo H.Comp: H - 7 si H > 7
        hEspeciales = 0.0   # 4. Horas Especiales: H > 11 → H - 11 (sin cap)
        compFestivo = 0     # 5. Comp. Festivo: 1 ud/día sáb/dom si H ≥ 4
        compNocturno = 0    # 6. Comp. Nocturnos: 1 ud/día si fin ≥ 23:00 o (+1)
        plusNoct = 0.0       # 7. Plus Nocturnidad: horas completas 22-06
        nocturno_days = set()
        detalle = []

        # ─── Process JORNADAS ───
        for _, row in jornadas_emp.iterrows():
            f = parse_date(row[col_fecha_j])
            if not f or f < rev_ini or f > horas_hasta:
                continue

            H = parse_float(row[col_H])

            if H <= 0:
                continue

            # ── Detect festivo/SDF ──
            es_sabdom = f.weekday() in (5, 6)
            es_sdf = es_sabdom  # Sáb/dom always SDF

            if not es_sdf and col_tipo_fes:
                tf = str(row.get(col_tipo_fes, "")).strip()
                es_sdf = tf != "" and tf.lower() != "nan" and tf.lower() != "fin de semana"
                if "fin de semana" in tf.lower():
                    es_sdf = True
                    es_sabdom = True

            # ── Per-day values ──
            dia_compLV = 0.0
            dia_plusSDF = 0.0
            dia_festHComp = 0.0
            dia_hEsp = 0.0
            dia_compFest = 0
            dia_compNoct = 0
            dia_plusNoct = 0

            if es_sdf:
                # 2. Plus SDF: first 7h
                dia_plusSDF = min(H, 7)
                plusSDF += dia_plusSDF

                # 3. Festivo H.Comp: excess over 7h
                if H > 7:
                    dia_festHComp = H - 7
                    festHComp += dia_festHComp

                # 5. Comp. Festivo: only sáb/dom, H ≥ 4h → 1 unit
                if es_sabdom and H >= 4:
                    dia_compFest = 1
                    compFestivo += 1
            else:
                # 1. Complementarias L-V: col Diferencia (solo laborables, nunca sáb/dom)
                if col_dif:
                    dia_compLV = parse_float(row[col_dif])
                    compLV += dia_compLV

            # 4. Horas Especiales: any day, H > 11h → H - 11 (no cap)
            if H > 11:
                dia_hEsp = H - 11
                hEspeciales += dia_hEsp

            # 6 & 7. Nocturnidad: from Jornadas cols N (inicio) and O (fin)
            if col_hora_fin:
                h_fin_raw = row.get(col_hora_fin)
                h_ini_raw = row.get(col_hora_ini) if col_hora_ini else None

                night_h, is_comp = calc_night_hours_from_jornada(h_ini_raw, h_fin_raw)

                dia_plusNoct = night_h
                plusNoct += night_h

                if is_comp:
                    day_key = f.strftime("%Y-%m-%d")
                    if day_key not in nocturno_days:
                        nocturno_days.add(day_key)
                        dia_compNoct = 1
                        compNocturno += 1

            detalle.append({
                "Fecha": f.strftime("%d/%m/%Y"),
                "Horas (H)": round(H, 2),
                "Tipo": "Festivo" if es_sdf else "Laborable",
                "Comp.L-V": round(dia_compLV, 2) if not es_sdf else None,
                "Plus SDF": round(dia_plusSDF, 2) if es_sdf else None,
                "Fest.HComp": round(dia_festHComp, 2) if dia_festHComp > 0 else None,
                "H.Esp.": round(dia_hEsp, 2) if dia_hEsp > 0 else None,
                "Comp.Fest": dia_compFest if dia_compFest > 0 else None,
                "Comp.Noct": dia_compNoct if dia_compNoct > 0 else None,
                "Plus Noct": dia_plusNoct if dia_plusNoct > 0 else None,
            })

        detalle.sort(key=lambda x: x["Fecha"])

        # ─── Round totals ───
        compLV = round(compLV, 2)
        plusSDF = round(plusSDF, 2)
        festHComp = round(festHComp, 2)
        hEspeciales = round(hEspeciales, 2)
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
            "col_dif_found": col_dif is not None,
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

    if is_liq:
        st.markdown(f'<div class="mode-banner mode-liq">🔴 LIQUIDACIÓN — Fin de contrato: {r["f_fin"]}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="mode-banner mode-mens">🟢 CÁLCULO MENSUAL — Empleado activo</div>', unsafe_allow_html=True)

    if not r.get("col_dif_found"):
        st.warning("⚠ Columna 'Diferencia con horas especiales' no encontrada. Comp. L-V será 0. Revisa el informe de Jornadas.")

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
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.document",
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
        ("Complem. L-V", r["compLV"], "Σ col Diferencia en laborables (neto)", "h"),
        ("Plus SDF", r["plusSDF"], "min(H,7) en sáb/dom/fest", "h"),
        ("Festivo H.Comp.", r["festHComp"], "H − 7 si H>7 en festivos", "h"),
        ("Horas Especiales", r["hEspeciales"], "H > 11h → H − 11", "h"),
        ("Comp. Festivo", r["compFestivo"], "Sáb/Dom, 1 ud si H≥4h", "ud"),
        ("Comp. Nocturnos", r["compNocturno"], "1 ud/día si fin ≥23:00 o (+1)", "ud"),
        ("Plus Nocturnidad", r["plusNoct"], "Horas completas 22:00-06:00", "h"),
    ]
    st.markdown(f"<div style='font-size:0.65rem; font-weight:700; color:#9ca3af; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.5rem;'>Conceptos hora · {r['per_rev']}</div>", unsafe_allow_html=True)

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
                "Horas (H)": st.column_config.NumberColumn("H", format="%.2f"),
                "Tipo": st.column_config.TextColumn("Tipo", width="small"),
                "Comp.L-V": st.column_config.NumberColumn("Comp.L-V", format="%.2f", help="Col Diferencia en laborables"),
                "Plus SDF": st.column_config.NumberColumn("Plus SDF", format="%.2f"),
                "Fest.HComp": st.column_config.NumberColumn("Fest.HComp", format="%.2f"),
                "H.Esp.": st.column_config.NumberColumn("H.Esp.", format="%.2f"),
                "Comp.Fest": st.column_config.NumberColumn("C.Fest", help="1 ud sáb/dom H≥4"),
                "Comp.Noct": st.column_config.NumberColumn("C.Noct", help="1 ud fin≥23:00"),
                "Plus Noct": st.column_config.NumberColumn("P.Noct", format="%.0f", help="Horas 22-06"),
            },
        )
    else:
        st.info("No se encontraron jornadas en el periodo indicado.")

    # Nocturno summary
    if r["compNocturno"] > 0 or r["plusNoct"] > 0:
        st.markdown(f"""
        <div style="background:#eff6ff; border:1px solid #dbeafe; border-radius:10px; padding:0.8rem 1.2rem; margin-top:0.5rem;">
            <div style="font-size:0.65rem; font-weight:700; color:#2563eb; text-transform:uppercase; margin-bottom:0.3rem;">Nocturnidad (cols N y O del informe)</div>
            <div style="font-size:0.8rem; color:#1e40af;">
                <strong>{r['compNocturno']}</strong> días con comp. nocturno (fin ≥23:00 o (+1)) · 
                <strong>{r['plusNoct']:.0f}h</strong> plus nocturnidad (horas completas 22-06)
            </div>
        </div>
        """, unsafe_allow_html=True)

elif not btn_calculate:
    st.markdown("""
    <div style="display:flex; flex-direction:column; align-items:center; justify-content:center; padding:4rem 0; color:#9ca3af;">
        <div style="width:80px; height:80px; border-radius:20px; background:#e8f6f6; display:flex; align-items:center; justify-content:center; margin-bottom:1.2rem; font-size:2rem;">📋</div>
        <div style="font-size:0.9rem; font-weight:800; color:#374151;">Panel de liquidación</div>
        <div style="font-size:0.75rem; color:#9ca3af; max-width:320px; text-align:center; margin-top:0.3rem; line-height:1.6;">
            Carga Contratos + Jornadas y selecciona empleado.<br>
            <strong style="color:#dc2626;">🔴 Con fecha fin</strong> = liquidación completa (horas + vacaciones)<br>
            <strong style="color:#16a34a;">🟢 Sin fecha fin</strong> = solo horas mensuales
        </div>
    </div>
    """, unsafe_allow_html=True)
