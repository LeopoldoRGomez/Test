import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import io
from fpdf import FPDF
import qrcode
import json

# ==============================================================================
# Módulo: CONFIGURACIÓN Y PREPARACIÓN DE LA BASE DE DATOS
# ==============================================================================

DB_NAME = 'warehouse.db'

APPLICATION_OPTIONS = ["Open Hole", "Cemented", "Intervention Tool", "Activation Ball", "Floating Equipment", "Miscellaneous"]
UNIQUE_TOOL_APPLICATION_OPTIONS = ["Open Hole", "Cemented", "Intervention Tool", "Activation Ball", "Floating Equipment"]

def get_tool_details_by_id(tool_id):
    """Retrieves full details of a tool by its ID."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT part_number, serial_number, description, tool_type, application, specific_type, attributes FROM tools WHERE id = ?", (tool_id,))
    result = c.fetchone()
    conn.close()
    if result:
        return {
            'part_number': result[0],
            'serial_number': result[1],
            'description': result[2],
            'tool_type': result[3],
            'application': result[4],
            'specific_type': result[5],
            'attributes': result[6]
        }
    return None

def generate_qr_code(data_dict):
    """Generates a QR code image from a dictionary of data."""
    qr_data = json.dumps(data_dict)
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(qr_data)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='PNG')
    img_byte_arr.seek(0)
    return img_byte_arr

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS responsibles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS tools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_number TEXT NOT NULL,
            serial_number TEXT,
            description TEXT,
            tool_type TEXT NOT NULL,
            application TEXT NOT NULL,
            specific_type TEXT NOT NULL,
            attributes TEXT,
            is_active BOOLEAN DEFAULT 1,
            UNIQUE(part_number, serial_number)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS inventory_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tool_id INTEGER NOT NULL,
            movement_type TEXT NOT NULL,
            quantity INTEGER,
            location TEXT NOT NULL,
            date TEXT NOT NULL,
            responsible TEXT NOT NULL,
            sales_order TEXT,
            well TEXT,
            FOREIGN KEY(tool_id) REFERENCES tools(id)
        )
    ''')

    c.execute('''CREATE TABLE IF NOT EXISTS tool_types (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, application TEXT NOT NULL, is_active BOOLEAN DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS part_number_equivalences (id INTEGER PRIMARY KEY AUTOINCREMENT, supplier_pn TEXT UNIQUE NOT NULL, client_pn TEXT UNIQUE NOT NULL, client_description TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS clients (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, is_active BOOLEAN DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS wells (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, latitude TEXT, longitude TEXT, well_trajectory TEXT, well_fluid TEXT, is_active BOOLEAN DEFAULT 1)''')

    # Migración de columnas (seguridad)
    c.execute("PRAGMA table_info(inventory_movements)")
    columns = [info[1] for info in c.fetchall()]
    if 'well' not in columns:
        c.execute("ALTER TABLE inventory_movements ADD COLUMN well TEXT")

    conn.commit()
    conn.close()

# ==============================================================================
# Módulo: FUNCIONES AUXILIARES DE LA BASE DE DATOS
# ==============================================================================

def get_responsibles(active_only=True):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    query = "SELECT name FROM responsibles WHERE is_active = 1 ORDER BY name" if active_only else "SELECT name FROM responsibles ORDER BY name"
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def manage_responsible(action, name, new_name=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if action == 'add' and name:
        c.execute("INSERT OR IGNORE INTO responsibles (name) VALUES (?)", (name,))
    elif action == 'edit' and name and new_name:
        c.execute("UPDATE responsibles SET name = ? WHERE name = ?", (new_name, name))
    elif action == 'deactivate' and name:
        c.execute("UPDATE responsibles SET is_active = 0 WHERE name = ?", (name,))
    conn.commit()
    conn.close()
    st.cache_data.clear()

def get_tool_types_df():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT id, name, application, is_active FROM tool_types ORDER BY name", conn)
    conn.close()
    return df

def manage_tool_type(action, name, application=None, new_name=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if action == 'add_or_edit' and name and application:
        c.execute("INSERT OR REPLACE INTO tool_types (id, name, application) VALUES ((SELECT id FROM tool_types WHERE name = ?), ?, ?)", (name, name, application))
    elif action == 'deactivate' and name:
        c.execute("UPDATE tool_types SET is_active = 0 WHERE name = ?", (name,))
    conn.commit()
    conn.close()

def get_tool_types_by_application(application):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT name FROM tool_types WHERE application = ? AND is_active = 1 ORDER BY name", (application,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def add_part_number_equivalence(supplier_pn, client_pn, client_description):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO part_number_equivalences (supplier_pn, client_pn, client_description) VALUES (?, ?, ?)", (supplier_pn, client_pn, client_description))
    conn.commit()
    conn.close()

def get_part_number_equivalences():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT supplier_pn, client_pn, client_description FROM part_number_equivalences ORDER BY supplier_pn", conn)
    conn.close()
    return df

def delete_part_number_equivalence(supplier_pn):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM part_number_equivalences WHERE supplier_pn = ?", (supplier_pn,))
    conn.commit()
    conn.close()

def get_clients(active_only=True):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    query = "SELECT name FROM clients WHERE is_active = 1 ORDER BY name" if active_only else "SELECT name FROM clients ORDER BY name"
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def manage_client(action, name, new_name=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if action == 'add' and name:
        c.execute("INSERT OR IGNORE INTO clients (name) VALUES (?)", (name,))
    elif action == 'edit' and name and new_name:
        c.execute("UPDATE clients SET name = ? WHERE name = ?", (new_name, name))
    elif action == 'deactivate' and name:
        c.execute("UPDATE clients SET is_active = 0 WHERE name = ?", (name,))
    conn.commit()
    conn.close()
    st.cache_data.clear()

def get_wells(active_only=True):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    query = "SELECT name, latitude, longitude, well_trajectory, well_fluid FROM wells WHERE is_active = 1 ORDER BY name" if active_only else "SELECT name, latitude, longitude, well_trajectory, well_fluid FROM wells ORDER BY name"
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def get_all_wells_for_admin():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT id, name, latitude, longitude, well_trajectory, well_fluid, is_active FROM wells ORDER BY name", conn)
    conn.close()
    return df

def get_all_wells_for_map():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT name, latitude, longitude, well_trajectory, well_fluid FROM wells WHERE latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY name", conn)
    conn.close()
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    return df

def manage_well(action, name, new_name=None, latitude=None, longitude=None, well_trajectory=None, well_fluid=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if action == 'add' and name:
        c.execute("INSERT OR IGNORE INTO wells (name, latitude, longitude, well_trajectory, well_fluid) VALUES (?, ?, ?, ?, ?)", (name, latitude, longitude, well_trajectory, well_fluid))
    elif action == 'edit' and name and new_name:
        c.execute("UPDATE wells SET name = ?, latitude = ?, longitude = ?, well_trajectory = ?, well_fluid = ? WHERE name = ?", (new_name, latitude, longitude, well_trajectory, well_fluid, name))
    elif action == 'deactivate' and name:
        c.execute("UPDATE wells SET is_active = 0 WHERE name = ?", (name,))
    conn.commit()
    conn.close()
    st.cache_data.clear()

def get_all_tools_for_management():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT id, part_number, serial_number, description FROM tools ORDER BY part_number", conn)
    conn.close()
    return df

def delete_tool(tool_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM inventory_movements WHERE tool_id = ?", (tool_id,))
    c.execute("DELETE FROM tools WHERE id = ?", (tool_id,))
    conn.commit()
    conn.close()

def reset_all_data():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM inventory_movements")
    c.execute("DELETE FROM tools")
    conn.commit()
    conn.close()

# ==============================================================================
# Módulo: LÓGICA DE NEGOCIO
# ==============================================================================

def add_importation(sales_order, responsible, date, tools_to_add):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        for tool_data in tools_to_add:
            c.execute("SELECT id FROM tools WHERE part_number = ? AND (serial_number = ? OR (serial_number IS NULL AND ? IS NULL))", (tool_data['part_number'], tool_data.get('serial_number'), tool_data.get('serial_number')))
            tool_id_result = c.fetchone()
            if tool_id_result:
                tool_id = tool_id_result[0]
            else:
                attributes = {}
                if tool_data.get('seat_size'): attributes['seat_size'] = tool_data['seat_size']
                if tool_data.get('receptacle_size'): attributes['receptacle_size'] = tool_data['receptacle_size']
                attr_json = json.dumps(attributes) if attributes else None

                c.execute("INSERT INTO tools (part_number, serial_number, description, tool_type, application, specific_type, attributes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (tool_data['part_number'], tool_data.get('serial_number'), tool_data.get('description', ''), tool_data['tool_type'], tool_data['application'], tool_data['specific_type'], attr_json))
                tool_id = c.lastrowid
            
            c.execute("INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible, sales_order) VALUES (?, 'Importation', ?, 'Warehouse', ?, ?, ?)",
                      (tool_id, tool_data.get('quantity', 1), date, responsible, sales_order))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_tools_in_location(location, tool_category=None, tool_application=None, tool_specific_type=None, well=None):
    conn = sqlite3.connect(DB_NAME)
    stock_column = {'Warehouse': 'warehouse_stock', 'Field': 'field_stock', 'Installed': 'installed_stock'}.get(location)
    
    query = f"""
    WITH ToolStock AS (
        SELECT tool_id, well,
            SUM(CASE WHEN movement_type IN ('Importation', 'Return') THEN quantity WHEN movement_type = 'Dispatch' THEN -quantity ELSE 0 END) as warehouse_stock,
            SUM(CASE WHEN movement_type = 'Dispatch' THEN quantity WHEN movement_type = 'RevertInstallation' THEN quantity WHEN movement_type IN ('Return', 'Installed') THEN -quantity ELSE 0 END) as field_stock,
            SUM(CASE WHEN movement_type = 'Installed' THEN quantity WHEN movement_type = 'RevertInstallation' THEN -quantity ELSE 0 END) as installed_stock
        FROM inventory_movements GROUP BY tool_id, well
    )
    SELECT t.*, ts.warehouse_stock, ts.field_stock, ts.installed_stock, ts.well
    FROM tools t LEFT JOIN ToolStock ts ON t.id = ts.tool_id WHERE COALESCE(ts.{stock_column}, 0) > 0
    """
    params = []
    if tool_category: query += " AND t.tool_type = ?"; params.append(tool_category)
    if tool_application: query += " AND t.application = ?"; params.append(tool_application)
    if tool_specific_type: query += " AND t.specific_type = ?"; params.append(tool_specific_type)
    if well: query += " AND ts.well = ?"; params.append(well)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    stock_list = []
    for _, row in df.iterrows():
        display = f"{row['description']} / PN: {row['part_number']} / SN: {row['serial_number'] or 'N/A'} / {row['specific_type']}"
        stock_list.append({
            "id": row['id'], "display_name": display, "type": row['tool_type'], "quantity": int(row[stock_column]),
            "part_number": row['part_number'], "serial_number": row['serial_number'], "well": row['well']
        })
    return stock_list

def dispatch_tools(tools, responsible, date, well):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for tool in tools:
        c.execute("INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible, well) VALUES (?, 'Dispatch', ?, 'Field', ?, ?, ?)",
                  (tool['id'], tool['quantity_to_dispatch'], date, responsible, well))
    conn.commit()
    conn.close()

def return_tools_batch(tools, responsible, date):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for tool in tools:
        c.execute("INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible, well) VALUES (?, 'Return', ?, 'Warehouse', ?, ?, ?)",
                  (tool['id'], tool['quantity'], date, responsible, tool.get('well')))
    conn.commit()
    conn.close()

def update_field_tool_status(tool_id, new_status, responsible, date, quantity=1):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    movement_type = 'Installed' if new_status == 'Installed' else 'RevertInstallation'
    location = 'Installed' if new_status == 'Installed' else 'Field'
    c.execute("INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible) VALUES (?, ?, ?, ?, ?, ?)",
              (tool_id, movement_type, quantity, location, date, responsible))
    conn.commit()
    conn.close()

def get_client_pn(supplier_pn):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT client_pn, client_description FROM part_number_equivalences WHERE supplier_pn = ?", (supplier_pn,))
    result = c.fetchone()
    conn.close()
    return result if result else (None, None)

# ==============================================================================
# Módulo: GENERACIÓN DE PDF (Delivery & Backload)
# ==============================================================================

def generate_delivery_note_pdf(doc_number, contract_number, client, well, responsible, dispatch_date, tools_data):
    pdf = FPDF('P', 'mm', 'A4')
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, 'Delivery Note', 0, 1, 'C')
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 7, f'Client: {client}', 0, 1)
    pdf.cell(0, 7, f'Well: {well}', 0, 1)
    pdf.cell(0, 7, f'Date: {dispatch_date}', 0, 1)
    pdf.cell(0, 7, f'Doc #: {doc_number}', 0, 1)
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(30, 10, 'Part Number', 1); pdf.cell(120, 10, 'Description', 1); pdf.cell(30, 10, 'Qty', 1, 1)
    pdf.set_font('Arial', '', 9)
    for tool in tools_data:
        pdf.cell(30, 10, tool['part_number'], 1)
        pdf.cell(120, 10, tool['display_name'][:65], 1)
        pdf.cell(30, 10, str(tool['quantity_to_dispatch']), 1, 1)
    return pdf.output(dest='S').encode('latin1')

def generate_backload_note_pdf(doc_number, responsible, return_date, tools_data):
    pdf = FPDF('P', 'mm', 'A4')
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, 'Backload Note', 0, 1, 'C')
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 7, f'Doc #: {doc_number}', 0, 1)
    pdf.cell(0, 7, f'Responsible: {responsible}', 0, 1)
    pdf.cell(0, 7, f'Date: {return_date}', 0, 1)
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(30, 10, 'Part Number', 1); pdf.cell(120, 10, 'Description', 1); pdf.cell(30, 10, 'Qty', 1, 1)
    pdf.set_font('Arial', '', 9)
    for tool in tools_data:
        pdf.cell(30, 10, tool['part_number'], 1)
        pdf.cell(120, 10, tool['display_name'][:65], 1)
        pdf.cell(30, 10, str(tool['quantity']), 1, 1)
    return pdf.output(dest='S').encode('latin1')

# ==============================================================================
# Módulo: REPORTES Y BÚSQUEDA
# ==============================================================================

def get_movements_history(start_date, end_date):
    conn = sqlite3.connect(DB_NAME)
    query = """
        SELECT im.date, im.movement_type, t.part_number, t.serial_number, im.quantity, im.location, im.responsible, im.sales_order, im.well
        FROM inventory_movements im JOIN tools t ON im.tool_id = t.id
        WHERE im.date BETWEEN ? AND ? ORDER BY im.date DESC
    """
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    conn.close()
    return df

def get_full_stock_report():
    conn = sqlite3.connect(DB_NAME)
    query = """
        WITH ToolStock AS (
            SELECT tool_id,
                SUM(CASE WHEN movement_type IN ('Importation', 'Return') THEN quantity WHEN movement_type = 'Dispatch' THEN -quantity ELSE 0 END) as warehouse_stock,
                SUM(CASE WHEN movement_type = 'Dispatch' THEN quantity WHEN movement_type = 'RevertInstallation' THEN quantity WHEN movement_type IN ('Return', 'Installed') THEN -quantity ELSE 0 END) as field_stock
            FROM inventory_movements GROUP BY tool_id
        )
        SELECT t.part_number, t.serial_number, t.description, ts.warehouse_stock, ts.field_stock
        FROM tools t JOIN ToolStock ts ON t.id = ts.tool_id WHERE (ts.warehouse_stock + ts.field_stock) > 0
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def search_inventory(query_term=None, sales_order_filter=None, well_filter=None):
    conn = sqlite3.connect(DB_NAME)
    query = "SELECT t.*, im.location, im.date, im.well, im.sales_order FROM tools t JOIN inventory_movements im ON t.id = im.tool_id"
    conditions = []
    params = []
    if query_term:
        conditions.append("(t.part_number LIKE ? OR t.serial_number LIKE ? OR t.description LIKE ?)")
        lt = f"%{query_term}%"; params.extend([lt, lt, lt])
    if sales_order_filter: conditions.append("im.sales_order = ?"); params.append(sales_order_filter)
    if well_filter: conditions.append("im.well = ?"); params.append(well_filter)
    if conditions: query += " WHERE " + " AND ".join(conditions)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# ==============================================================================
# Módulo: INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================================

st.set_page_config(page_title="Warehouse Inventory", layout="wide")
st.title("Warehouse Inventory")
init_db()

menu = st.sidebar.radio("Navigation", ["IN", "OUT", "Field Status", "Reports", "Query", "Administration", "Map of Wells"])

# --- SECCIÓN IN ---
if menu == "IN":
    st.header("Inventory In")
    in_type = st.radio("Entry Type", ["Importation", "Return"], horizontal=True)
    resps = get_responsibles()
    
    if not resps:
        st.warning("Please add responsibles in Administration first.")
    else:
        if in_type == "Importation":
            input_mode = st.radio("Mode", ["Single Entry", "Batch Mode"], horizontal=True)
            if input_mode == "Single Entry":
                col1, col2, col3 = st.columns(3)
                so = col1.text_input("Sales Order #")
                resp = col2.selectbox("Responsible", resps)
                date = col3.date_input("Date")
                
                with st.form("single_import"):
                    pn = st.text_input("Part Number")
                    sn = st.text_input("Serial Number")
                    desc = st.text_input("Description")
                    qty = st.number_input("Quantity", min_value=1, value=1)
                    app = st.selectbox("Application", UNIQUE_TOOL_APPLICATION_OPTIONS + ["Miscellaneous"])
                    spec = st.text_input("Specific Type")
                    if st.form_submit_button("Add to List"):
                        if 'tools_to_add' not in st.session_state: st.session_state.tools_to_add = []
                        st.session_state.tools_to_add.append({'part_number': pn, 'serial_number': sn, 'description': desc, 'quantity': qty, 'application': app, 'specific_type': spec, 'tool_type': 'Unique_Tools' if sn else 'Miscelaneous'})
                
                if st.session_state.get('tools_to_add'):
                    st.dataframe(pd.DataFrame(st.session_state.tools_to_add))
                    if st.button("Save Importation"):
                        add_importation(so, resp, date.strftime('%Y-%m-%d'), st.session_state.tools_to_add)
                        st.session_state.tools_to_add = []
                        st.success("Saved!")
                        st.rerun()
            else:
                uploaded = st.file_uploader("Upload Excel", type=["xlsx"])
                if uploaded:
                    df = pd.read_excel(uploaded).fillna('')
                    st.dataframe(df)
                    if st.button("Process Batch"):
                        # Lógica de batch...
                        st.info("Batch processed (Simulated)")

        elif in_type == "Return":
            resp = st.selectbox("Responsible", resps)
            date = st.date_input("Return Date")
            tools_in_field = get_tools_in_location('Field')
            if tools_in_field:
                sel = st.selectbox("Select Tool", [t['display_name'] for t in tools_in_field])
                qty = st.number_input("Qty", min_value=1)
                doc = st.text_input("Backload Doc #")
                if st.button("Register Return"):
                    t_data = [t for t in tools_in_field if t['display_name'] == sel][0]
                    return_tools_batch([{'id': t_data['id'], 'quantity': qty, 'well': t_data['well'], 'display_name': sel, 'part_number': t_data['part_number']}], resp, date.strftime('%Y-%m-%d'))
                    pdf = generate_backload_note_pdf(doc, resp, date.strftime('%Y-%m-%d'), [{'display_name': sel, 'quantity': qty, 'part_number': t_data['part_number']}])
                    st.download_button("Download Backload Note", pdf, "backload.pdf")

# --- SECCIÓN OUT ---
elif menu == "OUT":
    st.header("Inventory Out to Field")
    resps = get_responsibles(); cls = get_clients(); wells = get_wells()
    if not resps or not cls or not wells:
        st.error("Setup required in Administration.")
    else:
        col1, col2, col3, col4 = st.columns(4)
        date = col1.date_input("Date")
        resp = col2.selectbox("Responsible", resps)
        client = col3.selectbox("Client", cls)
        well = col4.selectbox("Well", wells)
        
        available = get_tools_in_location('Warehouse')
        if available:
            sel = st.selectbox("Select Tool", [a['display_name'] for a in available])
            qty = st.number_input("Quantity", min_value=1)
            if st.button("Add to Dispatch"):
                if 'dispatch_list' not in st.session_state: st.session_state.dispatch_list = []
                t_info = [a for a in available if a['display_name'] == sel][0]
                st.session_state.dispatch_list.append({'id': t_info['id'], 'display_name': sel, 'quantity_to_dispatch': qty, 'part_number': t_info['part_number']})
        
        if st.session_state.get('dispatch_list'):
            st.dataframe(pd.DataFrame(st.session_state.dispatch_list))
            doc = st.text_input("Delivery Note #")
            if st.button("Confirm Dispatch"):
                dispatch_tools(st.session_state.dispatch_list, resp, date.strftime('%Y-%m-%d'), well)
                pdf = generate_delivery_note_pdf(doc, "N/A", client, well, resp, date.strftime('%Y-%m-%d'), st.session_state.dispatch_list)
                st.download_button("Download PDF", pdf, "delivery_note.pdf")
                st.session_state.dispatch_list = []

# --- SECCIÓN FIELD STATUS ---
elif menu == "Field Status":
    st.header("Field Tool Status")
    field_tools = get_tools_in_location('Field')
    if field_tools:
        st.dataframe(pd.DataFrame(field_tools))
        sel = st.selectbox("Select Tool to Update", [f['display_name'] for f in field_tools])
        status = st.radio("New Status", ["Installed", "RevertInstallation"])
        resp = st.selectbox("Responsible", get_responsibles())
        if st.button("Update Status"):
            t_id = [f for f in field_tools if f['display_name'] == sel][0]['id']
            update_field_tool_status(t_id, status, resp, datetime.today().strftime('%Y-%m-%d'))
            st.success("Status Updated")
            st.rerun()

# --- SECCIÓN ADMINISTRATION ---
elif menu == "Administration":
    st.header("Administration")
    pwd = st.text_input("Admin Password", type="password")
    if pwd == "5050":
        choice = st.selectbox("Manage", ["Responsibles", "Clients", "Wells", "Equivalences", "Database"])
        if choice == "Responsibles":
            n = st.text_input("Name"); 
            if st.button("Add"): manage_responsible('add', n); st.rerun()
            st.write(get_responsibles(False))
        elif choice == "Clients":
            c = st.text_input("Client Name")
            if st.button("Add Client"): manage_client('add', c); st.rerun()
            st.write(get_clients(False))
        elif choice == "Wells":
            col1, col2, col3 = st.columns(3)
            w = col1.text_input("Well Name")
            lat = col2.text_input("Lat")
            lon = col3.text_input("Lon")
            if st.button("Add Well"): manage_well('add', w, latitude=lat, longitude=lon); st.rerun()
            st.write(get_wells(False))
        elif choice == "Database":
            if st.button("WIPE ALL DATA"): reset_all_data(); st.warning("Data deleted.")

# --- SECCIÓN MAP OF WELLS ---
elif menu == "Map of Wells":
    st.header("Well Locations")
    df_map = get_all_wells_for_map()
    if not df_map.empty:
        st.map(df_map)
    else:
        st.info("No wells with coordinates found.")

# --- SECCIÓN QUERY ---
elif menu == "Query":
    st.header("Query Inventory")
    q = st.text_input("Search (PN, SN, Desc)")
    if st.button("Search"):
        res = search_inventory(query_term=q)
        st.dataframe(res)
