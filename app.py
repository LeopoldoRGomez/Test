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
# Descripción: Este módulo es el punto de partida de la aplicación. Se encarga de:
# 1. Definir constantes globales: Como el nombre de la base de datos (DB_NAME) y las listas de opciones para los menús desplegables (APPLICATION_OPTIONS, UNIQUE_TOOL_APPLICATION_OPTIONS).
# 2. Inicializar la base de datos (init_db): Crea todas las tablas necesarias si no existen al iniciar la aplicación. Esto asegura que la estructura de la base de datos esté siempre lista para ser utilizada.
#    - Tablas creadas: `responsibles`, `tools`, `inventory_movements`, `tool_types`, `part_number_equivalences`, `clients`.
# 3. Migración de datos: Incluye una función para añadir la columna `well` a la tabla `inventory_movements` si no existe, garantizando la compatibilidad con versiones anteriores de la base de datos.
# 4. Población inicial de datos: Si la tabla `responsibles` está vacía, la puebla con una lista inicial de nombres.
# 5. Funciones de utilidad: Contiene `get_tool_details_by_id` para obtener detalles de una herramienta y `generate_qr_code` para crear códigos QR a partir de datos.
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
    img_byte_arr.seek(0) # Important: Rewind the buffer to the beginning
    return img_byte_arr


def init_db():
    """Initializes the database and creates tables if they don't exist."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Table for responsibles
    c.execute('''
        CREATE TABLE IF NOT EXISTS responsibles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

    # Table for tool definitions
    c.execute('''
        CREATE TABLE IF NOT EXISTS tools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_number TEXT NOT NULL,
            serial_number TEXT,
            description TEXT,
            tool_type TEXT NOT NULL, -- 'Unique_Tools' or 'Miscelaneous'
            application TEXT NOT NULL, -- 'Open Hole' or 'Cemented'
            specific_type TEXT NOT NULL,
            attributes TEXT, -- JSON for extra attributes
            is_active BOOLEAN DEFAULT 1,
            UNIQUE(part_number, serial_number)
        )
    ''')

    # Table for inventory movements (the core log)
    c.execute('''
        CREATE TABLE IF NOT EXISTS inventory_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tool_id INTEGER NOT NULL,
            movement_type TEXT NOT NULL, -- 'Importation', 'Return', 'Dispatch', 'Installed'
            quantity INTEGER,
            location TEXT NOT NULL, -- 'Warehouse', 'Field', 'Installed'
            date TEXT NOT NULL,
            responsible TEXT NOT NULL,
            sales_order TEXT,
            FOREIGN KEY(tool_id) REFERENCES tools(id)
        )
    ''')

    # Table for manageable tool types
    c.execute('''
        CREATE TABLE IF NOT EXISTS tool_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            application TEXT NOT NULL,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

    # Table for part number equivalences
    c.execute('''
        CREATE TABLE IF NOT EXISTS part_number_equivalences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_pn TEXT UNIQUE NOT NULL,
            client_pn TEXT UNIQUE NOT NULL,
            client_description TEXT
        )
    ''')

    # Table for clients
    c.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

    # Table for wells
    c.execute('''
        CREATE TABLE IF NOT EXISTS wells (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            latitude TEXT,
            longitude TEXT,
            well_trajectory TEXT,
            well_fluid TEXT,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

    # --- Data Migration / Initial Population ---

    # Add 'well' column to inventory_movements if it doesn't exist (for backwards compatibility)
    c.execute("PRAGMA table_info(inventory_movements)")
    columns = [info[1] for info in c.fetchall()]
    if 'well' not in columns:
        c.execute("ALTER TABLE inventory_movements ADD COLUMN well TEXT")

    # Add 'description' column to tools if it doesn't exist
    c.execute("PRAGMA table_info(tools)")
    columns = [info[1] for info in c.fetchall()]
    if 'description' not in columns:
        c.execute("ALTER TABLE tools ADD COLUMN description TEXT")

    # Add 'attributes' column to tools if it doesn't exist
    c.execute("PRAGMA table_info(tools)")
    columns = [info[1] for info in c.fetchall()]
    if 'attributes' not in columns:
        c.execute("ALTER TABLE tools ADD COLUMN attributes TEXT")

    # Add 'client_description' column to part_number_equivalences if it doesn't exist
    c.execute("PRAGMA table_info(part_number_equivalences)")
    columns = [info[1] for info in c.fetchall()]
    if 'client_description' not in columns:
        c.execute("ALTER TABLE part_number_equivalences ADD COLUMN client_description TEXT")

    # Add 'latitude' column to wells if it doesn't exist
    c.execute("PRAGMA table_info(wells)")
    columns = [info[1] for info in c.fetchall()]
    if 'latitude' not in columns:
        c.execute("ALTER TABLE wells ADD COLUMN latitude TEXT")

    # Add 'longitude' column to wells if it doesn't exist
    c.execute("PRAGMA table_info(wells)")
    columns = [info[1] for info in c.fetchall()]
    if 'longitude' not in columns:
        c.execute("ALTER TABLE wells ADD COLUMN longitude TEXT")

    # Add 'is_active' column to wells if it doesn't exist
    c.execute("PRAGMA table_info(wells)")
    columns = [info[1] for info in c.fetchall()]
    if 'is_active' not in columns:
        c.execute("ALTER TABLE wells ADD COLUMN is_active BOOLEAN DEFAULT 1")

    # Add 'well_trajectory' column to wells if it doesn't exist
    c.execute("PRAGMA table_info(wells)")
    columns = [info[1] for info in c.fetchall()]
    if 'well_trajectory' not in columns:
        c.execute("ALTER TABLE wells ADD COLUMN well_trajectory TEXT")

    # Add 'well_fluid' column to wells if it doesn't exist
    c.execute("PRAGMA table_info(wells)")
    columns = [info[1] for info in c.fetchall()]
    if 'well_fluid' not in columns:
        c.execute("ALTER TABLE wells ADD COLUMN well_fluid TEXT")

    # Populate initial responsibles if table is empty
    c.execute("SELECT COUNT(*) FROM responsibles")
    if c.fetchone()[0] == 0:
        initial_names = ['Pablo', 'Antony', 'Warith']
        for name in initial_names:
            c.execute("INSERT INTO responsibles (name) VALUES (?)" , (name,))

    conn.commit()
    conn.close()

# ==============================================================================
# Módulo: FUNCIONES AUXILIARES DE LA BASE DE DATOS
# Descripción: Este módulo centraliza todas las funciones que interactúan directamente con la base de datos para realizar operaciones CRUD (Crear, Leer, Actualizar, Eliminar).
# Su propósito es separar la lógica de acceso a datos del resto de la aplicación, facilitando el mantenimiento.
# Funciones destacadas:
# - Gestión de Responsables: `get_responsibles`, `manage_responsible` (añadir, editar, desactivar).
# - Gestión de Tipos de Herramienta: `get_tool_types_df`, `manage_tool_type`, `get_tool_types_by_application`.
# - Gestión de Clientes: `get_clients`, `manage_client`.
# - Gestión de Equivalencias de Números de Parte: `add_part_number_equivalence`, `get_part_number_equivalences`, `delete_part_number_equivalence`.
# - Gestión de Herramientas: `get_all_tools_for_management`, `delete_tool`.
# - Operaciones de Reseteo: `reset_all_data` para borrar todos los datos de herramientas y movimientos, una función crítica y peligrosa para la administración.
# ==============================================================================

def get_responsibles(active_only=True):
    """Gets the list of responsibles."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    query = "SELECT name FROM responsibles WHERE is_active = 1 ORDER BY name" if active_only else "SELECT name FROM responsibles ORDER BY name"
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def manage_responsible(action, name, new_name=None):
    """Adds, edits, or deactivates a responsible."""
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
    """Gets a DataFrame of all tool types for display in Admin."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT id, name, application, is_active FROM tool_types ORDER BY name", conn)
    conn.close()
    return df

def manage_tool_type(action, name, application=None, new_name=None):
    """Adds, edits, or deactivates a tool type."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if action == 'add_or_edit' and name and application:
        # Use INSERT OR REPLACE to handle both adding and editing based on the unique name
        c.execute("INSERT OR REPLACE INTO tool_types (id, name, application) VALUES ((SELECT id FROM tool_types WHERE name = ?), ?, ?)", (name, name, application))
    elif action == 'deactivate' and name:
        c.execute("UPDATE tool_types SET is_active = 0 WHERE name = ?", (name,))
    elif action == 'edit_name' and name and new_name:
        c.execute("UPDATE tool_types SET name = ? WHERE name = ?", (new_name, name))
    conn.commit()
    conn.close()

def get_tool_types_by_application(application):
    """Gets active tool types for a specific application."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT name FROM tool_types WHERE application = ? AND is_active = 1 ORDER BY name", (application,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def add_part_number_equivalence(supplier_pn, client_pn, client_description):
    """Adds a new part number equivalence."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("INSERT OR REPLACE INTO part_number_equivalences (id, supplier_pn, client_pn, client_description) VALUES ((SELECT id FROM part_number_equivalences WHERE supplier_pn = ?), ?, ?, ?)", (supplier_pn, supplier_pn, client_pn, client_description))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_part_number_equivalences():
    """Gets all part number equivalences."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT supplier_pn, client_pn, client_description FROM part_number_equivalences ORDER BY supplier_pn", conn)
    conn.close()
    return df

def delete_part_number_equivalence(supplier_pn):
    """Deletes a part number equivalence by supplier_pn."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("DELETE FROM part_number_equivalences WHERE supplier_pn = ?", (supplier_pn,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_clients(active_only=True):
    """Gets the list of clients."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    query = "SELECT name FROM clients WHERE is_active = 1 ORDER BY name" if active_only else "SELECT name FROM clients ORDER BY name"
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def manage_client(action, name, new_name=None):
    """Adds, edits, or deactivates a client."""
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
    """Gets the list of wells."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    query = "SELECT name, latitude, longitude, well_trajectory, well_fluid FROM wells WHERE is_active = 1 ORDER BY name" if active_only else "SELECT name, latitude, longitude, well_trajectory, well_fluid FROM wells ORDER BY name"
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def get_all_wells_for_admin():
    """Gets a DataFrame of all wells for display in Admin."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT id, name, latitude, longitude, well_trajectory, well_fluid, is_active FROM wells ORDER BY name", conn)
    conn.close()
    return df

def get_all_wells_for_map():
    """Gets a DataFrame of all wells (active and inactive) with coordinates for map display."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT name, latitude, longitude, well_trajectory, well_fluid FROM wells WHERE latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY name", conn)
    conn.close()
    # Convert latitude and longitude to numeric, handling potential errors
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    # Drop rows where conversion failed
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    print("DataFrame for map:")
    print(df)
    print("DataFrame dtypes:")
    print(df.dtypes)
    return df

def manage_well(action, name, new_name=None, latitude=None, longitude=None, well_trajectory=None, well_fluid=None):
    """Adds, edits, or deactivates a well."""
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
    """Gets a simple list of all tools for the management section."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT id, part_number, serial_number, description FROM tools ORDER BY part_number", conn)
    conn.close()
    return df

def delete_tool(tool_id):
    """Deletes a tool and all its associated movements."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("DELETE FROM inventory_movements WHERE tool_id = ?", (tool_id,))
        c.execute("DELETE FROM tools WHERE id = ?", (tool_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_data_preview_for_reset():
    """Fetches the first 10 rows from tables that will be reset."""
    conn = sqlite3.connect(DB_NAME)
    preview_data = {}
    try:
        preview_data['tools'] = pd.read_sql_query("SELECT * FROM tools LIMIT 10", conn)
    except Exception as e:
        st.error(f"Error fetching preview for 'tools': {e}")
        preview_data['tools'] = pd.DataFrame()

    try:
        preview_data['inventory_movements'] = pd.read_sql_query("SELECT * FROM inventory_movements LIMIT 10", conn)
    except Exception as e:
        st.error(f"Error fetching preview for 'inventory_movements': {e}")
        preview_data['inventory_movements'] = pd.DataFrame()
    finally:
        conn.close()
    return preview_data


def reset_all_data():
    """Deletes all tools and inventory movements from the database."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("DELETE FROM inventory_movements")
        c.execute("DELETE FROM tools")
        c.execute("DELETE FROM sqlite_sequence WHERE name IN ('tools', 'inventory_movements')")
        conn.commit()
    except sqlite3.OperationalError:
        conn.commit() 
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ==============================================================================
# Módulo: FUNCIONES DE LÓGICA DE NEGOCIO
# Descripción: Este módulo contiene la lógica de negocio principal de la aplicación. Orquesta las operaciones de inventario
# utilizando las funciones del módulo de base de datos y las procesa para cumplir con los requisitos de la aplicación.
# Funciones principales:
# - `add_importation`: Registra la entrada de nuevas herramientas al inventario.
# - `get_tools_in_location`: Calcula y devuelve el stock disponible de herramientas en una ubicación específica (Almacén, Campo, Instalado), aplicando filtros si es necesario.
# - `dispatch_tools`: Registra la salida de herramientas del almacén hacia el campo.
# - `return_tools_batch`: Registra la devolución de herramientas desde el campo al almacén.
# - `update_field_tool_status`: Cambia el estado de una herramienta en campo (ej. de 'Campo' a 'Instalado').
# - `get_client_pn`: Obtiene el número de parte del cliente a partir de una equivalencia.
# - Generación de PDFs: `generate_delivery_note_pdf` y `generate_backload_note_pdf` para crear los documentos de despacho y devolución.
# - Búsqueda y Reportes: `get_movements_history`, `search_inventory`, `get_full_stock_report`, `get_warehouse_stock_report`, `get_installed_tools_with_details`.
# ==============================================================================

def add_importation(sales_order, responsible, date, tools_to_add):
    """Saves a new importation to the database."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        for tool_data in tools_to_add:
            # Find or create the tool definition
            # For Unique_Tools, check for existing combination of Sales Order, Part Number, and Serial Number
            if tool_data['tool_type'] == 'Unique_Tools':
                c.execute("""
                    SELECT
                        t.id
                    FROM tools t
                    JOIN inventory_movements im ON t.id = im.tool_id
                    WHERE
                        t.part_number = ? AND
                        (t.serial_number = ? OR (t.serial_number IS NULL AND ? IS NULL)) AND
                        im.sales_order = ? AND
                        im.movement_type = 'Importation'
                """, (
                    tool_data['part_number'],
                    tool_data.get('serial_number'),
                    tool_data.get('serial_number'),
                    sales_order
                ))
                if c.fetchone():
                    raise ValueError(f"Unique Tool with Part Number {tool_data['part_number']}, Serial Number {tool_data.get('serial_number', 'N/A')} and Sales Order {sales_order} already exists.")

            c.execute("SELECT id FROM tools WHERE part_number = ? AND (serial_number = ? OR (serial_number IS NULL AND ? IS NULL))", (tool_data['part_number'], tool_data.get('serial_number'), tool_data.get('serial_number')))
            tool_id_result = c.fetchone()
            
            if tool_id_result:
                tool_id = tool_id_result[0]
            else:
                # Prepare attributes for storage
                attributes_to_store = {}
                if 'seat_size' in tool_data and tool_data['seat_size']:
                    attributes_to_store['seat_size'] = tool_data['seat_size']
                
                attributes_json = json.dumps(attributes_to_store) if attributes_to_store else None

                c.execute("""
                    INSERT INTO tools (part_number, serial_number, description, tool_type, application, specific_type, attributes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    tool_data['part_number'], tool_data.get('serial_number'), tool_data.get('description', ''),
                    tool_data['tool_type'], tool_data['application'], tool_data['specific_type'], attributes_json
                ))
                tool_id = c.lastrowid
            
            # Record the inventory movement
            c.execute("""
                INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible, sales_order)
                VALUES (?, 'Importation', ?, 'Warehouse', ?, ?, ?)
            """, (tool_id, tool_data.get('quantity', 1), date, responsible, sales_order))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_tools_in_location(location, tool_category=None, tool_application=None, tool_specific_type=None, well=None):
    """Recupera herramientas y su stock en una ubicación específica, con filtros opcionales."""
    conn = sqlite3.connect(DB_NAME)
    
    # Determine the stock column to filter by
    stock_column = ''
    if location == 'Warehouse':
        stock_column = 'warehouse_stock'
    elif location == 'Field':
        stock_column = 'field_stock'
    elif location == 'Installed':
        stock_column = 'installed_stock'
    else:
        conn.close()
        return []

    query = f"""
    WITH ToolStock AS (
        SELECT
            tool_id,
            well, -- Include well in CTE
            SUM(CASE 
                WHEN movement_type IN ('Importation', 'Return') THEN quantity
                WHEN movement_type = 'Dispatch' THEN -quantity
                ELSE 0 
            END) as warehouse_stock,
            SUM(CASE 
                WHEN movement_type = 'Dispatch' THEN quantity
                WHEN movement_type = 'RevertInstallation' THEN quantity
                WHEN movement_type IN ('Return', 'Installed') THEN -quantity
                ELSE 0 
            END) as field_stock,
            SUM(CASE
                WHEN movement_type = 'Installed' THEN quantity
                WHEN movement_type = 'RevertInstallation' THEN -quantity
                ELSE 0
            END) as installed_stock
        FROM inventory_movements
        GROUP BY tool_id, well -- Group by well as well
    )
    SELECT 
        t.id, 
        t.part_number, 
        t.serial_number, 
        t.description, 
        t.specific_type, 
        t.tool_type,
        t.application,
        t.attributes,
        COALESCE(ts.warehouse_stock, 0) as warehouse_stock,
        COALESCE(ts.field_stock, 0) as field_stock,
        COALESCE(ts.installed_stock, 0) as installed_stock,
        ts.well -- Select well from CTE
    FROM tools t
    LEFT JOIN ToolStock ts ON t.id = ts.tool_id
    WHERE t.is_active = 1 AND COALESCE(ts.{stock_column}, 0) > 0
    """
    
    params = []
    if tool_category:
        query += " AND t.tool_type = ?"
        params.append(tool_category)
    if tool_application:
        query += " AND t.application = ?"
        params.append(tool_application)
    if tool_specific_type:
        query += " AND t.specific_type = ?"
        params.append(tool_specific_type)
    if well:
        query += " AND ts.well = ?"
        params.append(well)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    stock_list = []
    
    for index, row in df.iterrows():
        current_stock = row[stock_column]
        if current_stock > 0:
            serial_number_for_dict = row['serial_number']
            if pd.isna(serial_number_for_dict) or str(serial_number_for_dict).strip() == '':
                serial_number_for_dict = None

            if row['tool_type'] == 'Unique_Tools':
                display_name = f"{row['description']} / PN: {row['part_number']} / SN: {serial_number_for_dict if serial_number_for_dict else 'N/A'} / {row['specific_type']}"
                
                # Add seat_size to display_name if available
                if row['attributes']:
                    try:
                        attributes = json.loads(row['attributes'])
                        if 'seat_size' in attributes and attributes['seat_size']:
                            display_name += f" [Seat Size: {attributes['seat_size']}]"
                        if 'receptacle_size' in attributes and attributes['receptacle_size']:
                            display_name += f" [Receptacle Size: {attributes['receptacle_size']}]"
                    except json.JSONDecodeError:
                        pass # Handle malformed JSON if necessary

                quantity = 1
            else: # Miscelaneous
                display_name = f"{row['description']} / PN: {row['part_number']} / SN: N/A / {row['specific_type']}"
                
                # Add seat_size to display_name if available (though less likely for misc)
                if row['attributes']:
                    try:
                        attributes = json.loads(row['attributes'])
                        if 'seat_size' in attributes and attributes['seat_size']:
                            display_name += f" [Seat Size: {attributes['seat_size']}]"
                        if 'receptacle_size' in attributes and attributes['receptacle_size']:
                            display_name += f" [Receptacle Size: {attributes['receptacle_size']}]"
                    except json.JSONDecodeError:
                        pass # Handle malformed JSON if necessary

                display_name += f" - Stock: {int(current_stock)}"
                quantity = int(current_stock)
            
            stock_list.append({
                "id": row['id'],
                "display_name": display_name,
                "type": row['tool_type'],
                "quantity": quantity,
                "application": row['application'],
                "specific_type": row['specific_type'],
                "part_number": row['part_number'],
                "serial_number": serial_number_for_dict, # Add serial_number here
                "well": row['well'] 
            })
            
    return stock_list


def dispatch_tools(tools_to_dispatch, responsible, date, well):
    """Registra la salida de herramientas del almacén a campo."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        for tool in tools_to_dispatch:
            c.execute("""
                INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible, well)
                VALUES (?, 'Dispatch', ?, 'Field', ?, ?, ?)
            """, (
                tool['id'],
                tool['quantity_to_dispatch'],
                date,
                responsible,
                well
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def return_tools_batch(tools_to_return, responsible, date):
    """Registers the return of a batch of tools from the field."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        for tool_data in tools_to_return:
            tool_id = tool_data['id']
            quantity = int(tool_data.get('quantity', 1))
            well = tool_data.get('well') # Get well from tool_data

            c.execute("""
                INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible, well)
                VALUES (?, 'Return', ?, 'Warehouse', ?, ?, ?)
            """, (tool_id, quantity, date, responsible, well))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def update_field_tool_status(tool_id, new_status, responsible, date, quantity=1):
    """Actualiza el estado de una herramienta en campo o revierte una instalación."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    if new_status == 'Installed':
        movement_type = 'Installed'
        location = 'Installed'
    elif new_status == 'Returned':
        movement_type = 'Return'
        location = 'Warehouse'
    elif new_status == 'RevertInstallation':
        movement_type = 'RevertInstallation' # Specific type for reversal
        location = 'Field'                  # It goes back to the Field
    else:
        conn.close()
        return # Do nothing if status is unknown

    c.execute("""
        INSERT INTO inventory_movements (tool_id, movement_type, quantity, location, date, responsible)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (tool_id, movement_type, quantity, location, date, responsible))
    conn.commit()
    conn.close()

def get_client_pn(supplier_pn):
    """Retrieves the client Part Number for a given supplier Part Number."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT client_pn, client_description FROM part_number_equivalences WHERE supplier_pn = ?", (supplier_pn,))
    result = c.fetchone()
    conn.close()
    return result if result else (None, None)

def generate_delivery_note_pdf(doc_number, contract_number, client, well, responsible, dispatch_date, tools_data):
    """Generates a Delivery Note PDF with specified details and tool list."""
    pdf = FPDF('P', 'mm', 'A4')
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_page()

    # QR Code Data
    qr_content = {
        "client": client,
        "well": well,
        "tools": []
    }
    for tool in tools_data:
        full_tool_details = get_tool_details_by_id(tool['id'])
        if full_tool_details:
            qr_content["tools"].append({
                "Part Number": full_tool_details['part_number'],
                "Serial Number": full_tool_details['serial_number'] if full_tool_details['serial_number'] else 'N/A',
                "Description": full_tool_details['description'],
                "Quantity": tool['quantity_to_dispatch']
            })

    qr_image_bytes = generate_qr_code(qr_content)
    
    # Save QR code to a temporary file and embed it
    import tempfile
    import os
    temp_qr_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
    temp_qr_file.write(qr_image_bytes.getvalue())
    temp_qr_file.close() # Explicitly close the file
    temp_qr_file_path = temp_qr_file.name
    pdf.image(temp_qr_file_path, x=170, y=10, w=30)
    os.unlink(temp_qr_file_path) # Manually delete the file

    # Title
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, 'Delivery Note', 0, 1, 'C')

    # General Information
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 7, f'Client: {client}', 0, 1)
    pdf.cell(0, 7, f'Date: {dispatch_date}', 0, 1)
    pdf.cell(0, 7, f'Responsible: {responsible}', 0, 1)
    pdf.cell(0, 7, f'Document Number: {doc_number}', 0, 1)
    pdf.cell(0, 7, f'Contract Number: {contract_number}', 0, 1)
    pdf.cell(0, 7, f'Well: {well}', 0, 1)
    pdf.ln(10)

    # Table Header
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(20, 10, 'Item #', 1, 0, 'C')
    pdf.cell(130, 10, 'Tool Description', 1, 0, 'C')
    pdf.cell(40, 10, 'Quantity', 1, 1, 'C')

    # Table Rows
    pdf.set_font('Arial', '', 9)
    item_num = 1
    for tool in tools_data:
        full_tool_details = get_tool_details_by_id(tool['id'])
        
        supplier_line = f"{full_tool_details['description']} / PN: {full_tool_details['part_number']} / SN: {full_tool_details['serial_number'] if full_tool_details['serial_number'] else 'N/A'}"
        
        client_pn_data = get_client_pn(tool['part_number'])
        client_pn = client_pn_data[0] if client_pn_data else None
        client_description = client_pn_data[1] if client_pn_data else None
        
        client_line = ""
        if client_pn:
            client_line += f"Client PN: {client_pn}"
        if client_description:
            if client_line: client_line += " - "
            client_line += f"Client Desc: {client_description}"

        line_height = 5
        cell_width = 130
        
        full_description_text = supplier_line
        if client_line:
            full_description_text += "\n" + client_line

        # Calculate the height needed for the multi-line description cell
        # This is an approximation, FPDF doesn't have a direct way to get multi_cell height without drawing.
        # A common workaround is to estimate based on string width and cell width.
        # For two lines, we can simply set it to line_height * 2
        row_height = line_height * 2 # Always 2 lines for description

        # Store current X and Y
        start_x = pdf.get_x()
        start_y = pdf.get_y()

        # Draw Item #
        pdf.cell(20, row_height, str(item_num), 1, 0, 'C')

        # Draw Description (multi-line)
        # Move to the description column's X position
        pdf.set_xy(start_x + 20, start_y)
        pdf.multi_cell(cell_width, line_height, full_description_text, 1, 'L', 0)
        
        # Draw Quantity
        # Move to the quantity column's X position, and back to the start_y
        pdf.set_xy(start_x + 20 + cell_width, start_y)
        pdf.cell(40, row_height, str(tool['quantity_to_dispatch']), 1, 0, 'C')

        # Move to the next line, ensuring it's below the tallest cell
        pdf.set_y(start_y + row_height)

        item_num += 1

    pdf.ln(10)

    # Footer / Signature Section
    pdf.set_font('Arial', '', 10)

    # Calculate starting Y position for the footer to be at the bottom of the page
    footer_height = 50 # Height of the boxes (increased to accommodate all text)
    start_y_footer = pdf.h - pdf.b_margin - footer_height
    pdf.set_y(start_y_footer)

    page_width = pdf.w
    margin = 15
    box_width = (page_width - 2 * margin) / 2
    box_height = footer_height

    # Draw left box
    pdf.rect(margin, start_y_footer, box_width, box_height)

    # Draw right box
    pdf.rect(margin + box_width, start_y_footer, box_width, box_height)

    # Set position for text in the right box
    # Move to the start of the right box, with a small internal padding
    text_start_x_right_box = margin + box_width + 5 # 5mm padding from left edge of right box
    text_start_y_right_box = start_y_footer + 5 # 5mm padding from top edge of right box
    
    pdf.set_xy(text_start_x_right_box, text_start_y_right_box)
    pdf.cell(0, 7, f'For: {client}', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Received by:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Name:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Contact Number:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Signature:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Seal:', 0, 1, 'L')

    # Removed pdf.ln(2) here to prevent pushing content outside the box

    return pdf.output(dest='S').encode('latin1')


def generate_backload_note_pdf(doc_number, responsible, return_date, tools_data):
    """Generates a Backload Note PDF with specified details and tool list."""
    pdf = FPDF('P', 'mm', 'A4')
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_page()

    # QR Code Data
    qr_content = {
        "client": "N/A", # Placeholder as client is not captured in return
        "well": "N/A",   # Placeholder as well is not captured in return
        "tools": []
    }
    for tool in tools_data:
        full_tool_details = get_tool_details_by_id(tool['id'])
        if full_tool_details:
            qr_content["tools"].append({
                "Part Number": full_tool_details['part_number'],
                "Serial Number": full_tool_details['serial_number'] if full_tool_details['serial_number'] else 'N/A',
                "Description": full_tool_details['description'],
                "Quantity": tool['quantity']
            })

    qr_image_bytes = generate_qr_code(qr_content)

    # Save QR code to a temporary file and embed it
    import tempfile
    import os
    temp_qr_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
    temp_qr_file.write(qr_image_bytes.getvalue())
    temp_qr_file.close() # Explicitly close the file
    temp_qr_file_path = temp_qr_file.name
    pdf.image(temp_qr_file_path, x=170, y=10, w=30)
    os.unlink(temp_qr_file_path) # Manually delete the file

    # Title
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, 'Backload Note', 0, 1, 'C')

    # General Information
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 7, f'Document Number: {doc_number}', 0, 1)
    pdf.cell(0, 7, f'Responsible: {responsible}', 0, 1)
    pdf.cell(0, 7, f'Date: {return_date}', 0, 1)
    pdf.ln(10)

    # Table Header
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(20, 10, 'Item #', 1, 0, 'C')
    pdf.cell(130, 10, 'Tool Description', 1, 0, 'C')
    pdf.cell(40, 10, 'Quantity', 1, 1, 'C')

    # Table Rows
    pdf.set_font('Arial', '', 9)
    item_num = 1
    for tool in tools_data:
        full_tool_details = get_tool_details_by_id(tool['id'])

        supplier_line = f"{full_tool_details['description']} / PN: {full_tool_details['part_number']}"
        
        client_pn_data = get_client_pn(tool['part_number'])
        client_pn = client_pn_data[0] if client_pn_data else None
        client_description = client_pn_data[1] if client_pn_data else None
        
        client_line = ""
        if client_pn:
            client_line += f"Client PN: {client_pn}"
        if client_description:
            if client_line: client_line += " - "
            client_line += f"Client Desc: {client_description}"

        line_height = 5
        cell_width = 130

        full_description_text = supplier_line
        if client_line:
            full_description_text += "\n" + client_line

        # Calculate the height needed for the multi-line description cell
        row_height = line_height * 2 # Always 2 lines for description

        # Store current X and Y
        start_x = pdf.get_x()
        start_y = pdf.get_y()

        # Draw Item #
        pdf.cell(20, row_height, str(item_num), 1, 0, 'C')

        # Draw Description (multi-line)
        pdf.set_xy(start_x + 20, start_y)
        pdf.multi_cell(cell_width, line_height, full_description_text, 1, 'L', 0)
        
        # Draw Quantity
        pdf.set_xy(start_x + 20 + cell_width, start_y)
        pdf.cell(40, row_height, str(tool['quantity']), 1, 0, 'C')

        # Move to the next line, ensuring it's below the tallest cell
        pdf.set_y(start_y + row_height)

        item_num += 1

    pdf.ln(10)

    # Footer / Signature Section
    pdf.set_font('Arial', '', 10)

    # Calculate starting Y position for the footer to be at the bottom of the page
    footer_height = 50 # Height of the boxes (increased to accommodate all text)
    start_y_footer = pdf.h - pdf.b_margin - footer_height
    pdf.set_y(start_y_footer)

    page_width = pdf.w
    margin = 15
    box_width = (page_width - 2 * margin) / 2
    box_height = footer_height

    # Draw left box
    pdf.rect(margin, start_y_footer, box_width, box_height)

    # Draw right box
    pdf.rect(margin + box_width, start_y_footer, box_width, box_height)

    # Set position for text in the right box
    # Move to the start of the right box, with a small internal padding
    text_start_x_right_box = margin + box_width + 5 # 5mm padding from left edge of right box
    text_start_y_right_box = start_y_footer + 5 # 5mm padding from top edge of right box
    
    pdf.ln(10)

    # Footer / Signature Section
    pdf.set_font('Arial', '', 10)

    # Calculate starting Y position for the footer to be at the bottom of the page
    footer_height = 50 # Height of the boxes (increased to accommodate all text)
    start_y_footer = pdf.h - pdf.b_margin - footer_height
    pdf.set_y(start_y_footer)

    page_width = pdf.w
    margin = 15
    box_width = (page_width - 2 * margin) / 2
    box_height = footer_height

    # Draw left box
    pdf.rect(margin, start_y_footer, box_width, box_height)

    # Draw right box
    pdf.rect(margin + box_width, start_y_footer, box_width, box_height)

    # Set position for text in the right box
    # Move to the start of the right box, with a small internal padding
    text_start_x_right_box = margin + box_width + 5 # 5mm padding from left edge of right box
    text_start_y_right_box = start_y_footer + 5 # 5mm padding from top edge of right box
    
    pdf.set_xy(text_start_x_right_box, text_start_y_right_box)
    pdf.cell(0, 7, 'For: PDO (Petroleum Development Oman)', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Received by:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Name:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Contact Number:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Signature:', 0, 1, 'L')
    pdf.set_x(text_start_x_right_box) # Reset X for next line
    pdf.cell(0, 7, 'Seal:', 0, 1, 'L')

    return pdf.output(dest='S').encode('latin1')


def get_movements_history(start_date, end_date):
    """Obtiene el historial de movimientos en un rango de fechas."""
    conn = sqlite3.connect(DB_NAME)
    query = """
        SELECT im.date, im.movement_type, t.part_number, t.serial_number, im.quantity, im.location, im.responsible, im.sales_order, im.well
        FROM inventory_movements im
        JOIN tools t ON im.tool_id = t.id
        WHERE im.date BETWEEN ? AND ?
        ORDER BY im.date DESC
    """
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    conn.close()
    return df

def search_inventory(query_term=None, sales_order_filter=None, well_filter=None):
    """Searches for tools based on a query term and/or filters across multiple fields."""
    conn = sqlite3.connect(DB_NAME)
    
    base_query = """
        SELECT
            t.part_number,
            t.serial_number,
            t.description,
            t.specific_type,
            im.location,
            im.date,
            im.responsible,
            im.well,
            latest_import.sales_order
        FROM tools t
        JOIN (
            SELECT tool_id, MAX(id) as max_id
            FROM inventory_movements
            GROUP BY tool_id
        ) AS latest_im ON t.id = latest_im.tool_id
        JOIN inventory_movements im ON latest_im.max_id = im.id
        LEFT JOIN (
            SELECT tool_id, sales_order, MAX(id) as max_import_id
            FROM inventory_movements
            WHERE movement_type = 'Importation'
            GROUP BY tool_id
        ) AS latest_import ON t.id = latest_import.tool_id
    """

    conditions = []
    params = []

    if query_term:
        like_term = f"%{query_term}%"
        conditions.append("(t.part_number LIKE ? OR t.serial_number LIKE ? OR t.description LIKE ?)")
        params.extend([like_term, like_term, like_term])

    if sales_order_filter:
        conditions.append("latest_import.sales_order = ?")
        params.append(sales_order_filter)

    if well_filter:
        conditions.append("im.well = ?")
        params.append(well_filter)

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    df = pd.read_sql_query(base_query, conn, params=params)
    conn.close()
    return df

def get_full_stock_report():
    """Gets a DataFrame with the current stock of all tools in all locations."""
    conn = sqlite3.connect(DB_NAME)
    
    query = """
        WITH ToolStock AS (
            SELECT
                tool_id,
                SUM(CASE 
                    WHEN movement_type IN ('Importation', 'Return') THEN quantity
                    WHEN movement_type = 'Dispatch' THEN -quantity
                    ELSE 0 
                END) as warehouse_stock,
                SUM(CASE 
                    WHEN movement_type = 'Dispatch' THEN quantity
                    WHEN movement_type = 'RevertInstallation' THEN quantity
                    WHEN movement_type IN ('Return', 'Installed') THEN -quantity
                    ELSE 0 
                END) as field_stock,
                SUM(CASE
                    WHEN movement_type = 'Installed' THEN quantity
                    WHEN movement_type = 'RevertInstallation' THEN -quantity
                    ELSE 0
                END) as installed_stock
            FROM inventory_movements
            GROUP BY tool_id
            HAVING (COALESCE(warehouse_stock, 0) + COALESCE(field_stock, 0)) > 0
        )
        SELECT 
            t.part_number, 
            t.serial_number, 
            t.description,
            t.specific_type, 
            t.tool_type,
            COALESCE(ts.warehouse_stock, 0) as warehouse_stock,
            COALESCE(ts.field_stock, 0) as field_stock
        FROM tools t
        JOIN ToolStock ts ON t.id = ts.tool_id
        WHERE t.is_active = 1
        ORDER BY t.part_number, t.serial_number
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Rename columns for better readability in the report
    df.rename(columns={
        'part_number': 'Part Number',
        'serial_number': 'Serial Number',
        'description': 'Description',
        'specific_type': 'Tool Type',
        'tool_type': 'Category',
        'warehouse_stock': 'Stock en Almacén',
        'field_stock': 'Stock en Campo (No Instalado)'
    }, inplace=True)
    
    return df

def get_warehouse_stock_report():
    """Gets a DataFrame with the current stock of tools in the warehouse."""
    conn = sqlite3.connect(DB_NAME)
    
    query = """
        WITH ToolStock AS (
            SELECT
                tool_id,
                SUM(CASE 
                    WHEN movement_type IN ('Importation', 'Return') THEN quantity
                    WHEN movement_type = 'Dispatch' THEN -quantity
                    ELSE 0 
                END) as warehouse_stock
            FROM inventory_movements
            GROUP BY tool_id
        )
        SELECT 
            t.part_number, 
            t.serial_number, 
            t.description,
            t.specific_type, 
            t.tool_type,
            COALESCE(ts.warehouse_stock, 0) as warehouse_stock
        FROM tools t
        LEFT JOIN ToolStock ts ON t.id = ts.tool_id
        WHERE t.is_active = 1 AND COALESCE(ts.warehouse_stock, 0) > 0
        ORDER BY t.part_number, t.serial_number
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Rename columns for better readability in the report
    df.rename(columns={
        'part_number': 'Part Number',
        'serial_number': 'Serial Number',
        'description': 'Description',
        'specific_type': 'Tool Type',
        'tool_type': 'Category',
        'warehouse_stock': 'Stock in Warehouse'
    }, inplace=True)
    
    return df

def get_installed_tools_with_details():
    """Gets detailed information for all currently installed tools."""
    conn = sqlite3.connect(DB_NAME)
    query = """
        WITH ToolStock AS (
            SELECT
                tool_id,
                SUM(CASE
                    WHEN movement_type = 'Installed' THEN quantity
                    WHEN movement_type = 'RevertInstallation' THEN -quantity
                    ELSE 0
                END) as installed_stock
            FROM inventory_movements
            GROUP BY tool_id
        ),
        InstalledTools AS (
            SELECT tool_id, installed_stock
            FROM ToolStock
            WHERE installed_stock > 0
        )
        SELECT
            it.tool_id as id,
            t.part_number,
            t.serial_number,
            t.description,
            t.specific_type,
            t.tool_type,
            it.installed_stock as quantity,
            (SELECT date FROM inventory_movements im_date WHERE im_date.tool_id = it.tool_id AND im_date.movement_type = 'Installed' ORDER BY im_date.id DESC LIMIT 1) as installation_date,
            (SELECT well FROM inventory_movements im_well WHERE im_well.tool_id = it.tool_id AND im_well.movement_type = 'Dispatch' AND im_well.well IS NOT NULL ORDER BY im_well.id DESC LIMIT 1) as well
        FROM InstalledTools it
        JOIN tools t ON it.tool_id = t.id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df.to_dict('records')

def get_wells_in_field():
    """Gets a list of unique well names where tools are currently in the 'Field' location (i.e., have positive field stock)."""
    conn = sqlite3.connect(DB_NAME)
    query = """
    WITH WellFieldStock AS (
        SELECT
            well,
            SUM(CASE
                WHEN movement_type = 'Dispatch' THEN quantity
                WHEN movement_type IN ('Return', 'Installed') THEN -quantity
                ELSE 0
            END) as current_field_stock
        FROM inventory_movements
        WHERE well IS NOT NULL AND well != ''
        GROUP BY well
        HAVING current_field_stock > 0
    )
    SELECT well FROM WellFieldStock ORDER BY well
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['well'].tolist()

def get_all_sales_orders():
    """Gets a list of all unique, non-empty sales orders."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT DISTINCT sales_order FROM inventory_movements WHERE sales_order IS NOT NULL AND sales_order != '' ORDER BY sales_order")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def get_all_wells():
    """Gets a list of all unique, non-empty well names."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT DISTINCT well FROM inventory_movements WHERE well IS NOT NULL AND well != '' ORDER BY well")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

# ==============================================================================
# Módulo: INTERFAZ DE USUARIO DE STREAMLIT
# Descripción: Este es el módulo principal que construye la interfaz gráfica de la aplicación utilizando Streamlit.
# Define la navegación, las vistas y la interacción con el usuario para cada sección de la aplicación.
# Secciones:
# - Navegación Principal: Una barra lateral (`sidebar`) permite al usuario moverse entre las secciones: IN, OUT, Field Status, Reports, Query, y Administration.
# - Sección IN (Entrada): Maneja la importación de herramientas nuevas y la devolución de herramientas desde el campo. Permite la entrada de datos de forma individual o por lotes desde un archivo Excel.
# - Sección OUT (Salida): Gestiona el despacho de herramientas desde el almacén al campo. Incluye la selección de herramientas, la confirmación de la lista y la generación de la Nota de Entrega (Delivery Note).
# - Sección Field Status (Estado en Campo): Permite actualizar el estado de las herramientas que están en campo a "Instalado" y también revertir este estado.
# - Sección Reports (Reportes): Ofrece la posibilidad de generar y descargar reportes de historial de movimientos y de stock actual.
# - Sección Query (Consulta): Proporciona una barra de búsqueda para encontrar herramientas por número de parte, serie, descripción o pozo.
# - Sección Administration (Administración): Un área protegida por contraseña donde se pueden gestionar responsables, clientes, tipos de herramientas y realizar operaciones peligrosas como el borrado de datos.
# ==============================================================================

st.set_page_config(page_title="Warehouse Inventory", layout="wide")
st.title("Warehouse Inventory")

# Initialize the database
init_db()

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
main_menu = st.sidebar.radio("Select a main section:", ["IN", "OUT", "Field Status", "Reports", "Query", "Administration", "Map of Wells"], key="main_navigation_menu")


# --- IN Section ---
if main_menu == "IN":
    st.header("Inventory In")
    in_type = st.radio("Entry Type:", ["Importation", "Return"], key="in_entry_type_radio", horizontal=True)

    input_mode_options = ["Single Entry"]
    if in_type == "Importation":
        input_mode_options.append("Batch Mode")

    input_mode = st.radio("Entry Mode:", input_mode_options, key="in_entry_mode_radio", horizontal=True)
    st.markdown("---")

    if in_type == "Importation":
        st.subheader("New Importation")
        if input_mode == "Single Entry":
            col1, col2, col3 = st.columns(3)
            with col1:
                sales_order = st.text_input("Sales Order #", key="in_single_so_input")
            with col2:
                responsible = st.selectbox("Responsible", options=get_responsibles(), key="in_single_resp_select")
            with col3:
                date = st.date_input("Date", value=datetime.today(), key="in_single_date_input")
            
            st.markdown("---")

            col_form, col_list = st.columns([2, 3])

            with col_form:
                st.subheader("Add Tool")
                tool_type = st.radio("Tool Type", ["Unique_Tools", "Miscelaneous"], key="in_single_tool_type_selector", horizontal=True)

                application = "N/A"
                tool_type_options = []

                if tool_type == "Unique_Tools":
                    application = st.selectbox("Tool Application", options=[""] + UNIQUE_TOOL_APPLICATION_OPTIONS, key="in_single_app_unique", index=0)
                    tool_type_options = get_tool_types_by_application(application)
                
                if tool_type == "Unique_Tools":
                    st.markdown("**Unique Tool**")
                
                specific_type = st.selectbox("Tool Type", options=[""] + tool_type_options, key="in_single_spec_type_unique", index=0)

                seat_size = None
                # Conditionally show seat_size input for specific sleeve types
                if specific_type in ['Open Hole Multi-Entry Sleeve', 'Open Single-Entry Sleeve', 'Cemented Multi-Entry Sleeve', 'Cemented Single-Entry Sleeve', 'Open Hole Single-Entry Sleeve']:
                    seat_size = st.text_input("Seat Size", key="in_single_seat_size_unique")

                receptacle_size = None
                if specific_type == 'Landing Sub':
                    receptacle_size = st.text_input("Receptacle Size", key="in_single_receptacle_size_unique")

                with st.form("add_tool_form", clear_on_submit=True):
                    if tool_type == "Unique_Tools":
                        part_number = st.text_input("Part Number", key="in_single_pn_unique")
                        serial_number = st.text_input("Serial Number", key="in_single_sn_unique")
                        description = st.text_input("Description", key="in_single_desc_unique")
                        quantity = 1
                    else: # Miscelaneous
                        st.markdown("**Miscellaneous Tool**")
                        # Ensure application is set to Miscellaneous for this type
                        application = "Miscellaneous" 
                        specific_type = st.selectbox("Tool Type", options=[""] + get_tool_types_by_application("Miscellaneous"), key="in_single_spec_type_misc", index=0)
                        part_number = st.text_input("Part Number", key="in_single_pn_misc")
                        description = st.text_input("Description", key="in_single_desc_misc")
                        quantity = st.number_input("Quantity", min_value=1, step=1, key="in_single_qty_misc")
                        serial_number = None

                    add_tool_button = st.form_submit_button("Add Tool to List")

                    if add_tool_button:
                        if 'tools_to_add' not in st.session_state:
                            st.session_state.tools_to_add = []

                        is_valid = True
                        if not part_number:
                            st.warning("Part Number is mandatory.")
                            is_valid = False
                        if tool_type == 'Unique_Tools' and not specific_type:
                            st.warning("For Unique Tools, Tool Type is mandatory.")
                            is_valid = False
                        if tool_type == 'Miscelaneous' and not description:
                            st.warning("For Miscellaneous Tools, Description is mandatory.")
                            is_valid = False

                        if is_valid:
                            new_tool = {
                                'part_number': part_number, 'serial_number': serial_number, 'quantity': quantity,
                                'tool_type': tool_type, 'application': application, 'specific_type': specific_type,
                                'description': description, 'seat_size': seat_size, 'receptacle_size': receptacle_size
                            }
                            st.session_state.tools_to_add.append(new_tool)
                            st.rerun()

            with col_list:
                st.subheader("Tools to Import")
                if st.session_state.get('tools_to_add'):
                    display_df = pd.DataFrame(st.session_state.tools_to_add)
                    st.dataframe(display_df, use_container_width=True)
                    
                    col_final_1, col_final_2 = st.columns(2)
                    with col_final_1:
                        if st.button("✅ Save Full Importation", key="in_single_save_import_button"):
                            error_messages = []
                            if not sales_order:
                                error_messages.append("Sales Order is mandatory.")
                            if not responsible:
                                error_messages.append("Responsible is mandatory.")
                            
                            if error_messages:
                                st.error("❌ " + " ".join(error_messages))
                            else:
                                try:
                                    add_importation(sales_order, responsible, date.strftime('%Y-%m-%d'), st.session_state.tools_to_add)
                                    st.success(f"✅ Importation with Sales Order '{sales_order}' saved successfully.")
                                    st.session_state.tools_to_add = []
                                    st.rerun()
                                except ValueError as e:
                                    st.error(f"❌ Validation Error: {e}")
                                except Exception as e:
                                    st.error(f"❌ An unexpected error occurred: {e}")
                    with col_final_2:
                        if st.button("🗑️ Clear Tool List", key="in_single_clear_list_button"):
                            st.session_state.tools_to_add = []
                            st.rerun()
                else:
                    st.info("The list of tools to import will appear here.")

        elif input_mode == "Batch Mode":
            st.subheader("Batch Import from Excel")
            st.markdown("Ensure your Excel file has the following columns: `tool_type`, `part_number`, `serial_number`, `quantity`, `application`, `specific_type`, `description`.")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                sales_order_batch = st.text_input("Sales Order #", key="in_batch_so_input")
            with col2:
                responsible_batch = st.selectbox("Responsible", options=get_responsibles(), key="in_batch_resp_select")
            with col3:
                date_batch = st.date_input("Date", value=datetime.today(), key="in_batch_date_input")

            uploaded_file = st.file_uploader("Upload your Excel file", type=["xlsx"], key="in_batch_uploader")

            if uploaded_file:
                try:
                    df = pd.read_excel(uploaded_file, dtype=str).fillna('')
                    st.markdown("**Data Preview:**")
                    st.dataframe(df)

                    if st.button("Validate and Prepare Import", key="in_batch_validate_button"):
                        valid_tools = df.to_dict('records')
                        st.session_state.batch_to_add = valid_tools
                        st.success("File validated and ready for import.")
                        st.rerun()

                except Exception as e:
                    st.error(f"Error reading file: {e}")

            if 'batch_to_add' in st.session_state and st.session_state.batch_to_add:
                st.markdown("### Validated Tools to Import")
                st.dataframe(pd.DataFrame(st.session_state.batch_to_add))
                
                col_final_1, col_final_2 = st.columns(2)
                with col_final_1:
                    if st.button("✅ Save Batch Import", key="in_batch_save_import_button"):
                        if not sales_order_batch or not responsible_batch:
                            st.error("❌ Sales Order and Responsible are mandatory for batch import.")
                        else:
                            try:
                                add_importation(sales_order_batch, responsible_batch, date_batch.strftime('%Y-%m-%d'), st.session_state.batch_to_add)
                                st.success("✅ Batch Import saved successfully.")
                                st.session_state.batch_to_add = []
                                st.rerun()
                            except ValueError as e:
                                st.error(f"❌ Validation Error: {e}")
                            except Exception as e:
                                st.error(f"❌ An unexpected error occurred during batch import: {e}")
                with col_final_2:
                    if st.button("🗑️ Cancel Batch", key="in_batch_cancel_button"):
                        st.session_state.batch_to_add = []
                        st.rerun()

    elif in_type == "Return":
        st.subheader("Register Return from Field")
        if input_mode == "Single Entry":
            responsible_return = st.selectbox("Responsible", options=get_responsibles(), key="in_return_resp_select")
            date_return = st.date_input("Return Date", value=datetime.today(), key="in_return_date_input")

            # Filter 1: Tool Category (Unique Tool or Miscellaneous)
            selected_category_return = st.selectbox(
                "1) Select Tool Category",
                options=["", "Unique_Tools", "Miscelaneous"],
                key="in_return_category_filter"
            )

            effective_application_return = None
            selected_application_return = None # Initialize to None

            if selected_category_return == "Unique_Tools":
                # Display Filter 2: Tool Application for Unique Tools
                all_applications_return = UNIQUE_TOOL_APPLICATION_OPTIONS
                selected_application_return = st.selectbox(
                    "2) Select Application",
                    options=[""] + all_applications_return,
                    key="in_return_application_filter"
                )
                effective_application_return = selected_application_return
            elif selected_category_return == "Miscelaneous":
                # For Miscellaneous, application is implicitly "Miscellaneous"
                effective_application_return = "Miscellaneous"
                st.markdown("*(Application: Miscellaneous)*") # Indicate that application is fixed

            # Filter 3: Tool Type (dynamically populated based on effective_application)
            all_specific_types_return = []
            if effective_application_return: # Only fetch specific types if an application is determined
                all_specific_types_return = get_tool_types_by_application(effective_application_return)

            selected_specific_type_return = st.selectbox(
                "3) Select Specific Tool Type",
                options=[""] + all_specific_types_return,
                key="in_return_specific_type_filter"
            )

            # New: Filter by Well
            all_wells_in_field = get_wells_in_field()
            selected_well_return = st.selectbox(
                "4) Select Well (where tool was dispatched)",
                options=[""] + all_wells_in_field,
                key="in_return_well_filter"
            )

            tools_in_field = get_tools_in_location(
                'Field',
                tool_category=selected_category_return if selected_category_return else None,
                tool_application=effective_application_return if effective_application_return else None,
                tool_specific_type=selected_specific_type_return if selected_specific_type_return else None,
                well=selected_well_return if selected_well_return else None
            )

            if not tools_in_field:
                st.warning("No tools in field to return matching selected filters.")
            else:
                tool_options = {tool['display_name']: tool for tool in tools_in_field}
                selected_tool_display_name = st.selectbox("Select a tool to return", options=[""] + list(tool_options.keys()), index=0, key="in_return_tool_select")
                selected_tool_data = tool_options.get(selected_tool_display_name)
                if selected_tool_data:
                    quantity_to_return = 1
                    if selected_tool_data['type'] == 'Miscelaneous':
                        quantity_to_return = st.number_input("Quantity to return", min_value=1, max_value=selected_tool_data['quantity'], value=1, step=1, key="in_return_qty_input")
                    
                    # New button to add to preview list
                    if st.button("Add to Return List", key="in_return_add_to_list_button"):
                        if 'backload_tools_preview' not in st.session_state:
                            st.session_state.backload_tools_preview = []
                        
                        # Add relevant data to the preview list
                        st.session_state.backload_tools_preview.append({
                            'id': selected_tool_data['id'],
                            'part_number': selected_tool_data['part_number'],
                            'serial_number': selected_tool_data.get('serial_number'),
                            'description': selected_tool_data.get('description', ''),
                            'quantity': quantity_to_return,
                            'display_name': selected_tool_data['display_name'],
                            'well': selected_tool_data.get('well') # Add well here
                        })
                        st.success(f"Tool {selected_tool_display_name} added to return list.")
                        st.rerun()

            # Display preview and PDF generation fields if there are items in the preview list
            if st.session_state.get('backload_tools_preview'):
                st.markdown("---")
                st.subheader("Tools to Return (Preview)")
                preview_df = pd.DataFrame(st.session_state.backload_tools_preview)
                st.dataframe(preview_df[['display_name', 'quantity']], use_container_width=True)

                col_preview_confirm, col_preview_clear = st.columns(2)
                with col_preview_confirm:
                    if st.button("✅ Confirm Return List", key="in_return_confirm_list_button"):
                        # Store the final list for PDF generation and database update
                        st.session_state.final_backload_list = st.session_state.backload_tools_preview
                        st.session_state.backload_responsible = responsible_return
                        st.session_state.backload_date = date_return.strftime('%Y-%m-%d')
                        st.success("Return list confirmed. Proceed to Backload Note Generation.")
                        st.session_state.backload_tools_preview = [] # Clear preview list
                        st.rerun()
                with col_preview_clear:
                    if st.button("🗑️ Clear Return List", key="in_return_clear_list_button"):
                        st.session_state.backload_tools_preview = []
                        st.info("Return list cleared.")
                        st.rerun()

            # Backload Note Generation section (shown after return list is confirmed)
            if 'pdf_output_for_download_backload' in st.session_state and st.session_state.pdf_output_for_download_backload:
                st.success("✅ Backload Note generated and return registered successfully! Click below to download.")
                
                st.download_button(
                    label="Download Backload Note",
                    data=st.session_state.pdf_output_for_download_backload,
                    file_name=st.session_state.generated_backload_note_filename,
                    mime="application/pdf"
                )

                if st.button("Start New Return"):
                    # Clear all related session state to reset the form
                    keys_to_clear = [
                        'pdf_output_for_download_backload', 'generated_backload_note_filename',
                        'final_backload_list', 'backload_responsible', 'backload_date'
                    ]
                    for key in keys_to_clear:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()

            elif st.session_state.get('final_backload_list'):
                st.markdown("---")
                st.subheader("Backload Note Generation")
                st.write("**Confirmed Return Details:**")
                st.dataframe(pd.DataFrame(st.session_state.final_backload_list)[['display_name', 'quantity']], use_container_width=True)

                backload_doc_number = st.text_input("Document Number", key="in_return_backload_doc_number_final")

                if st.button("Generate and Download Backload Note", key="in_return_generate_backload_button"):
                    if backload_doc_number:
                        # Call return_tools_batch here, as the return is now finalized with BN details
                        return_tools_batch(
                            st.session_state.final_backload_list,
                            st.session_state.backload_responsible,
                            st.session_state.backload_date
                        )

                        pdf_output = generate_backload_note_pdf(
                            backload_doc_number,
                            st.session_state.backload_responsible,
                            st.session_state.backload_date,
                            st.session_state.final_backload_list
                        )
                        file_name = f"{st.session_state.backload_date}_Client_N_A_Backload_Note.pdf"

                        st.session_state.pdf_output_for_download_backload = pdf_output
                        st.session_state.generated_backload_note_filename = file_name
                        
                        st.rerun()
                    else:
                        st.warning("Please enter a Document Number for the Backload Note.")


        

# --- OUT Section ---
# --- OUT Section ---
elif main_menu == "OUT":
    st.header("Inventory Out to Field")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        date_out = st.date_input("Date", value=datetime.today(), key="out_dispatch_date_input")
    with col2:
        responsible_out = st.selectbox("Responsible", options=get_responsibles(), key="out_dispatch_resp_select")
    with col3:
        client_out = st.selectbox("Client", options=[""] + get_clients(), key="out_dispatch_client_input")
    with col4:
        well_out = st.selectbox("Well", options=[""] + get_wells(active_only=True), key="out_dispatch_well_select")

    st.markdown("---")

    # Initialize session state for the dispatch list
    if 'dispatch_list' not in st.session_state:
        st.session_state.dispatch_list = []

    # --- Logic to calculate available tools based on what's already in the dispatch list ---
    stock_list = get_tools_in_location('Warehouse')
    dispatch_misc_quantities = {}
    for item in st.session_state.dispatch_list:
        if item['type'] == 'Miscelaneous':
            dispatch_misc_quantities[item['id']] = dispatch_misc_quantities.get(item['id'], 0) + item['quantity_to_dispatch']

    available_tools = []
    for tool in stock_list:
        if tool['type'] == 'Unique_Tools':
            if not any(d['id'] == tool['id'] for d in st.session_state.dispatch_list):
                available_tools.append(tool)
        elif tool['type'] == 'Miscelaneous':
            dispatched_qty = dispatch_misc_quantities.get(tool['id'], 0)
            remaining_stock = tool['quantity'] - dispatched_qty
            if remaining_stock > 0:
                adjusted_tool = tool.copy()
                adjusted_tool['quantity'] = remaining_stock
                # Adjust display name to show remaining stock
                base_name = tool['display_name'].split(' - Stock:')[0]
                adjusted_tool['display_name'] = f"{base_name} - Stock: {remaining_stock}"
                available_tools.append(adjusted_tool)

    # --- UI for adding tools (formless) ---
    st.subheader("Add Tool to Dispatch")

    # Filter 1: Tool Category (Unique Tool or Miscellaneous)
    selected_category = st.selectbox(
        "1) Select Tool Category",
        options=["", "Unique_Tools", "Miscelaneous"],
        key="out_dispatch_category_filter"
    )

    effective_application = None
    selected_application = None # Initialize to None

    if selected_category == "Unique_Tools":
        # Display Filter 2: Tool Application for Unique Tools
        all_applications = UNIQUE_TOOL_APPLICATION_OPTIONS
        selected_application = st.selectbox(
            "2) Select Application",
            options=[""] + all_applications,
            key="out_dispatch_application_filter"
        )
        effective_application = selected_application
    elif selected_category == "Miscelaneous":
        # For Miscellaneous, application is implicitly "Miscellaneous"
        effective_application = "Miscellaneous"

    # Filter 3: Tool Type (dynamically populated based on effective_application)
    all_specific_types = []
    if effective_application: # Only fetch specific types if an application is determined
        all_specific_types = get_tool_types_by_application(effective_application)

    selected_specific_type = st.selectbox(
        "3) Select Specific Tool Type",
        options=[""] + all_specific_types,
        key="out_dispatch_specific_type_filter"
    )

    # Get available tools based on selected filters
    stock_list = get_tools_in_location(
        'Warehouse',
        tool_category=selected_category if selected_category else None,
        tool_application=effective_application if effective_application else None, # Use effective_application here
        tool_specific_type=selected_specific_type if selected_specific_type else None
    )

    dispatch_misc_quantities = {}
    for item in st.session_state.dispatch_list:
        if item['type'] == 'Miscelaneous':
            dispatch_misc_quantities[item['id']] = dispatch_misc_quantities.get(item['id'], 0) + item['quantity_to_dispatch']

    available_tools = []
    for tool in stock_list:
        if tool['type'] == 'Unique_Tools':
            if not any(d['id'] == tool['id'] for d in st.session_state.dispatch_list):
                available_tools.append(tool)
        elif tool['type'] == 'Miscelaneous':
            dispatched_qty = dispatch_misc_quantities.get(tool['id'], 0)
            remaining_stock = tool['quantity'] - dispatched_qty
            if remaining_stock > 0:
                adjusted_tool = tool.copy()
                adjusted_tool['quantity'] = remaining_stock
                # Adjust display name to show remaining stock
                base_name = tool['display_name'].split(' - Stock:')[0]
                adjusted_tool['display_name'] = f"{base_name} - Stock: {remaining_stock}"
                available_tools.append(adjusted_tool)

    if not available_tools:
        st.warning("No tools available in warehouse matching selected filters.")
    else:
        col_select, col_qty = st.columns([4, 1]) # Adjusted columns
        with col_select:
            tool_options = {tool['display_name']: tool for tool in available_tools}
            selected_tool_display_name = st.selectbox("Select a tool", options=[""] + list(tool_options.keys()), key="out_dispatch_tool_selector", index=0)
            selected_tool_data = tool_options.get(selected_tool_display_name)

        quantity_to_dispatch = 1
        if selected_tool_data:
            if selected_tool_data['type'] == 'Miscelaneous':
                with col_qty:
                    max_qty = selected_tool_data['quantity'] # This is the remaining stock
                    quantity_to_dispatch = st.number_input("Quantity", min_value=1, max_value=max_qty, value=1, step=1, key="out_dispatch_qty_input")
            
            # Button moved below the columns
            if st.button("Add to Dispatch List", key="out_dispatch_add_to_list_button"):
                # Get the base name of the tool without the stock info
                clean_display_name = selected_tool_data['display_name'].split(' - Stock:')[0]
                
                st.session_state.dispatch_list.append({
                    "id": selected_tool_data['id'],
                    "display_name": clean_display_name,
                    "quantity_to_dispatch": quantity_to_dispatch,
                    "type": selected_tool_data['type'],
                    "part_number": selected_tool_data['part_number'] # Add part_number here
                })
                st.rerun()

    # --- Display the list of tools to be dispatched ---
    if st.session_state.get('dispatch_list'):
        st.markdown("---")
        st.subheader("Tools to Dispatch")
        dispatch_df = pd.DataFrame(st.session_state.dispatch_list)
        
        # Group by tool to sum quantities of the same misc tool
        final_dispatch_df = dispatch_df.groupby(['id', 'display_name', 'type', 'part_number']).agg(
            quantity_to_dispatch=('quantity_to_dispatch', 'sum')
        ).reset_index()

        st.dataframe(final_dispatch_df[['display_name', 'quantity_to_dispatch']], use_container_width=True)

        col1_confirm, col2_confirm = st.columns(2)
        with col1_confirm:
            if st.button("✅ Confirm Dispatch", key="out_confirm_dispatch_button"):
                validation_errors = []
                if not client_out or client_out == "":
                    validation_errors.append("Client field is mandatory.")
                if not well_out:
                    validation_errors.append("Well field is mandatory.")
                
                if validation_errors:
                    for error in validation_errors:
                        st.error(error)
                else:
                    # Save the confirmed dispatch list to session state
                    st.session_state.confirmed_dispatch_list = final_dispatch_df.to_dict('records')
                    st.session_state.dispatch_well = well_out
                    st.session_state.dispatch_responsible = responsible_out
                    st.session_state.dispatch_date = date_out.strftime('%Y-%m-%d')
                    st.session_state.dispatch_client = client_out # Store client in session state
                    st.success("Dispatch confirmed. Proceed to Delivery Note Generation.")
                    st.rerun()
        with col2_confirm:
            if st.button("🗑️ Clear Dispatch List", key="out_clear_dispatch_button"):
                st.session_state.dispatch_list = []
                st.session_state.confirmed_dispatch_list = [] # Clear confirmed list too
                st.rerun()

    # --- Delivery Note Generation Section (shown after dispatch is confirmed) ---
    # This section is now stateful. It either shows the form or the download button.

    # State 1: A PDF has been generated and is ready for download.
    if 'pdf_output_for_download' in st.session_state and st.session_state.pdf_output_for_download:
        st.success("✅ Delivery Note generated and dispatch registered successfully! Click below to download.")
        
        st.download_button(
            label="Download Delivery Note",
            data=st.session_state.pdf_output_for_download,
            file_name=st.session_state.generated_delivery_note_filename,
            mime="application/pdf",
            key="out_download_delivery_note_button"
        )

        if st.button("Start New Dispatch", key="out_start_new_dispatch_button"):
            # Clear all related session state to reset the form
            keys_to_clear = [
                'pdf_output_for_download', 'generated_delivery_note_filename',
                'dispatch_list', 'confirmed_dispatch_list', 'dispatch_well',
                'dispatch_responsible', 'dispatch_date', 'dispatch_client'
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

    # State 2: No PDF is ready, show the form to generate one.
    elif st.session_state.get('confirmed_dispatch_list'):
        st.markdown("---")
        st.subheader("Delivery Note Generation")
        st.write("**Confirmed Dispatch Details:**")
        st.dataframe(pd.DataFrame(st.session_state.confirmed_dispatch_list)[['display_name', 'quantity_to_dispatch']], use_container_width=True)

        doc_number = st.text_input("Document Number", key="out_dn_doc_number")
        contract_number = st.text_input("Contract Number", key="out_dn_contract_number")

        if st.button("Generate and Download Delivery Note", key="out_generate_delivery_note_button"):
            # --- Validation ("Red" button behavior) ---
            validation_errors = []
            current_dispatch_list = st.session_state.get('confirmed_dispatch_list', [])
            current_dispatch_well = st.session_state.get('dispatch_well', '')
            current_dispatch_responsible = st.session_state.get('dispatch_responsible', '')
            current_dispatch_date = st.session_state.get('dispatch_date', '')
            current_client_out = st.session_state.get('dispatch_client', '') # Get client from session state

            if not doc_number: validation_errors.append("Document Number is mandatory.")
            if not contract_number: validation_errors.append("Contract Number is mandatory.")
            if not current_dispatch_list: validation_errors.append("No tools confirmed for dispatch.")
            if not current_dispatch_well: validation_errors.append("Well field is mandatory.")
            if not current_dispatch_responsible: validation_errors.append("Responsible is mandatory.")
            if not current_dispatch_date: validation_errors.append("Date is mandatory.")
            if not current_client_out: validation_errors.append("Client is mandatory.")

            if validation_errors:
                for error in validation_errors:
                    st.warning(f"⚠️ {error}")
            else:
                # --- Execution ("Green" button behavior) ---
                # If validation passes, proceed with DB update and PDF generation
                dispatch_tools(
                    current_dispatch_list,
                    current_dispatch_responsible,
                    current_dispatch_date,
                    current_dispatch_well
                )

                pdf_output = generate_delivery_note_pdf(
                    doc_number,
                    contract_number,
                    current_client_out, # Use client from the initial selection
                    current_dispatch_well,
                    current_dispatch_responsible,
                    current_dispatch_date,
                    current_dispatch_list
                )
                file_name = f"{current_dispatch_date}_{current_client_out}_{current_dispatch_well}_Delivery_Note.pdf"

                # Store the generated PDF in session state so the download button can appear on rerun
                st.session_state.pdf_output_for_download = pdf_output
                st.session_state.generated_delivery_note_filename = file_name
                
                st.rerun()


# --- Field Status Section ---
elif main_menu == "Field Status":
    st.header("Field Tool Status")

    # Initialize session state for the install list
    if 'install_list' not in st.session_state:
        st.session_state.install_list = []

    # --- UI for adding tools to the installation list (formless) ---
    st.subheader("Select Tools to Mark as Installed")
    
    # Filter 1: Tool Category (Unique Tool or Miscellaneous)
    selected_category_fs = st.selectbox(
        "1) Select Tool Category",
        options=["", "Unique_Tools", "Miscelaneous"],
        key="fs_category_filter"
    )

    effective_application_fs = None
    selected_application_fs = None # Initialize to None

    if selected_category_fs == "Unique_Tools":
        # Display Filter 2: Tool Application for Unique Tools
        all_applications_fs = UNIQUE_TOOL_APPLICATION_OPTIONS
        selected_application_fs = st.selectbox(
            "2) Select Application",
            options=[""] + all_applications_fs,
            key="fs_application_filter"
        )
        effective_application_fs = selected_application_fs
    elif selected_category_fs == "Miscelaneous":
        # For Miscellaneous, application is implicitly "Miscellaneous"
        effective_application_fs = "Miscellaneous"

    # Filter 3: Tool Type (dynamically populated based on effective_application)
    all_specific_types_fs = []
    if effective_application_fs: # Only fetch specific types if an application is determined
        all_specific_types_fs = get_tool_types_by_application(effective_application_fs)

    selected_specific_type_fs = st.selectbox(
        "3) Select Specific Tool Type",
        options=[""] + all_specific_types_fs,
        key="fs_specific_type_filter"
    )

    # --- Logic to calculate available tools based on what's already in the list ---
    tools_in_field = get_tools_in_location(
        'Field',
        tool_category=selected_category_fs if selected_category_fs else None,
        tool_application=effective_application_fs if effective_application_fs else None,
        tool_specific_type=selected_specific_type_fs if selected_specific_type_fs else None
    )

    install_misc_quantities = {}
    for item in st.session_state.install_list:
        if item['type'] == 'Miscelaneous':
            install_misc_quantities[item['id']] = install_misc_quantities.get(item['id'], 0) + item['quantity_to_install']

    available_tools_field = []
    for tool in tools_in_field:
        if tool['type'] == 'Unique_Tools':
            if not any(d['id'] == tool['id'] for d in st.session_state.install_list):
                available_tools_field.append(tool)
        elif tool['type'] == 'Miscelaneous':
            installed_qty = install_misc_quantities.get(tool['id'], 0)
            remaining_stock = tool['quantity'] - installed_qty
            if remaining_stock > 0:
                adjusted_tool = tool.copy()
                adjusted_tool['quantity'] = remaining_stock
                base_name = tool['display_name'].split(' - Stock:')[0]
                adjusted_tool['display_name'] = f"{base_name} - Stock: {remaining_stock}"
                available_tools_field.append(adjusted_tool)

    if not available_tools_field:
        st.warning("No more tools in field to mark as installed matching selected filters.")
    else:
        responsible_status = st.selectbox("Responsible", options=get_responsibles(), key="fs_resp_select")
        date_status = st.date_input("Installation Date", value=datetime.today(), key="fs_date_input")
        
        col_select, col_qty = st.columns([4, 1])
        with col_select:
            tool_options = {tool['display_name']: tool for tool in available_tools_field}
            selected_tool_display_name = st.selectbox("Select a tool to install", options=[""] + list(tool_options.keys()), key="fs_install_tool_selector", index=0)
            selected_tool_data = tool_options.get(selected_tool_display_name)

        quantity_to_install = 1
        if selected_tool_data:
            if selected_tool_data['type'] == 'Miscelaneous':
                with col_qty:
                    max_qty = selected_tool_data['quantity']
                    quantity_to_install = st.number_input("Quantity", min_value=1, max_value=max_qty, value=1, step=1, key="fs_install_qty_input")
            
            if st.button("Add to Installation List", key="fs_add_to_install_list_button"):
                clean_display_name = selected_tool_data['display_name'].split(' - Stock:')[0]
                st.session_state.install_list.append({
                    "id": selected_tool_data['id'],
                    "display_name": clean_display_name,
                    "quantity_to_install": quantity_to_install,
                    "responsible": responsible_status,
                    "date": date_status.strftime('%Y-%m-%d'),
                    "type": selected_tool_data['type']
                })
                st.rerun()

    # --- Section to display and confirm the installation list ---
    if st.session_state.get('install_list'):
        st.markdown("---")
        st.subheader("Tools List to Confirm Installation")
        install_df = pd.DataFrame(st.session_state.install_list)
        
        final_install_df = install_df.groupby(['id', 'display_name', 'type', 'responsible', 'date']).agg(
            quantity_to_install=('quantity_to_install', 'sum')
        ).reset_index()

        st.dataframe(final_install_df[['display_name', 'quantity_to_install', 'responsible', 'date']], use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Confirm All Installations", key="fs_confirm_installations_button"):
                for item in final_install_df.to_dict('records'):
                    update_field_tool_status(item['id'], 'Installed', item['responsible'], item['date'], item['quantity_to_install'])
                st.success("✅ All installations have been successfully registered.")
                st.session_state.install_list = []
                st.rerun()
        with col2:
            if st.button("🗑️ Clear Installation List", key="fs_clear_installation_list_button"):
                st.session_state.install_list = []
                st.rerun()

    st.markdown("---")

    # --- Section to view and revert installed tools ---
    st.subheader("Currently Installed Tools")
    installed_tools = get_installed_tools_with_details()
    if not installed_tools:
        st.info("No tools marked as installed.")
    else:
        installed_df = pd.DataFrame(installed_tools)
        installed_df['display_name'] = installed_df.apply(
            lambda row: f"{row['part_number']} / {row['serial_number']} ({row['specific_type']})" if row['tool_type'] == 'Unique_Tools' else f"{row['part_number']} ({row['specific_type']}) - Stock: {int(row['quantity'])}",
            axis=1
        )
        
        st.markdown("**Click the button to revert a tool installation.**")

        for index, row in installed_df.iterrows():
            col_info, col_date, col_well, col_action = st.columns([3, 1, 1, 1])
            with col_info:
                st.text(row['display_name'])
            with col_date:
                st.text(f"Inst: {row['installation_date']}")
            with col_well:
                st.text(f"Well: {row['well']}")
            with col_action:
                if st.button(f"Revert", key=f"fs_revert_{row['id']}"):
                    update_field_tool_status(row['id'], 'RevertInstallation', get_responsibles()[0], datetime.today().strftime('%Y-%m-%d'), row['quantity'])
                    st.success(f"The installation of {row['display_name']} has been reverted.")
                    st.rerun()

# --- Reports Section ---
elif main_menu == "Reports":
    st.header("📊 Inventory Reports")
    report_type = st.selectbox("Select report type:", ["Movement History", "Current Stock Status", "Current Warehouse Stock Status"])

    if report_type == "Movement History":
        st.subheader("Movement History")
        start_date = st.date_input("Start Date", value=datetime(2023, 1, 1), key="report_movement_start_date")
        end_date = st.date_input("End Date", value=datetime.today(), key="report_movement_end_date")
        if st.button("Generate Movement Report", key="report_movement_generate_button"):
            history_df = get_movements_history(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            st.dataframe(history_df, use_container_width=True)
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                history_df.to_excel(writer, index=False, sheet_name='MovementHistory')
            excel_data = output.getvalue()
            st.download_button(
                label="📥 Download Report in Excel",
                data=excel_data,
                file_name=f"inventory_movement_report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="report_movement_download_button"
            )
    
    elif report_type == "Current Stock Status":
        st.subheader("Current Stock Status of All Tools")
        if st.button("Generate Stock Report", key="report_current_stock_generate_button"):
            stock_df = get_full_stock_report()
            st.dataframe(stock_df, use_container_width=True)

            output_stock = io.BytesIO()
            with pd.ExcelWriter(output_stock, engine='xlsxwriter') as writer:
                stock_df.to_excel(writer, index=False, sheet_name='CurrentStock')
            excel_data_stock = output_stock.getvalue()
            st.download_button(
                label="📥 Download Stock Report in Excel",
                data=excel_data_stock,
                file_name=f"inventory_stock_report_{datetime.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="report_current_stock_download_button"
            )
    
    elif report_type == "Current Warehouse Stock Status":
        st.subheader("Current Warehouse Stock Status")
        if st.button("Generate Warehouse Stock Report", key="report_warehouse_stock_generate_button"):
            warehouse_stock_df = get_warehouse_stock_report()
            st.dataframe(warehouse_stock_df, use_container_width=True)

            output_warehouse_stock = io.BytesIO()
            with pd.ExcelWriter(output_warehouse_stock, engine='xlsxwriter') as writer:
                warehouse_stock_df.to_excel(writer, index=False, sheet_name='CurrentWarehouseStock')
            excel_data_warehouse_stock = output_warehouse_stock.getvalue()
            st.download_button(
                label="📥 Download Warehouse Stock Report in Excel",
                data=excel_data_warehouse_stock,
                file_name=f"warehouse_stock_report_{datetime.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="report_warehouse_stock_download_button"
            )

# --- Administration Section ---
elif main_menu == "Administration":
    st.header("⚙️ Administration")
    admin_menu = st.selectbox("Select an area to administer:", ["Responsibles", "Tool Types", "Tool Database Management", "Part Number Equivalences", "Clients", "Wells"])

    # Define a simple password for demonstration purposes
    ADMIN_PASSWORD = "5050"

    # Check if admin is already authenticated
    if st.session_state.get('admin_password_correct', False):
        # Admin content (already authenticated)
        if admin_menu == "Responsibles":
            # --- Responsibles Management ---
            st.subheader("Manage Responsibles")
            
            # Add Responsible
            with st.form("add_responsible_form", clear_on_submit=True):
                st.write("Add New Responsible")
                new_responsible_name = st.text_input("Responsible Name", key="admin_add_resp_name")
                if st.form_submit_button("Add Responsible"):
                    manage_responsible('add', new_responsible_name)
                    st.success(f"Responsible '{new_responsible_name}' added.")
                    st.cache_data.clear() # Clear cache to refresh selectbox options
                    st.rerun()

            st.markdown("---")
            
            # Deactivate Responsible
            st.write("Deactivate Existing Responsible")
            responsibles_to_deactivate = get_responsibles(active_only=True)
            if responsibles_to_deactivate:
                selected_responsible_deactivate = st.selectbox("Select Responsible to Deactivate", options=responsibles_to_deactivate, key="admin_deact_resp_select")
                if st.button("Deactivate Responsible"):
                    manage_responsible('deactivate', selected_responsible_deactivate)
                    st.success(f"Responsible '{selected_responsible_deactivate}' deactivated.")
                    st.cache_data.clear() # Clear cache to refresh selectbox options
                    st.rerun()
            else:
                st.info("No active responsibles to deactivate.")

            st.markdown("---")

            # Edit Responsible Name
            st.write("Edit Responsible Name")
            all_responsibles = get_responsibles(active_only=False) # Get all to allow editing inactive ones too
            if all_responsibles:
                selected_responsible_edit = st.selectbox("Select Responsible to Edit", options=all_responsibles, key="admin_edit_resp_select")
                new_responsible_name_edit = st.text_input(f"New name for '{selected_responsible_edit}'", key="admin_new_resp_name_edit")
                if st.button("Save Name Changes"):
                    if new_responsible_name_edit and new_responsible_name_edit != selected_responsible_edit:
                        manage_responsible('edit', selected_responsible_edit, new_responsible_name_edit)
                        st.success(f"Responsible name updated from '{selected_responsible_edit}' to '{new_responsible_name_edit}'.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.warning("Please enter a valid and different new name.")
            else:
                st.info("No responsibles to edit.")

        elif admin_menu == "Clients":
            # --- Clients Management ---
            st.subheader("Manage Clients")
            
            # Add Client
            with st.form("add_client_form", clear_on_submit=True):
                st.write("Add New Client")
                new_client_name = st.text_input("Client Name", key="admin_add_client_name")
                if st.form_submit_button("Add Client"):
                    manage_client('add', new_client_name)
                    st.success(f"Client '{new_client_name}' added.")
                    st.cache_data.clear() # Clear cache to refresh selectbox options
                    st.rerun()

            st.markdown("---")
            
            # Deactivate Client
            st.write("Deactivate Existing Client")
            clients_to_deactivate = get_clients(active_only=True)
            if clients_to_deactivate:
                selected_client_deactivate = st.selectbox("Select Client to Deactivate", options=clients_to_deactivate, key="admin_deact_client_select")
                if st.button("Deactivate Client"):
                    manage_client('deactivate', selected_client_deactivate)
                    st.success(f"Client '{selected_client_deactivate}' deactivated.")
                    st.cache_data.clear() # Clear cache to refresh selectbox options
                    st.rerun()
            else:
                st.info("No active clients to deactivate.")

            st.markdown("---")

            # Edit Client Name
            st.write("Edit Client Name")
            all_clients = get_clients(active_only=False) # Get all to allow editing inactive ones too
            if all_clients:
                selected_client_edit = st.selectbox("Select Client to Edit", options=all_clients, key="admin_edit_client_select")
                new_client_name_edit = st.text_input(f"New name for '{selected_client_edit}'", key="admin_new_client_name_edit")
                if st.button("Save Name Changes"):
                    if new_client_name_edit and new_client_name_edit != selected_client_edit:
                        manage_client('edit', selected_client_edit, new_client_name_edit)
                        st.success(f"Client name updated from '{selected_client_edit}' to '{new_client_name_edit}'.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.warning("Please enter a valid and different new name.")
            else:
                st.info("No clients to edit.")

        elif admin_menu == "Wells":
            # --- Wells Management ---
            st.subheader("Manage Wells")
            
            # Add Well
            with st.form("add_well_form", clear_on_submit=True):
                st.write("Add New Well")
                well_name = st.text_input("Well Name", key="admin_add_well_name")
                well_latitude = st.text_input("Latitude (optional)", key="admin_add_well_lat")
                well_longitude = st.text_input("Longitude (optional)", key="admin_add_well_lon")
                well_trajectory = st.selectbox("Well Trajectory", options=["", "Vertical", "Horizontal", "Deviated"], key="admin_add_well_trajectory")
                well_fluid = st.selectbox("Well Fluid", options=["", "Oil", "Gas"], key="admin_add_well_fluid")
                if st.form_submit_button("Add Well"):
                    if well_name:
                        manage_well('add', well_name, latitude=well_latitude, longitude=well_longitude, well_trajectory=well_trajectory, well_fluid=well_fluid)
                        st.success(f"Well '{well_name}' added.")
                        st.cache_data.clear() # Clear cache to refresh selectbox options
                        st.rerun()
                    else:
                        st.warning("Well Name is mandatory.")

            st.markdown("---")
            
            # Deactivate Well
            st.write("Deactivate Existing Well")
            wells_to_deactivate = get_wells(active_only=True)
            if wells_to_deactivate:
                selected_well_deactivate = st.selectbox("Select Well to Deactivate", options=wells_to_deactivate, key="admin_deact_well_select")
                if st.button("Deactivate Well"):
                    manage_well('deactivate', selected_well_deactivate)
                    st.success(f"Well '{selected_well_deactivate}' deactivated.")
                    st.cache_data.clear() # Clear cache to refresh selectbox options
                    st.rerun()
            else:
                st.info("No active wells to deactivate.")

            st.markdown("---")

            # Edit Well Name and Coordinates
            st.write("Edit Well Name and Coordinates")
            all_wells_admin_df = get_all_wells_for_admin()
            if not all_wells_admin_df.empty:
                # Convert DataFrame to a list of tuples for selectbox options
                well_options_for_edit = [f"{row['name']} (Lat: {row['latitude']}, Lon: {row['longitude']})" for index, row in all_wells_admin_df.iterrows()]
                selected_well_display = st.selectbox("Select Well to Edit", options=[""] + well_options_for_edit, key="admin_edit_well_select")
                
                if selected_well_display:
                    # Extract original name from display string
                    original_well_name = selected_well_display.split(' (')[0]
                    # Find the row for the selected well
                    selected_well_row = all_wells_admin_df[all_wells_admin_df['name'] == original_well_name].iloc[0]

                    new_well_name_edit = st.text_input(f"New name for '{original_well_name}'", value=selected_well_row['name'], key="admin_new_well_name_edit")
                    new_well_latitude_edit = st.text_input(f"New Latitude for '{original_well_name}'", value=selected_well_row['latitude'], key="admin_new_well_lat_edit")
                    new_well_longitude_edit = st.text_input(f"New Longitude for '{original_well_name}'", value=selected_well_row['longitude'], key="admin_new_well_lon_edit")
                    new_well_trajectory_edit = st.selectbox(f"New Trajectory for '{original_well_name}'", options=["", "Vertical", "Horizontal", "Deviated"], index=["", "Vertical", "Horizontal", "Deviated"].index(selected_well_row['well_trajectory']) if selected_well_row['well_trajectory'] else 0, key="admin_new_well_trajectory_edit")
                    new_well_fluid_edit = st.selectbox(f"New Fluid for '{original_well_name}'", options=["", "Oil", "Gas"], index=["", "Oil", "Gas"].index(selected_well_row['well_fluid']) if selected_well_row['well_fluid'] else 0, key="admin_new_well_fluid_edit")

                    if st.button("Save Well Changes"):
                        if new_well_name_edit:
                            manage_well('edit', original_well_name, new_well_name_edit, new_well_latitude_edit, new_well_longitude_edit, new_well_trajectory_edit, new_well_fluid_edit)
                            st.success(f"Well '{original_well_name}' updated.")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.warning("Well Name cannot be empty.")
            else:
                st.info("No wells to edit.")

            st.markdown("---")

            # Display all wells
            st.subheader("Registered Wells")
            st.dataframe(get_all_wells_for_admin(), use_container_width=True)

        elif admin_menu == "Tool Types":
            # --- Tool Types Management ---
            st.subheader("Manage Tool Types")

            # Add/Edit Tool Type
            with st.form("add_edit_tool_type_form", clear_on_submit=True):
                st.write("Add or Update Tool Type")
                tool_type_name = st.text_input("Tool Type Name", key="admin_tool_type_name_input")
                tool_type_application = st.selectbox("Application", options=APPLICATION_OPTIONS, key="admin_tool_type_app_select")
                if st.form_submit_button("Save Tool Type"):
                    if tool_type_name and tool_type_application:
                        manage_tool_type('add_or_edit', tool_type_name, tool_type_application)
                        st.success(f"Tool type '{tool_type_name}' ({tool_type_application}) saved.")
                        st.rerun()
                    else:
                        st.warning("Please enter a name and select an application for the tool type.")

            st.markdown("---")

            # Deactivate Tool Type
            st.write("Deactivate Existing Tool Type")
            tool_types_df = get_tool_types_df()
            active_tool_types = tool_types_df[tool_types_df['is_active'] == 1]['name'].tolist()
            if active_tool_types:
                selected_tool_type_deactivate = st.selectbox("Select Tool Type to Deactivate", options=active_tool_types, key="admin_deact_tool_type_select")
                if st.button("Deactivate Tool Type"):
                    manage_tool_type('deactivate', selected_tool_type_deactivate)
                    st.success(f"Tool type '{selected_tool_type_deactivate}' deactivated.")
                    st.rerun()
            else:
                st.info("No active tool types to deactivate.")

            st.markdown("---")

            # Display all tool types
            st.subheader("Registered Tool Types")
            st.dataframe(tool_types_df, use_container_width=True)

        elif admin_menu == "Tool Database Management":
            st.markdown("### Delete a Specific Tool")
            all_tools_df = get_all_tools_for_management()
            if all_tools_df.empty:
                st.info("No tools in the database to manage.")
            else:
                # Add a temporary 'Seleccionar' column for the checkbox
                all_tools_df['Seleccionar'] = False

                edited_df = st.data_editor(
                    all_tools_df,
                    hide_index=True, # Hide default index
                    column_config={
                        "id": st.column_config.Column("ID", width="small", disabled=True),
                        "part_number": st.column_config.Column("Part Number"),
                        "serial_number": st.column_config.Column("Serial Number"),
                        "description": st.column_config.Column("Description"),
                        "Seleccionar": st.column_config.CheckboxColumn("Select", default=False)
                    },
                    column_order=["Seleccionar", "part_number", "serial_number", "description", "id"], # Custom order
                    num_rows="dynamic",
                    key="tools_data_editor"
                )

            st.markdown("---")
            st.markdown("### Reset Tool Database")
            if st.button("Reset All Tools"):
                reset_all_data()
                st.success("All tools have been deleted from the database.")
                st.rerun()
                
                # Get selected row IDs based on the 'Seleccionar' checkbox
                selected_tool_ids = edited_df[edited_df['Seleccionar'] == True]['id'].tolist()

                # Initialize session state for selected tools to delete
                if 'tools_to_delete' not in st.session_state:
                    st.session_state.tools_to_delete = []

                # Step 1: Select tools and initiate deletion
                if st.button("Delete Selected Tools", key="initiate_delete_tools_button"):
                    if selected_tool_ids:
                        st.session_state.tools_to_delete = selected_tool_ids
                        st.rerun() # Rerun to show confirmation
                    else:
                        st.warning("Please select at least one tool to delete.")

                # Step 2: Confirmation of deletion
                if st.session_state.tools_to_delete:
                    num_to_delete = len(st.session_state.tools_to_delete)
                    st.warning(f"Are you sure you want to delete {num_to_delete} tool(s)? This action is irreversible.")
                    col_confirm_yes, col_confirm_no = st.columns(2)
                    with col_confirm_yes:
                        if st.button(f"Yes, Delete {num_to_delete} Tool(s) PERMANENTLY", key="confirm_final_delete_tools_button"):
                            try:
                                for tool_id in st.session_state.tools_to_delete:
                                    delete_tool(int(tool_id))
                                st.success(f"{num_to_delete} tool(s) deleted successfully.")
                                st.session_state.tools_to_delete = [] # Clear selection after deletion
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error deleting tool(s): {e}")
                    with col_confirm_no:
                        if st.button("Cancel Deletion", key="cancel_delete_tools_button"):
                            st.session_state.tools_to_delete = [] # Clear selection on cancel
                            st.info("Deletion canceled.")
                            st.rerun()

            st.markdown("---")
            st.subheader("Danger Zone")
            st.warning("⚠️ Caution! The following actions are irreversible and will permanently delete data.")

            with st.expander("Delete ALL Tool and Movement Data"):
                st.warning("Are you ABSOLUTELY sure you want to delete ALL data? This action is irreversible.")
                
                # Show preview of data to be deleted
                preview_data = get_data_preview_for_reset()
                if preview_data:
                    if not preview_data.get('tools', pd.DataFrame()).empty:
                        st.write("**Tools to be Deleted (first 10 rows):**")
                        st.dataframe(preview_data['tools'])
                    else:
                        st.info("The 'tools' table is already empty.")
                    
                    if not preview_data.get('inventory_movements', pd.DataFrame()).empty:
                        st.write("**Inventory Movements to be Deleted (first 10 rows):**")
                        st.dataframe(preview_data['inventory_movements'])
                    else:
                        st.info("The 'inventory_movements' table is already empty.")

                if st.button("Yes, I am sure. Delete ALL Data PERMANENTLY", key="final_reset_all_data_button"):
                    reset_all_data()
                    st.success("All tool and movement data has been deleted.")
                    # Reset password state after full reset to force re-login
                    st.session_state.admin_password_correct = False 
                    st.rerun()
        elif admin_menu == "Part Number Equivalences":
            st.subheader("Manage Part Number Equivalences")

            with st.form("add_pn_equivalence_form", clear_on_submit=True):
                st.write("Add New Part Number Equivalence")
                supplier_pn = st.text_input("Supplier Part Number", key="admin_supplier_pn")
                client_pn = st.text_input("Client Part Number", key="admin_client_pn")
                client_description = st.text_input("Client Tool Description", key="admin_client_description")
                if st.form_submit_button("Add Equivalence"):
                    if supplier_pn and client_pn:
                        try:
                            add_part_number_equivalence(supplier_pn, client_pn, client_description)
                            st.success(f"Equivalence {supplier_pn} -> {client_pn} added.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error adding equivalence: {e}")
                    else:
                        st.warning("Both Part Numbers are required.")

            st.markdown("---")

            st.write("Delete Part Number Equivalence")
            pn_equivalences_df = get_part_number_equivalences()
            if not pn_equivalences_df.empty:
                selected_supplier_pn = st.selectbox("Select Supplier Part Number to Delete", options=pn_equivalences_df['supplier_pn'].tolist(), key="admin_delete_supplier_pn")
                if st.button("Delete Equivalence"):
                    try:
                        delete_part_number_equivalence(selected_supplier_pn)
                        st.success(f"Equivalence for {selected_supplier_pn} deleted.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting equivalence: {e}")
            else:
                st.info("No part number equivalences to delete.")

            st.markdown("---")

            st.subheader("Current Part Number Equivalences")
            pn_equivalences_df = get_part_number_equivalences()
            st.dataframe(pn_equivalences_df, use_container_width=True)
    else:
        # Show password input form if not authenticated
        with st.form("admin_login_form"):
            password_input = st.text_input("Enter administrator password to access", type="password", key="admin_password_input_form")
            submit_button = st.form_submit_button("Access")

            if submit_button:
                if password_input == ADMIN_PASSWORD:
                    st.session_state.admin_password_correct = True
                    st.success("Access granted.")
                    st.rerun() # Rerun to display admin content
                else:
                    st.error("Incorrect password.")

elif main_menu == "Map of Wells":
    st.header("🗺️ Map of Wells")
    wells_for_map_df = get_all_wells_for_map()

    if not wells_for_map_df.empty:
        import folium
        from streamlit_folium import st_folium

        # Calculate center of the map
        center_lat = wells_for_map_df['latitude'].mean()
        center_lon = wells_for_map_df['longitude'].mean()

        m = folium.Map(location=[center_lat, center_lon], zoom_start=10)

        for index, row in wells_for_map_df.iterrows():
            popup_html = f"<b>Name:</b> {row['name']}<br>"
            popup_html += f"<b>Latitude:</b> {row['latitude']}<br>"
            popup_html += f"<b>Longitude:</b> {row['longitude']}<br>"
            if row['well_trajectory']:
                popup_html += f"<b>Trajectory:</b> {row['well_trajectory']}<br>"
            if row['well_fluid']:
                popup_html += f"<b>Fluid:</b> {row['well_fluid']}<br>"

            folium.Marker(
                [row['latitude'], row['longitude']],
                tooltip=folium.Tooltip(popup_html)
            ).add_to(m)
        
        st_folium(m, width="100%", height=1000)

    else:
        st.info("No wells with coordinates available to display on the map.")

# --- Query Section ---
elif main_menu == "Query":
    st.header("🔍 Advanced Inventory Query")

    # --- Filter Controls ---
    col1, col2 = st.columns(2)
    with col1:
        # Sales Order Filter
        so_options = [""] + get_all_sales_orders()
        selected_so = st.selectbox("Filter by Sales Order", options=so_options, key="query_so_filter", index=0)
    
    with col2:
        # Well Filter
        well_options = [""] + get_all_wells()
        selected_well = st.selectbox("Filter by Well", options=well_options, key="query_well_filter", index=0)

    # --- Text Search ---
    query_term = st.text_input("Search by Part Number, Serial Number, or Description:", key="query_text_input")

    # --- Search Button and Logic ---
    if st.button("Search", key="query_search_button"):
        # At least one filter or a search term must be provided
        if query_term or selected_so or selected_well:
            results_df = search_inventory(
                query_term=query_term if query_term else None,
                sales_order_filter=selected_so if selected_so else None,
                well_filter=selected_well if selected_well else None
            )
            
            if not results_df.empty:
                st.dataframe(results_df, use_container_width=True)
                
                # Provide a download button for the results
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    results_df.to_excel(writer, index=False, sheet_name='QueryResults')
                excel_data = output.getvalue()
                st.download_button(
                    label="📥 Download Results in Excel",
                    data=excel_data,
                    file_name=f"query_results_{datetime.today().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="query_download_button"
                )
            else:
                st.info("No tools found matching your criteria.")
        else:
            st.warning("Please enter a search term or select a filter.")