import os
import datetime
import sqlite3
import pandas as pd

class CryptoDatabase:
    """
    Clase para gestionar la base de datos de CryptoRuanas S.A.S.
    Maneja las tablas 'Blockchain' y 'MemeCoin'.
    """
    
    def __init__(self, db_name="crypto_ruanas.sqlite"):
        """
        Inicializa la base de datos y crea las tablas si no existen.
        """
        self.db_name = db_name
        print(f"Iniciando conexión con: {self.db_name}")
        self.setup_database()

    def setup_database(self):
        """
        Crea las tablas 'Blockchain' y 'MemeCoin' si no existen,
        incluyendo la relación de clave foránea.
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # Habilitar soporte para claves foráneas en SQLite
            cursor.execute("PRAGMA foreign_keys = ON;")

            # --- Crear la tabla 'Blockchain' ---
            # Almacena los ecosistemas (ej. Solana, Ethereum)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Blockchain (
                blockchain_id VARCHAR(50) PRIMARY KEY,
                nombre_blockchain VARCHAR(100) NOT NULL,
                simbolo_nativo VARCHAR(10)
            );
            """)

            # --- Crear la tabla 'MemeCoin' ---
            # Almacena cada moneda y la vincula a su blockchain
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS MemeCoin (
                coin_id VARCHAR(100) PRIMARY KEY,
                nombre VARCHAR(150) NOT NULL,
                simbolo VARCHAR(20) NOT NULL,
                precio_usd DECIMAL(20, 10),
                market_cap_usd NUMERIC(25, 2),
                volumen_24h NUMERIC(25, 2),
                ultima_actualizacion TIMESTAMP,
                
                -- Clave foránea que referencia a la tabla Blockchain
                blockchain_origen_id VARCHAR(50),
                CONSTRAINT fk_blockchain
                    FOREIGN KEY(blockchain_origen_id) 
                    REFERENCES Blockchain(blockchain_id)
                    ON DELETE SET NULL 
            );
            """)
            
            conn.commit()
            print("Tablas 'Blockchain' y 'MemeCoin' aseguradas correctamente.")
            
        except Exception as errores:
            print(f"Error al configurar la base de datos: {errores}")
        finally:
            if conn:
                conn.close()

    def upsert_memecoins_from_df(self, df):
        """
        Inserta o actualiza (UPSERT) datos de un DataFrame.
        El DataFrame debe tener columnas como:
        'coin_id', 'nombre', 'simbolo', 'precio_usd', 'market_cap_usd',
        'volumen_24h', 'blockchain_id', 'blockchain_nombre'
        """
        conn = None
        try:
            # Asegurarse de que el DF no tenga valores nulos en columnas clave
            df = df.dropna(subset=['coin_id', 'blockchain_id'])
            
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            # Habilitar soporte para claves foráneas
            cursor.execute("PRAGMA foreign_keys = ON;")

            for _, row in df.iterrows():
                
                # --- 1. UPSERT en la tabla 'Blockchain' ---
                # Intenta insertar la blockchain. Si ya existe (por PK), no hace nada.
                sql_blockchain = """
                INSERT INTO Blockchain (blockchain_id, nombre_blockchain)
                VALUES (?, ?)
                ON CONFLICT(blockchain_id) DO NOTHING;
                """
                cursor.execute(sql_blockchain, (row['blockchain_id'], row['blockchain_nombre']))

                # --- 2. UPSERT en la tabla 'MemeCoin' ---
                # Intenta insertar la moneda. Si ya existe (por PK 'coin_id'),
                # actualiza sus métricas (precio, market_cap, etc.).
                sql_memecoin = """
                INSERT INTO MemeCoin (
                    coin_id, nombre, simbolo, precio_usd, market_cap_usd, 
                    volumen_24h, blockchain_origen_id, ultima_actualizacion
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(coin_id) DO UPDATE SET
                    precio_usd = EXCLUDED.precio_usd,
                    market_cap_usd = EXCLUDED.market_cap_usd,
                    volumen_24h = EXCLUDED.volumen_24h,
                    blockchain_origen_id = EXCLUDED.blockchain_origen_id,
                    ultima_actualizacion = EXCLUDED.ultima_actualizacion;
                """
                
                # Preparar datos de la fila para la consulta
                datos_moneda = (
                    row['coin_id'],
                    row['nombre'],
                    row['simbolo'],
                    row.get('precio_usd', None),
                    row.get('market_cap_usd', None),
                    row.get('volumen_24h', None),
                    row['blockchain_id'],
                    datetime.datetime.now()
                )
                
                cursor.execute(sql_memecoin, datos_moneda)

            conn.commit()
            print(f"Procesados {len(df)} registros (UPSERT).")

        except Exception as errores:
            print(f"Error en 'upsert_memecoins_from_df': {errores}")
            if conn:
                conn.rollback() # Revertir cambios si hay un error
        finally:
            if conn:
                conn.close()

    # --- Métodos Genéricos (Similares a tu código original) ---
    
    def read_data(self, nom_table=""):
        """
        Lee todos los datos de una tabla específica y los devuelve como un DataFrame.
        (Idéntico a tu método).
        """
        df = pd.DataFrame()
        conn = None
        try:
            if len(nom_table) > 0:
                conn = sqlite3.connect(self.db_name)
                query = f"SELECT * FROM {nom_table}"
                df = pd.read_sql_query(sql=query, con=conn)
                print(f"*************** Consulta base datos tabla: {nom_table} *********")
                return df
            else:
                print("Error: 'nom_table' no puede estar vacío.")
                return df
        except Exception as errores:
            print(f"Error al obtener los datos de {nom_table}: {errores}")
            return df
        finally:
            if conn:
                conn.close()

    def update_data(self, nom_table="", data={}, condition=""):
        """
        Actualiza registros en una tabla basado en una condición.
        (Idéntico a tu método).
        """
        conn = None
        try:
            if len(nom_table) > 0 and len(data) > 0 and len(condition) > 0:
                conn = sqlite3.connect(self.db_name)
                cursor = conn.cursor()
                set_values = ", ".join([f"{key} = ?" for key in data.keys()])
                query = f"UPDATE {nom_table} SET {set_values} WHERE {condition}"
                
                cursor.execute(query, tuple(data.values()))
                conn.commit()
                
                print(f"*************** Datos actualizados en la tabla: {nom_table} con la condición: {condition}*********")
            else:
                print("Error: Nombre de tabla, datos a actualizar y condición son obligatorios.")
        except Exception as errores:
            print("Error al actualizar los datos:", errores)
        finally:
            if conn:
                cursor.close()
                conn.close()

    def delete_data(self, nom_table="", condition=""):
        """
        Elimina registros de una tabla basado en una condición.
        (Idéntico a tu método).
        """
        conn = None
        try:
            if len(nom_table) > 0 and len(condition) > 0:
                conn = sqlite3.connect(self.db_name)
                cursor = conn.cursor()
                query = f"DELETE FROM {nom_table} WHERE {condition}"
                cursor.execute(query)
                conn.commit()
                print(f"*************** Datos eliminados de la tabla: {nom_table} con la condición: {condition}*********")
            else:
                print("Error: Nombre de tabla y condición son obligatorios para la eliminación.")
        except Exception as errores:
            print("Error al eliminar los datos:", errores)
        finally:
            if conn:
                cursor.close()
                conn.close()

    # --- Método Específico del Caso de Estudio ---
    
    def get_memecoins_with_blockchain_info(self):
        """
        Consulta específica que une las tablas para mostrar la información 
        requerida por CryptoRuanas.
        """
        df = pd.DataFrame()
        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            query = """
            SELECT
                m.nombre AS "Meme Coin",
                m.simbolo,
                m.precio_usd,
                m.market_cap_usd,
                b.nombre_blockchain AS "Ecosistema",
                m.ultima_actualizacion
            FROM
                MemeCoin m
            JOIN
                Blockchain b ON m.blockchain_origen_id = b.blockchain_id
            ORDER BY
                m.market_cap_usd DESC;
            """
            df = pd.read_sql_query(sql=query, con=conn)
            return df
        except Exception as errores:
            print(f"Error al consultar datos combinados: {errores}")
            return df
        finally:
            if conn:
                conn.close()

# --- Ejemplo de uso ---
if __name__ == "__main__":
    
    # 1. Crear instancia de la base de datos
    # (Esto creará el archivo 'crypto_ruanas.sqlite' y las tablas)
    db = CryptoDatabase()

    # 2. Preparar datos de prueba (simulando una llamada a CoinGecko)
    datos_prueba = {
        'coin_id': ['pepe', 'dogwifhat', 'bonk', 'shiba-inu'],
        'nombre': ['Pepe', 'dogwifhat', 'Bonk', 'Shiba Inu'],
        'simbolo': ['PEPE', 'WIF', 'BONK', 'SHIB'],
        'precio_usd': [0.000007, 2.50, 0.00002, 0.000025],
        'market_cap_usd': [3000000000, 2500000000, 1200000000, 15000000000],
        'volumen_24h': [500000000, 400000000, 200000000, 1000000000],
        'blockchain_id': ['ethereum', 'solana', 'solana', 'ethereum'],
        'blockchain_nombre': ['Ethereum', 'Solana', 'Solana', 'Ethereum']
    }
    df_monedas = pd.DataFrame(datos_prueba)

    print("\n--- 1. Probando UPSERT de Memecoins ---")
    db.upsert_memecoins_from_df(df_monedas)

    # 3. Leer datos de una tabla
    print("\n--- 2. Leyendo datos de la tabla 'MemeCoin' ---")
    df_leido = db.read_data(nom_table="MemeCoin")
    print(df_leido.head())

    print("\n--- 3. Leyendo datos de la tabla 'Blockchain' ---")
    df_blockchains = db.read_data(nom_table="Blockchain")
    print(df_blockchains.head()) # Debería mostrar 'ethereum' y 'solana' sin duplicados

    # 4. Simular una actualización de precios
    datos_actualizados = {
        'coin_id': ['pepe', 'dogwifhat'],
        'nombre': ['Pepe', 'dogwifhat'],
        'simbolo': ['PEPE', 'WIF'],
        'precio_usd': [0.000009, 2.75], # Precios actualizados
        'market_cap_usd': [3100000000, 2750000000],
        'volumen_24h': [600000000, 450000000],
        'blockchain_id': ['ethereum', 'solana'],
        'blockchain_nombre': ['Ethereum', 'Solana']
    }
    df_actualizado = pd.DataFrame(datos_actualizados)
    
    print("\n--- 4. Probando UPSERT (actualización de precios) ---")
    db.upsert_memecoins_from_df(df_actualizado)
    
    df_leido_act = db.read_data(nom_table="MemeCoin")
    print(df_leido_act[['nombre', 'precio_usd', 'market_cap_usd']])

    # 5. Usar el método de consulta específico
    print("\n--- 5. Consulta final para CryptoRuanas (con JOIN) ---")
    df_reporte = db.get_memecoins_with_blockchain_info()
    print(df_reporte)
    
    # 6. Probar eliminación (ej. eliminar 'bonk')
    print("\n--- 6. Eliminando un registro (Bonk) ---")
    db.delete_data(nom_table="MemeCoin", condition="simbolo = 'BONK'")
    
    df_reporte_final = db.get_memecoins_with_blockchain_info()
    print(df_reporte_final)
