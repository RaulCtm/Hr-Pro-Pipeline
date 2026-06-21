"""
Persistencia en PostgreSQL normalizado y seguro (Issue #7 + #17).

Toma un diccionario ensamblado de Redis, hashea el passport y encripta
los datos financieros (IBAN, Salary) a nivel de columna antes de persistir.
"""

from __future__ import annotations
import logging
import hashlib
import os
from typing import Any
import psycopg2

from core.database import get_postgres_connection

logger = logging.getLogger(__name__)

# Obtenemos la clave de encriptación de las variables de entorno
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "default-key")

def upsert_employee(employee_data: dict[str, Any]) -> None:
    passport = employee_data.get("passport")
    if not passport:
        logger.warning("Intento de upsert sin passport. Saltando...")
        return

    passport_hash = hashlib.sha256(passport.encode('utf-8')).hexdigest()
    iban = employee_data.get("iban")
    salary = employee_data.get("salary")

    conn = get_postgres_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            # 2. UPSERT en employees y obtención del UUID
            cur.execute("""
                INSERT INTO employees (passport_hash, fullname, name, last_name, sex)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (passport_hash) DO UPDATE SET
                    fullname = COALESCE(EXCLUDED.fullname, employees.fullname),
                    name = COALESCE(EXCLUDED.name, employees.name),
                    last_name = COALESCE(EXCLUDED.last_name, employees.last_name),
                    sex = COALESCE(EXCLUDED.sex, employees.sex),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING employee_id;
            """, (
                passport_hash, 
                employee_data.get("fullname"), 
                employee_data.get("name"), 
                employee_data.get("last_name"), 
                ",".join(employee_data.get("sex", [])) if isinstance(employee_data.get("sex"), list) else employee_data.get("sex")
            ))
            
            employee_id = cur.fetchone()[0]

            # 3. UPSERT en employee_locations
            cur.execute("""
                INSERT INTO employee_locations (employee_id, address, city, country, personal_email, personal_phone)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (employee_id) DO UPDATE SET
                    address = COALESCE(EXCLUDED.address, employee_locations.address),
                    city = COALESCE(EXCLUDED.city, employee_locations.city),
                    country = COALESCE(EXCLUDED.country, employee_locations.country),
                    personal_email = COALESCE(EXCLUDED.personal_email, employee_locations.personal_email),
                    personal_phone = COALESCE(EXCLUDED.personal_phone, employee_locations.personal_phone);
            """, (
                employee_id, employee_data.get("address"), employee_data.get("city"), 
                employee_data.get("country"), employee_data.get("email"), employee_data.get("telfnumber")
            ))

            # 4. UPSERT en employments
            cur.execute("""
                INSERT INTO employments (employee_id, company_name, company_address, company_phone, company_email, job_title)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (employee_id) DO UPDATE SET
                    company_name = COALESCE(EXCLUDED.company_name, employments.company_name),
                    company_address = COALESCE(EXCLUDED.company_address, employments.company_address),
                    company_phone = COALESCE(EXCLUDED.company_phone, employments.company_phone),
                    company_email = COALESCE(EXCLUDED.company_email, employments.company_email),
                    job_title = COALESCE(EXCLUDED.job_title, employments.job_title);
            """, (
                employee_id, employee_data.get("company"), employee_data.get("company_address"),
                employee_data.get("company_telfnumber"), employee_data.get("company_email"), employee_data.get("job")
            ))

            # 5. UPSERT en finances (CON ENCRIPTACIÓN pgcrypto)
            cur.execute("""
                INSERT INTO finances (employee_id, iban, salary)
                VALUES (
                    %s, 
                    CASE WHEN %s IS NOT NULL THEN pgp_sym_encrypt(%s, %s) ELSE NULL END,
                    CASE WHEN %s IS NOT NULL THEN pgp_sym_encrypt(%s, %s) ELSE NULL END
                )
                ON CONFLICT (employee_id) DO UPDATE SET
                    iban = CASE WHEN EXCLUDED.iban IS NOT NULL THEN EXCLUDED.iban ELSE finances.iban END,
                    salary = CASE WHEN EXCLUDED.salary IS NOT NULL THEN EXCLUDED.salary ELSE finances.salary END;
            """, (
                employee_id, 
                iban, iban, ENCRYPTION_KEY,
                salary, salary, ENCRYPTION_KEY
            ))

            conn.commit()
            logger.debug("Empleado persistido/actualizado en esquema 3FN con datos financieros encriptados: %s", employee_id)

    except psycopg2.Error as e:
        logger.error("Error de Postgres al hacer upsert de %s: %s", passport, e)
        conn.rollback()
    finally:
        conn.close()