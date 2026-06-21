-- Esquema normalizado (3FN) y seguro de empleados en PostgreSQL (Issue #7)

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Tabla principal: Identidad del empleado (Surrogate Key + Hashed PII)
CREATE TABLE IF NOT EXISTS employees (
    employee_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    passport_hash VARCHAR(64) UNIQUE NOT NULL, -- SHA-256 hash del passport
    fullname VARCHAR(255),
    name VARCHAR(100),
    last_name VARCHAR(100),
    sex VARCHAR(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de contacto y ubicación (ahora enlazada por UUID, no por passport)
CREATE TABLE IF NOT EXISTS employee_locations (
    employee_id UUID REFERENCES employees(employee_id) ON DELETE CASCADE,
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    personal_email VARCHAR(255),
    personal_phone VARCHAR(50),
    PRIMARY KEY (employee_id)
);

-- Tabla de empleo
CREATE TABLE IF NOT EXISTS employments (
    employee_id UUID REFERENCES employees(employee_id) ON DELETE CASCADE,
    company_name VARCHAR(255),
    company_address VARCHAR(255),
    company_phone VARCHAR(50),
    company_email VARCHAR(255),
    job_title VARCHAR(255),
    PRIMARY KEY (employee_id)
);

-- (El resto del esquema se queda igual, solo cambia la tabla finances y la vista)

-- Tabla de datos bancarios
CREATE TABLE IF NOT EXISTS finances (
    employee_id UUID REFERENCES employees(employee_id) ON DELETE CASCADE,
    iban BYTEA, -- Ahora es binario porque estará encriptado
    salary BYTEA, -- Encriptado también
    PRIMARY KEY (employee_id)
);

-- Vista analítica (Como RRHH no debe ver el IBAN/Salario real, no lo desencriptamos aquí)
CREATE OR REPLACE VIEW v_employees_complete AS
SELECT 
    e.employee_id, e.fullname, e.name, e.last_name, e.sex, e.updated_at,
    l.address, l.city, l.country, l.personal_email, l.personal_phone,
    emp.company_name, emp.company_address, emp.company_phone, emp.company_email, emp.job_title,
    f.iban AS iban_encrypted, -- Exponemos el dato encriptado (basura para un hacker)
    f.salary AS salary_encrypted
FROM employees e
LEFT JOIN employee_locations l ON e.employee_id = l.employee_id
LEFT JOIN employments emp ON e.employee_id = emp.employee_id
LEFT JOIN finances f ON e.employee_id = f.employee_id;