-- Create manually once.
CREATE DATABASE IF NOT EXISTS music_dw
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Create user and grant permissions (run once)
CREATE USER IF NOT EXISTS 'etl_user'@'localhost' IDENTIFIED BY 'etl_password';

-- Grant permissions to the ETL user
GRANT ALL PRIVILEGES ON music_dw.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;