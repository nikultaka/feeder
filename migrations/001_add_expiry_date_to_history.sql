-- Migration: Add expiry_date column to history table
-- Date: 2025-12-19

-- Add expiry_date column
ALTER TABLE history ADD COLUMN IF NOT EXISTS expiry_date VARCHAR(50);

-- Add index on expiry_date for better query performance (optional but recommended)
CREATE INDEX IF NOT EXISTS idx_history_expiry_date ON history(expiry_date);
