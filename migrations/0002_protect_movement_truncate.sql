-- Prevent bulk truncation from bypassing the row-level UPDATE/DELETE guards.
CREATE TRIGGER inventory_movements_no_truncate
    BEFORE TRUNCATE ON inventory_movements
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_inventory_movement_mutation();
