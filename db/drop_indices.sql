DO $$
DECLARE
 i RECORD;
BEGIN
    FOR i IN (SELECT schemaname, indexname fROM pg_indexes
                WHERE schemaname IN ('public', 'flags', 'legacy')
                AND NOT indexdef LIKE '%UNIQUE%')
    LOOP
        EXECUTE format('DROP INDEX IF EXISTS %s.%s', i.schemaname, i.indexname);
    END LOOP;
END $$;

