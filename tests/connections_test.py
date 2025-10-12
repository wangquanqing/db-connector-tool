from db_connector.core.connections import DatabaseManager

db_manager = DatabaseManager()
result = db_manager.execute_query("pg", "SELECT * FROM information_schema.table_privileges WHERE grantee=:grantee", {"grantee": "cvicse"})
print(result)
