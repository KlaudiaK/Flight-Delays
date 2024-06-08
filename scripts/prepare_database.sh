# Uruchomienie kontenera Docker z bazą danych PostgreSQL
echo "Starting PostgreSQL container..."
docker run --name postgresdb -p 8432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres

echo "Waiting for PostgreSQL container to start..."
sleep 10
echo "PostgreSQL container started successfully."

echo "Executing SQL setup script..."
psql -h localhost 8432 -U postgres -v user="${JDBC_USERNAME}" -v password="${JDBC_PASSWORD}" -v db_name="${JDBC_DATABASE}" -f scripts/setup.sql
echo "SQL setup script executed successfully."