
import duckdb
import sys

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print("Usage: pcat <file.parquet>")
    sys.exit(1)

  conn = duckdb.connect()
  conn.install_extension("parquet")
  conn.load_extension("parquet")
  ret = conn.execute(f"SELECT * FROM '{sys.argv[1]}'").fetchall()
  colnames = [x[0] for x in conn.description]
  print(colnames)
  for row in ret:
    print(row)
