def test_migration_files_are_ordered():
    from tools import apply_migrations

    files = apply_migrations.migration_files()
    names = [path.name for path in files]

    assert names == sorted(names)
    assert names[0].startswith("0000_")


def test_checksum_sql_is_stable():
    from tools import apply_migrations

    sql = "SELECT 1;\n"

    assert apply_migrations.checksum_sql(sql) == apply_migrations.checksum_sql(sql)
    assert apply_migrations.checksum_sql(sql) != apply_migrations.checksum_sql("SELECT 2;\n")

