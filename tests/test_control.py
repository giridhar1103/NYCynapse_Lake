import psycopg


def test_control_reconnects_after_the_server_drops_it(ctl, settings):
    # What a Postgres restart does: the server ends the session out from under the poller.
    pid = ctl.conn.execute("SELECT pg_backend_pid() AS pid").fetchone()["pid"]
    with psycopg.connect(settings.pg_dsn, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
    try:
        ctl.conn.execute("SELECT 1")
    except psycopg.OperationalError:
        pass  # the first use notices the dead session
    assert ctl.conn.execute("SELECT 1 AS one").fetchone()["one"] == 1
