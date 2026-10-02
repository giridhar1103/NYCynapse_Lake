from urllib.parse import parse_qs, urlparse

import httpx

from nycynapse_lake.http import Http
from nycynapse_lake.socrata import Dataset, Socrata, _after

DS = Dataset("data.example.test", "abcd-1234")


def test_keyset_clause_for_two_keys():
    clause = _after((":updated_at", ":id"), ("2026-01-01T00:00:00.000Z", "row-a'b"))
    assert clause == (
        ":updated_at > '2026-01-01T00:00:00.000Z' OR "
        "(:updated_at = '2026-01-01T00:00:00.000Z' AND :id > 'row-a''b')"
    )


def test_pages_follow_the_last_key_and_stop_on_a_short_page(tmp_path):
    seen = []
    bodies = iter([
        'v,":updated_at",":id"\n1,t1,r1\n2,t1,r2\n',
        'v,":updated_at",":id"\n3,t2,r3\n',
    ])

    def handler(req):
        seen.append(parse_qs(urlparse(str(req.url)).query))
        return httpx.Response(200, text=next(bodies))

    soda = Socrata(Http(transport=httpx.MockTransport(handler)))
    pages = list(soda.pages(DS, tmp_path, columns="v", where="v > 0",
                            keys=(":updated_at", ":id"), page_size=2))
    assert [p.rows for p in pages] == [2, 1]
    assert pages[0].last == ("t1", "r2")
    assert seen[0]["$where"] == ["v > 0"]
    assert ":id > 'r2'" in seen[1]["$where"][0]
    assert seen[0]["$order"] == [":updated_at, :id"]


def test_empty_page_ends_and_leaves_no_file(tmp_path):
    soda = Socrata(Http(transport=httpx.MockTransport(
        lambda req: httpx.Response(200, text='v,":id"\n'))))
    assert list(soda.pages(DS, tmp_path, columns="v", where="1=1")) == []
    assert not list(tmp_path.iterdir())


def test_multiline_fields_count_as_one_row(tmp_path):
    body = 'v,":id"\n"line one\nline two",r1\n'
    soda = Socrata(Http(transport=httpx.MockTransport(lambda req: httpx.Response(200, text=body))))
    pages = list(soda.pages(DS, tmp_path, columns="v", where="1=1"))
    assert pages[0].rows == 1 and pages[0].last == ("r1",)
