"""The PPD HTTP client, driven through its `opener` seam.

`PpdClient` takes an `urllib.request.OpenerDirector` precisely so it can be exercised without
network egress, and nothing used it — the class sat at 43.6% while the module's two free functions
were well covered. Everything below is offline.

The properties are about data integrity rather than plumbing. A response decoded with the wrong
charset mangles plan names silently; a 4xx body parsed as data produces rows from an error page;
a timeout that never reaches the socket lets one hung request stall an entire ingest run — and
that last one is stated as a requirement in the class docstring with nothing checking it.
"""

from __future__ import annotations

import email.message
import io
import urllib.error

import pytest

from pension_data.sources.ppd.client import (
    PPD_API_BASE_URL,
    PpdApiError,
    PpdClient,
    build_codebook_url,
    build_qvariables_url,
    parse_qvariables_json,
)


class _Response(io.BytesIO):
    """Enough of an `http.client.HTTPResponse` for the client's `with ... as response` block."""

    def __init__(self, body: bytes, *, status: int = 200, charset: str | None = "utf-8") -> None:
        super().__init__(body)
        self.status = status
        message = email.message.Message()
        content_type = "application/json"
        if charset is not None:
            content_type = f"{content_type}; charset={charset}"
        message["Content-Type"] = content_type
        self.headers = message

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class _Opener:
    """Records what the client asked for, and returns (or raises) what the test decides."""

    def __init__(self, outcome: _Response | Exception) -> None:
        self.outcome = outcome
        self.calls: list[dict[str, object]] = []

    def open(self, request, timeout=None):  # noqa: ANN001 - mirrors OpenerDirector.open
        self.calls.append(
            {
                "url": request.full_url,
                "timeout": timeout,
                "method": request.get_method(),
                "headers": dict(request.headers),
            }
        )
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def _json_client(body: str, **response_kwargs) -> tuple[PpdClient, _Opener]:
    opener = _Opener(
        _Response(body.encode(response_kwargs.pop("encoding", "utf-8")), **response_kwargs)
    )
    return PpdClient(opener=opener), opener


# ---------------------------------------------------------------------------------------------
# URL construction.
# ---------------------------------------------------------------------------------------------


def test_no_variables_is_refused_rather_than_sent():
    """An empty `variables=` is a valid URL the API answers — with the wrong thing."""
    with pytest.raises(PpdApiError, match="at least one variable"):
        build_qvariables_url(variables=[], fy_start=2020, fy_end=2021)


def test_an_inverted_year_range_is_refused():
    """`filterfystart > filterfyend` returns an empty set, which reads as 'no data published'."""
    with pytest.raises(PpdApiError, match="must not exceed"):
        build_qvariables_url(variables=["ppd_id"], fy_start=2022, fy_end=2021)


def test_a_single_year_range_is_allowed():
    url = build_qvariables_url(variables=["ppd_id"], fy_start=2021, fy_end=2021)
    assert "filterfystart=2021" in url
    assert "filterfyend=2021" in url


def test_variables_are_sent_as_one_comma_separated_parameter():
    """The API takes one `variables` key, not repeated ones. Repeating them drops all but one."""
    url = build_qvariables_url(
        variables=["ppd_id", "fy", "ActFundedRatio_GASB"], fy_start=2020, fy_end=2021
    )
    assert url.count("variables=") == 1
    assert "variables=ppd_id%2Cfy%2CActFundedRatio_GASB" in url


def test_the_codebook_url_asks_for_the_documented_template():
    url = build_codebook_url()
    assert "q=gettemplate" in url
    assert "template=data-codebook" in url
    assert "format=csv" in url


def test_a_mirror_base_url_reaches_the_request():
    """Overriding the base and still hitting production is the failure this prevents."""
    client = PpdClient(base_url="https://mirror.test/api/")
    assert client.qvariables_url(variables=["ppd_id"], fy_start=2020, fy_end=2021).startswith(
        "https://mirror.test/api/?"
    )
    assert client.codebook_url().startswith("https://mirror.test/api/?")


def test_the_default_base_url_is_the_documented_endpoint():
    assert (
        PpdClient()
        .qvariables_url(variables=["ppd_id"], fy_start=2020, fy_end=2021)
        .startswith(PPD_API_BASE_URL)
    )


# ---------------------------------------------------------------------------------------------
# The request the client actually makes.
# ---------------------------------------------------------------------------------------------


def test_the_configured_timeout_reaches_the_socket():
    """The class docstring calls a finite timeout mandatory "so a hung socket never stalls an
    ingest run". Nothing checked that it was passed on."""
    opener = _Opener(_Response(b"[]"))
    PpdClient(timeout=7.5, opener=opener).fetch_qvariables_raw(
        variables=["ppd_id"], fy_start=2020, fy_end=2021
    )
    assert opener.calls[0]["timeout"] == 7.5


def test_the_request_is_a_get_that_accepts_any_content_type():
    """The codebook is CSV and QVariables is JSON, from the same client."""
    opener = _Opener(_Response(b"[]"))
    PpdClient(opener=opener).fetch_codebook_raw()
    assert opener.calls[0]["method"] == "GET"
    assert opener.calls[0]["headers"]["Accept"] == "*/*"


def test_the_mirror_base_url_reaches_the_opener():
    opener = _Opener(_Response(b"[]"))
    PpdClient(base_url="https://mirror.test/api/", opener=opener).fetch_codebook_raw()
    assert str(opener.calls[0]["url"]).startswith("https://mirror.test/api/?")


# ---------------------------------------------------------------------------------------------
# Responses that are not what they claim.
# ---------------------------------------------------------------------------------------------


def test_an_error_status_is_refused_even_when_the_opener_returns_it():
    """A default opener raises on 4xx, but a mirror, a proxy or a redirect handler may hand the
    body back instead — and an HTML error page parses as 'no records'."""
    client, _ = _json_client("<html>Service Unavailable</html>", status=503)
    with pytest.raises(PpdApiError, match="HTTP 503"):
        client.fetch_codebook_raw()


@pytest.mark.parametrize("status", [400, 404, 500, 503])
def test_every_error_class_is_refused(status):
    client, _ = _json_client("[]", status=status)
    with pytest.raises(PpdApiError, match=str(status)):
        client.fetch_codebook_raw()


@pytest.mark.parametrize("status", [200, 201, 204, 304])
def test_success_and_redirect_statuses_are_accepted(status):
    client, _ = _json_client("[]", status=status)
    assert client.fetch_codebook_raw() == "[]"


def test_the_declared_charset_is_honoured():
    """Decoding a latin-1 body as utf-8 either raises or mangles plan names into mojibake, and
    the name is the join key for everything downstream."""
    opener = _Opener(
        _Response('[{"PlanName": "Café Municipal"}]'.encode("latin-1"), charset="latin-1")
    )
    rows = PpdClient(opener=opener).fetch_qvariables(
        variables=["PlanName"], fy_start=2020, fy_end=2021
    )
    assert rows == [{"PlanName": "Café Municipal"}]


def test_a_response_with_no_declared_charset_falls_back_to_utf8():
    opener = _Opener(_Response('[{"PlanName": "Café"}]'.encode(), charset=None))
    rows = PpdClient(opener=opener).fetch_qvariables(
        variables=["PlanName"], fy_start=2020, fy_end=2021
    )
    assert rows == [{"PlanName": "Café"}]


def test_an_http_error_names_the_code_and_the_url():
    """`urllib`'s own message says nothing about which request failed, and an ingest run makes
    many."""
    url = build_codebook_url()
    opener = _Opener(urllib.error.HTTPError(url, 404, "Not Found", None, None))
    with pytest.raises(PpdApiError) as excinfo:
        PpdClient(opener=opener).fetch_codebook_raw()
    assert "404" in str(excinfo.value)
    assert "gettemplate" in str(excinfo.value)


def test_a_transport_failure_names_the_reason_and_the_url():
    opener = _Opener(urllib.error.URLError("connection refused"))
    with pytest.raises(PpdApiError) as excinfo:
        PpdClient(opener=opener).fetch_codebook_raw()
    assert "connection refused" in str(excinfo.value)
    assert "gettemplate" in str(excinfo.value)


# ---------------------------------------------------------------------------------------------
# Parsing the two shapes the API has used.
# ---------------------------------------------------------------------------------------------


def test_a_bare_array_is_parsed():
    assert parse_qvariables_json('[{"ppd_id": 1}]') == [{"ppd_id": 1}]


@pytest.mark.parametrize("key", ["data", "result", "records"])
def test_each_documented_wrapper_key_is_unwrapped(key):
    assert parse_qvariables_json(f'{{"{key}": [{{"ppd_id": 1}}]}}') == [{"ppd_id": 1}]


def test_the_first_matching_wrapper_key_wins_deterministically():
    """Two keys present is malformed either way; what matters is that the choice is not arbitrary
    across runs, because a silent switch changes which rows get ingested."""
    body = '{"data": [{"ppd_id": 1}], "result": [{"ppd_id": 2}]}'
    assert parse_qvariables_json(body) == [{"ppd_id": 1}]


def test_an_object_with_no_records_array_is_refused():
    with pytest.raises(PpdApiError, match="no data/result/records array"):
        parse_qvariables_json('{"error": "rate limited"}')


def test_a_wrapper_key_holding_a_non_array_is_not_treated_as_records():
    with pytest.raises(PpdApiError, match="no data/result/records array"):
        parse_qvariables_json('{"data": {"ppd_id": 1}}')


@pytest.mark.parametrize("body", ["42", '"a string"', "null", "true"])
def test_a_scalar_response_is_refused(body):
    with pytest.raises(PpdApiError, match="neither an array nor an object"):
        parse_qvariables_json(body)


def test_a_list_of_non_objects_is_refused_rather_than_ingested():
    """`[1, 2, 3]` would otherwise reach the mapper, which reads keys off integers."""
    with pytest.raises(PpdApiError, match="not a list of objects"):
        parse_qvariables_json("[1, 2, 3]")


def test_a_partially_valid_list_is_refused_whole():
    """Silently dropping the bad row would under-report the plan universe with no warning."""
    with pytest.raises(PpdApiError, match="not a list of objects"):
        parse_qvariables_json('[{"ppd_id": 1}, "oops"]')


def test_invalid_json_names_the_parse_failure():
    with pytest.raises(PpdApiError, match="not valid JSON"):
        parse_qvariables_json("{not json")


def test_an_empty_array_is_a_valid_empty_result():
    """Distinct from every error above: the API answered, and there were no rows."""
    assert parse_qvariables_json("[]") == []


# `parse_qvariables_json` ends with `[dict(row) for row in records]` rather than `list(records)`.
# A test for that was written and then DELETED: `json.loads` builds fresh objects on every call and
# the parsed payload is discarded immediately, so the copy has no observable effect and replacing
# it with `list(records)` fails nothing. The break demo is what showed the assertion was vacuous —
# it passed against the broken code. A test that cannot fail is worse than no test.
