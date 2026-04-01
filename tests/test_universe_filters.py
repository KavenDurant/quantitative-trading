from qt.data.ingest.universe_builder import RawInstrument, filter_universe


def test_filter_universe_keeps_only_main_board_non_st():
    instruments = [
        RawInstrument(code="600519", name="A", list_date="2001-01-01", exchange="SH", board="main"),
        RawInstrument(code="300750", name="B", list_date="2018-01-01", exchange="SZ", board="gem"),
        RawInstrument(code="000001", name="C", list_date="1991-01-01", exchange="SZ", board="main", is_st=True),
    ]

    filtered = filter_universe(instruments)

    assert [item.code for item in filtered] == ["600519"]
