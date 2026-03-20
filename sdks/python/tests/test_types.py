from rune_sdk.types import RuneConfig, RuneContext


def test_rune_config_defaults():
    cfg = RuneConfig(name="echo")
    assert cfg.name == "echo"
    assert cfg.version == "0.0.0"
    assert cfg.supports_stream is False
    assert cfg.gate is None


def test_rune_context():
    ctx = RuneContext(rune_name="echo", request_id="r-1")
    assert ctx.rune_name == "echo"
    assert ctx.context == {}
