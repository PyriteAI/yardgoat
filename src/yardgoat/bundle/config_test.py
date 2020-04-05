from io import StringIO

import pytest

from . import config, exceptions


def test_BundleConfig_cmd_validator():
    bc = config.BundleConfig(name="foo")
    assert bc.cmd is None

    bc = config.BundleConfig(name="foo", cmd="echo foo")
    assert bc.cmd == "echo foo"

    bc = config.BundleConfig(name="foo", cmd=["echo", "foo"])
    assert bc.cmd == ["echo", "foo"]

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", cmd=3)

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", cmd=["echo", 3])


def test_BundleConfig_entrypoint_validator():
    bc = config.BundleConfig(name="foo")
    assert bc.entrypoint is None

    bc = config.BundleConfig(name="foo", entrypoint="echo foo")
    assert bc.entrypoint == "echo foo"

    bc = config.BundleConfig(name="foo", entrypoint=["echo", "foo"])
    assert bc.entrypoint == ["echo", "foo"]

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", entrypoint=3)

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", entrypoint=["echo", 3])


def test_BundleConfig_volumes_converter():
    bc = config.BundleConfig(
        name="foo",
        volumes={
            "/home/foo": "/mnt/foo",
            "/home/bar": {"bind": "/mnt/bar", "mode": "ro"},
        },
    )
    expected = {
        "/home/foo": {"bind": "/mnt/foo", "mode": "rw"},
        "/home/bar": {"bind": "/mnt/bar", "mode": "ro"},
    }

    assert bc.volumes == expected


def test_BundleCOnfig_volumes_validator():
    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", volumes={3: "/mnt/foo"})

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", volumes={("foo",): "/mnt/foo"})

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", volumes={"foo": 3})

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", volumes={"foo": ["hello"]})

    with pytest.raises(exceptions.MissingKeyError):
        config.BundleConfig(name="foo", volumes={"foo": {"mount": "/mnt"}})

    with pytest.raises(exceptions.MissingKeyError):
        config.BundleConfig(name="foo", volumes={"foo": {"bind": "/mnt"}})

    with pytest.raises(exceptions.MissingKeyError):
        config.BundleConfig(name="foo", volumes={"foo": {"mode": "rw"}})

    with pytest.raises(TypeError):
        config.BundleConfig(name="foo", volumes={"foo": {"bind": 3, "mode": "rw"}})

    with pytest.raises(ValueError):
        config.BundleConfig(name="foo", volumes={"foo": {"bind": "/", "mode": 3}})

    with pytest.raises(exceptions.InvalidKeyError):
        config.BundleConfig(
            name="foo", volumes={"foo": {"bind": "/", "mode": "rw", "foo": "bar"}}
        )


def test_BundleConfig_dumps():
    expected = """
name = "foo"
cmd = "bar"
entrypoint = "baz"
[volumes."/home/mnt"]
bind = "/mnt/bin"
mode = "rw"
[volumes."/home/bar"]
bind = "/mnt/bar"
mode = "rw"
""".replace(
        "\n", ""
    )

    bc = config.BundleConfig(
        name="foo",
        cmd="bar",
        entrypoint="baz",
        volumes={
            "/home/mnt": {"bind": "/mnt/bin", "mode": "rw"},
            "/home/bar": "/mnt/bar",
        },
    )

    actual = bc.dumps().replace("\n", "")
    assert actual == expected


def test_BundleConfig_dump():
    expected = """
name = "foo"
cmd = "bar"
entrypoint = "baz"
[volumes."/home/mnt"]
bind = "/mnt/bin"
mode = "rw"
[volumes."/home/bar"]
bind = "/mnt/bar"
mode = "rw"
""".replace(
        "\n", ""
    )

    bc = config.BundleConfig(
        name="foo",
        cmd="bar",
        entrypoint="baz",
        volumes={
            "/home/mnt": {"bind": "/mnt/bin", "mode": "rw"},
            "/home/bar": "/mnt/bar",
        },
    )

    sio = StringIO()
    bc.dump(sio)

    sio.seek(0)
    assert sio.read().replace("\n", "") == expected


def test_loads():
    expected = config.BundleConfig(
        name="foo",
        cmd="bar",
        entrypoint="baz",
        volumes={
            "/home/mnt": {"bind": "/mnt/bin", "mode": "rw"},
            "/home/bar": "/mnt/bar",
        },
    )

    text = """
name = "foo"
cmd = "bar"
entrypoint = "baz"
[volumes."/home/mnt"]
bind = "/mnt/bin"
mode = "rw"
[volumes."/home/bar"]
bind = "/mnt/bar"
mode = "rw"
"""
    actual = config.loads(text)
    assert actual == expected


def test_load():
    expected = config.BundleConfig(
        name="foo",
        cmd="bar",
        entrypoint="baz",
        volumes={
            "/home/mnt": {"bind": "/mnt/bin", "mode": "rw"},
            "/home/bar": "/mnt/bar",
        },
    )

    text = """
name = "foo"
cmd = "bar"
entrypoint = "baz"
[volumes."/home/mnt"]
bind = "/mnt/bin"
mode = "rw"
[volumes."/home/bar"]
bind = "/mnt/bar"
mode = "rw"
"""
    sio = StringIO(text)
    actual = config.load(sio)
    assert actual == expected
