from pegasus_wrapper import PegasusProfile

from Pegasus.api import Namespace


def test_pegasus_profile():
    profile = PegasusProfile(namespace="dagman", key="key", value="value")

    assert str(profile) == f"({Namespace.DAGMAN}, key=key, value=value)"
