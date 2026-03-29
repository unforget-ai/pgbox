"""Setup script — needed to build platform-specific wheels.

pgbox bundles pre-compiled PostgreSQL binaries, so wheels must be
tagged per-platform (not py3-none-any).
"""

import platform
import sys

from setuptools import setup
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    """Force platform-specific wheel (not pure Python)."""

    def has_ext_modules(self):
        return True


setup(
    distclass=BinaryDistribution,
)
